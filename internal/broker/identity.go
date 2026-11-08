package broker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/protocol"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// caller is an authenticated agent.
type caller struct {
	pod *corev1.Pod
	id  workspace.Identity
	// job is "" for workload Pods, or the Job kind: "mover" or "maintenance".
	job    string
	jobObj *batchv1.Job
	// workloadPod is the Pod a separate worker Job serves. It is nil after
	// that Pod is gone; the Job can still take and report its final snapshot.
	workloadPod *corev1.Pod
	workerEpoch int64
	// workerRef is a worker Job's registration; its target fields, not the
	// Job's editable annotations, name the workload.
	workerRef wsnapv1.JobRef
}

func (s *Server) resolve(r *http.Request) (*caller, error) {
	ctx := r.Context()
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return nil, fail(http.StatusUnauthorized, "missing bearer token")
	}
	ident, err := s.authenticate(r, token)
	if err != nil {
		return nil, err
	}
	var pod corev1.Pod
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: ident.Namespace, Name: ident.PodName}, &pod); apierrors.IsNotFound(err) {
		return nil, fail(http.StatusForbidden, "pod %s/%s: %v", ident.Namespace, ident.PodName, err)
	} else if err != nil {
		return nil, upstream(err, "pod %s/%s", ident.Namespace, ident.PodName)
	}
	if string(pod.UID) != ident.PodUID {
		return nil, fail(http.StatusForbidden, "token belongs to an earlier pod named %s", ident.PodName)
	}
	if s.Limiter != nil && !s.Limiter.Allow(ident.PodUID) {
		return nil, fail(http.StatusTooManyRequests, "too many requests from pod %s", pod.Name)
	}
	c := &caller{pod: &pod}
	if job := pod.Labels[wsnapv1.LabelJob]; job != "" {
		return s.resolveJob(ctx, c, job)
	}
	if c.id, err = workspace.FromPod(&pod); err != nil {
		return nil, fail(http.StatusForbidden, "%v", err)
	}
	var ws wsnapv1.Workspace
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: c.id.Name}, &ws); err == nil && ws.Spec.ExecutionMode() == wsnapv1.ExecutionWorker {
		return nil, fail(http.StatusForbidden, "workspace %s uses a separate worker; workload Pods cannot request snapshot credentials", c.id.Name)
	} else if err != nil && !apierrors.IsNotFound(err) {
		return nil, upstream(err, "workspace %s", c.id.Name)
	}
	if err := s.checkWorkloadIsolation(&pod); err != nil {
		return nil, err
	}
	if err := s.checkNoAPIToken(ctx, &pod); err != nil {
		return nil, err
	}
	return c, nil
}

// authenticate validates an agent token. A token the broker validated
// recently is answered from its cache; any other token costs a TokenReview,
// which is limited per client IP (429).
func (s *Server) authenticate(r *http.Request, token string) (Identity, error) {
	var (
		ident Identity
		err   error
		known bool
	)
	if ca, ok := s.Auth.(*CachingAuthenticator); ok {
		ident, err, known = ca.Known(token)
	}
	if !known {
		if ip := clientIP(r); !s.reviews().Allow(ip) {
			return Identity{}, fail(http.StatusTooManyRequests, "too many token reviews from %s", ip)
		}
		ident, err = s.Auth.Authenticate(r.Context(), token)
	}
	if errors.Is(err, ErrAuthUnavailable) {
		return ident, fail(http.StatusServiceUnavailable, "%v", err)
	}
	if err != nil {
		return ident, fail(http.StatusUnauthorized, "%v", err)
	}
	return ident, nil
}

func (s *Server) reviews() *PodLimiter {
	s.reviewsOnce.Do(func() {
		if s.ReviewsPerIP == nil {
			s.ReviewsPerIP = newReviewLimiter()
		}
	})
	return s.ReviewsPerIP
}

// checkNoAPIToken refuses workload Pods that mount the default API token.
func (s *Server) checkNoAPIToken(ctx context.Context, pod *corev1.Pod) error {
	if s.AllowAutomountedTokens {
		return nil
	}
	automount := pod.Spec.AutomountServiceAccountToken
	if automount == nil {
		sa := pod.Spec.ServiceAccountName
		if sa == "" {
			sa = "default"
		}
		var acct corev1.ServiceAccount
		if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: sa}, &acct); err == nil {
			automount = acct.AutomountServiceAccountToken
		}
	}
	if automount == nil || *automount {
		return fail(http.StatusForbidden, "pod %s mounts the default API token into untrusted containers; set automountServiceAccountToken: false", pod.Name)
	}
	return nil
}

// resolveJob accepts Pods of Jobs the controller created.
func (s *Server) resolveJob(ctx context.Context, c *caller, job string) (*caller, error) {
	owner := metav1.GetControllerOf(c.pod)
	if owner == nil || owner.Kind != "Job" {
		return nil, fail(http.StatusForbidden, "pod has label %s but no owning Job", wsnapv1.LabelJob)
	}
	var j batchv1.Job
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.pod.Namespace, Name: owner.Name}, &j); apierrors.IsNotFound(err) {
		return nil, fail(http.StatusForbidden, "job %s: %v", owner.Name, err)
	} else if err != nil {
		return nil, upstream(err, "job %s", owner.Name)
	}
	if j.UID != owner.UID || j.Labels[wsnapv1.LabelManagedBy] != wsnapv1.ManagedByValue || j.Labels[wsnapv1.LabelJob] != job {
		return nil, fail(http.StatusForbidden, "job %s is not a wsnap %s job", j.Name, job)
	}
	// An ownerReference can be written by anyone who creates Pods. Only a
	// Pod that runs exactly the Job's template, while the Job runs, acts
	// for the Job.
	if jobDone(&j) {
		return nil, fail(http.StatusForbidden, "job %s has finished", j.Name)
	}
	if err := sameAgentSpec(&c.pod.Spec, &j.Spec.Template.Spec, s.Audience); err != nil {
		return nil, fail(http.StatusForbidden, "pod %s does not run the template of job %s: %v", c.pod.Name, j.Name, err)
	}
	c.job, c.jobObj = job, &j
	c.id = workspace.Identity{
		Namespace:  j.Namespace,
		Name:       j.Labels[wsnapv1.LabelJobWorkspace],
		Repository: j.Labels[wsnapv1.LabelRepository],
	}
	if c.id.Name == "" || c.id.Repository == "" {
		return nil, fail(http.StatusForbidden, "job %s lacks workspace labels", j.Name)
	}
	// Labels can be copied; the registration in Workspace status cannot.
	var ws wsnapv1.Workspace
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: c.id.Name}, &ws); apierrors.IsNotFound(err) {
		return nil, fail(http.StatusForbidden, "workspace %s: %v", c.id.Name, err)
	} else if err != nil {
		return nil, upstream(err, "workspace %s", c.id.Name)
	}
	for _, ref := range ws.Status.Jobs {
		if ref.UID != string(j.UID) || ref.Kind != job {
			continue
		}
		if !workspace.JobRefMatches(ref, &j) {
			return nil, fail(http.StatusForbidden, "job %s no longer matches its registered volume identity", j.Name)
		}
		switch job {
		case "mover", "restorer":
			if err := s.checkJobVolume(ctx, c, ref); err != nil {
				return nil, err
			}
		case "worker":
			if err := s.resolveWorkerTarget(ctx, c, ref); err != nil {
				return nil, err
			}
		}
		return c, nil
	}
	// 503, not 403: the controller records a Job just after creating it,
	// and the agent retries 5xx. A forged Job is never recorded.
	return nil, fail(http.StatusServiceUnavailable, "job %s is not registered by wsnap (yet)", j.Name)
}

// checkJobVolume closes the gap between a controller's binding check and
// the Job Pod's later credential request. Pod volumes name PVCs, so the
// same name could refer to a replacement claim by then.
func (s *Server) checkJobVolume(ctx context.Context, c *caller, registered wsnapv1.JobRef) error {
	j := c.jobObj
	ref := wsnapv1.VolumeRef{Kind: workspace.KindPVC, ClaimName: registered.ClaimName,
		ClaimUID: registered.ClaimUID, VolumeName: registered.VolumeName, VolumeUID: registered.VolumeUID}
	if ref.ClaimName == "" || ref.ClaimUID == "" || ref.VolumeName == "" || ref.VolumeUID == "" {
		return fail(http.StatusForbidden, "job %s lacks a complete PVC and PV identity", j.Name)
	}
	if err := workspace.ValidateBinding(ctx, s.Reader, c.id.Namespace, ref); err != nil {
		if errors.Is(err, workspace.ErrBadVolume) {
			return fail(http.StatusForbidden, "job %s volume binding changed: %v", j.Name, err)
		}
		return upstream(err, "check job %s volume", j.Name)
	}
	var pv corev1.PersistentVolume
	if err := s.Reader.Get(ctx, client.ObjectKey{Name: ref.VolumeName}, &pv); err != nil {
		return upstream(err, "check job %s volume owner", j.Name)
	}
	if pv.Annotations[wsnapv1.AnnotationWorkspaceRef] != c.id.Namespace+"/"+c.id.Name {
		return fail(http.StatusForbidden, "job %s volume %s belongs to another workspace", j.Name, pv.Name)
	}
	return nil
}

// resolveWorkerTarget finds the workload Pod a worker Job serves. A worker
// outlives its Pod to take the final snapshot, so a missing Pod, or a new
// Pod with the same name, means that Pod stopped; it is not a refusal.
func (s *Server) resolveWorkerTarget(ctx context.Context, c *caller, ref wsnapv1.JobRef) error {
	j := c.jobObj
	if ref.TargetPodUID == "" || ref.TargetPodName == "" || ref.ClaimName == "" || ref.ClaimUID == "" ||
		ref.VolumeName == "" || ref.VolumeUID == "" {
		return fail(http.StatusForbidden, "worker job %s lacks a complete target and volume identity", j.Name)
	}
	c.workerEpoch, c.workerRef = ref.WriterEpoch, ref
	var target corev1.Pod
	err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: ref.TargetPodName}, &target)
	if apierrors.IsNotFound(err) || (err == nil && string(target.UID) != ref.TargetPodUID) {
		return nil
	}
	if err != nil {
		return upstream(err, "worker target pod %s", ref.TargetPodName)
	}
	if target.Labels[wsnapv1.LabelWorkspace] != c.id.Name || target.Labels[wsnapv1.LabelRepository] != c.id.Repository ||
		target.Spec.NodeName != c.pod.Spec.NodeName {
		return fail(http.StatusForbidden, "worker target pod %s no longer matches its registered workspace and node", target.Name)
	}
	vol, err := workspace.ResolveVolume(ctx, s.Reader, &target)
	if err != nil {
		return volumeError(err)
	}
	if vol.ClaimName != ref.ClaimName || vol.ClaimUID != ref.ClaimUID || vol.VolumeName != ref.VolumeName || vol.VolumeUID != ref.VolumeUID {
		return fail(http.StatusForbidden, "worker job %s does not share the volume of its target pod %s", j.Name, target.Name)
	}
	c.workloadPod = &target
	return nil
}

// sameAgentSpec checks that a Pod runs its Job's template where it
// matters: every container that can read the wsnap token is a template
// container, unchanged (image, command, arguments, environment, mounts,
// security context), and every volume the template defines is unchanged.
// The API server defaults a Job template and its Pods the same way, so
// these fields match exactly for a Pod the Job controller created. Other
// containers and volumes are allowed, as long as the added containers
// mount no template volume but ConfigMaps: admission webhooks (a service
// mesh, for example) add them to real Job Pods.
func sameAgentSpec(pod, tmpl *corev1.PodSpec, audience string) error {
	if ptr.Deref(pod.ShareProcessNamespace, false) || pod.HostPID {
		return fmt.Errorf("shares a process namespace")
	}
	podVolumes := map[string]corev1.Volume{}
	for _, v := range pod.Volumes {
		podVolumes[v.Name] = v
	}
	for _, tv := range tmpl.Volumes {
		if pv, ok := podVolumes[tv.Name]; !ok || !equality.Semantic.DeepEqual(pv, tv) {
			return fmt.Errorf("volume %q differs", tv.Name)
		}
	}
	tokenVolumes := map[string]bool{}
	for _, v := range pod.Volumes {
		if v.Projected == nil {
			continue
		}
		for _, src := range v.Projected.Sources {
			if src.ServiceAccountToken != nil && src.ServiceAccountToken.Audience == audience {
				tokenVolumes[v.Name] = true
			}
		}
	}
	tmplContainers := map[string]corev1.Container{}
	for _, c := range tmpl.Containers {
		tmplContainers[c.Name] = c
	}
	// Everything that decides what runs inside the container, including
	// lifecycle hooks and probes: an exec hook or probe runs in the agent
	// container and can read its token and state (reviewer #6).
	same := func(p corev1.Container) bool {
		t, ok := tmplContainers[p.Name]
		return ok && p.Image == t.Image && equality.Semantic.DeepEqual(p.Command, t.Command) &&
			equality.Semantic.DeepEqual(p.Args, t.Args) && equality.Semantic.DeepEqual(p.Env, t.Env) &&
			equality.Semantic.DeepEqual(p.EnvFrom, t.EnvFrom) && equality.Semantic.DeepEqual(p.VolumeMounts, t.VolumeMounts) &&
			equality.Semantic.DeepEqual(p.SecurityContext, t.SecurityContext) &&
			equality.Semantic.DeepEqual(p.Lifecycle, t.Lifecycle) &&
			equality.Semantic.DeepEqual(p.LivenessProbe, t.LivenessProbe) &&
			equality.Semantic.DeepEqual(p.ReadinessProbe, t.ReadinessProbe) &&
			equality.Semantic.DeepEqual(p.StartupProbe, t.StartupProbe) &&
			equality.Semantic.DeepEqual(p.VolumeDevices, t.VolumeDevices) &&
			p.WorkingDir == t.WorkingDir && p.Stdin == t.Stdin && p.StdinOnce == t.StdinOnce && p.TTY == t.TTY
	}
	// Every template volume is private to the template's containers except
	// ConfigMaps: besides the token, the state volume holds the engine's
	// storage credentials (Kopia's repository.config), and the workspace
	// and backend volumes hold data.
	private := map[string]bool{}
	for _, v := range tmpl.Volumes {
		if v.ConfigMap == nil {
			private[v.Name] = true
		}
	}
	for name := range tokenVolumes {
		private[name] = true
	}
	readsToken := func(mounts []corev1.VolumeMount) bool {
		return slices.ContainsFunc(mounts, func(m corev1.VolumeMount) bool { return private[m.Name] })
	}
	for _, c := range pod.InitContainers {
		if readsToken(c.VolumeMounts) {
			return fmt.Errorf("init container %q mounts a volume of the agent", c.Name)
		}
	}
	for _, c := range pod.EphemeralContainers {
		if readsToken(c.VolumeMounts) || tmplContainers[c.TargetContainerName].Name != "" {
			return fmt.Errorf("ephemeral container %q can reach the agent", c.Name)
		}
	}
	found := 0
	for _, c := range pod.Containers {
		if _, ok := tmplContainers[c.Name]; ok {
			if !same(c) {
				return fmt.Errorf("container %q differs from the template", c.Name)
			}
			found++
		} else if readsToken(c.VolumeMounts) {
			return fmt.Errorf("container %q mounts a volume of the agent", c.Name)
		}
	}
	if found != len(tmpl.Containers) {
		return fmt.Errorf("template containers are missing")
	}
	return nil
}

// checkWorkloadIsolation refuses Pods whose workload containers could
// request credentials themselves. The agent's token must be mounted only
// into the sidecar.
func (s *Server) checkWorkloadIsolation(pod *corev1.Pod) error {
	if err := workspace.CheckIsolation(pod, s.Audience, s.AllowAutomountedTokens); err != nil {
		return fail(http.StatusForbidden, "pod %s: %v", pod.Name, err)
	}
	return nil
}

// workloadStarted reports whether any workload container has started. The
// restore role is allowed only before that, because after it the volume
// holds live data that a restore would overwrite and the Pod runs untrusted
// code.
func workloadStarted(pod *corev1.Pod) bool {
	sidecar := workspace.SidecarName(pod)
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == sidecar {
			continue
		}
		if cs.State.Running != nil || cs.State.Terminated != nil || cs.RestartCount > 0 || (cs.Started != nil && *cs.Started) {
			return true
		}
	}
	return false
}

func (c *caller) allowed(role protocol.Role) error {
	switch c.job {
	case "mover":
		if role == protocol.RoleMover {
			return nil
		}
	case "maintenance":
		if role == protocol.RoleMaintenance {
			return nil
		}
	case "restorer":
		// Reads the snapshot, then snapshots the restored volume.
		if role == protocol.RoleRestore || role == protocol.RoleMover {
			return nil
		}
	case "worker":
		if role == protocol.RoleSnapshot {
			return nil
		}
		if role == protocol.RoleRestore && c.workloadPod != nil && !workloadStarted(c.workloadPod) {
			return nil
		}
	default:
		switch role {
		case protocol.RoleSnapshot:
			return nil
		case protocol.RoleRestore:
			if workloadStarted(c.pod) {
				return fail(http.StatusForbidden, "restore is allowed only before the workload containers start")
			}
			return nil
		}
	}
	return fail(http.StatusForbidden, "pod %s may not use role %q", c.pod.Name, role)
}
