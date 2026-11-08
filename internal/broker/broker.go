// Package broker serves credentials to wsnap agents. An agent proves which
// Pod it runs in with a projected service account token. The broker maps
// the Pod to its workspace, decides which roles the Pod may have, and
// returns a credential scoped to that workspace and role.
package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/credentials"
	"github.com/chrisyifanjin/wsnap/internal/engine"
	"github.com/chrisyifanjin/wsnap/internal/protocol"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
)

// Server is the broker's HTTP server.
type Server struct {
	// Client writes Workspace status and PV annotations, and lists
	// protected PVs from the cache.
	Client client.Client
	// Reader reads live objects. It must not be a cache: the broker's
	// decisions depend on the current Pod status, and Secrets are not
	// cached on purpose.
	Reader   client.Reader
	Auth     Authenticator
	Audience string
	Log      logr.Logger
	// AllowAutomountedTokens accepts workload Pods that mount the default
	// API token. Off by default: untrusted code holding an API token may be
	// able to create Pods and so obtain workspace credentials itself.
	AllowAutomountedTokens bool
	// Limiter bounds requests per Pod. Nil disables limiting.
	Limiter *PodLimiter
	// ReviewsPerIP bounds TokenReviews (for tokens not in the cache) per
	// client IP. Nil: the default.
	ReviewsPerIP *PodLimiter
	reviewsOnce  sync.Once
	// Hub wakes long polls when requests appear. Nil falls back to
	// periodic checks only.
	Hub *Hub
	// Gateway enables the gateway API (gateway.go). Nil disables it.
	Gateway GatewayAuth
}

// Handler returns the broker's routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/credentials", s.handleCredentials)
	mux.HandleFunc("POST /v1/report", s.handleReport)
	mux.HandleFunc("POST /v1/wait", s.handleWait)
	s.registerGateway(mux)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

type httpError struct {
	code int
	msg  string
}

func (e *httpError) Error() string { return e.msg }

func fail(code int, format string, args ...any) error {
	return &httpError{code: code, msg: fmt.Sprintf(format, args...)}
}

func volumeError(err error) error {
	if errors.Is(err, workspace.ErrBadVolume) {
		return fail(http.StatusFailedDependency, "%v", err)
	}
	return upstream(err, "resolve workspace volume")
}

// upstream maps an API server error: a missing object is a configuration
// problem (424), anything else may pass (503, which agents retry). A
// transient error must not become a 4xx, or a final snapshot is dropped.
func upstream(err error, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...) + ": " + err.Error()
	if apierrors.IsNotFound(err) {
		return fail(http.StatusFailedDependency, "%s", msg)
	}
	return fail(http.StatusServiceUnavailable, "%s", msg)
}

func (s *Server) writeError(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	var he *httpError
	if errors.As(err, &he) {
		code = he.code
	}
	s.Log.Info("request refused", "status", code, "error", err.Error())
	http.Error(w, err.Error(), code)
}

// caller is an authenticated agent.
type caller struct {
	pod *corev1.Pod
	id  workspace.Identity
	// job is "" for workload Pods, or the Job kind: "mover" or "maintenance".
	job    string
	jobObj *batchv1.Job
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
		if ref.UID == string(j.UID) && ref.Kind == job {
			if ref != workspace.JobRef(&j) {
				return nil, fail(http.StatusForbidden, "job %s no longer matches its registered volume identity", j.Name)
			}
			if job == "mover" || job == "restorer" {
				if err := s.checkJobVolume(ctx, c, ref); err != nil {
					return nil, err
				}
			}
			return c, nil
		}
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

func (s *Server) handleCredentials(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	resp, err := s.credentials(r)
	role := "unknown"
	if resp != nil {
		role = string(resp.Role)
		issueSeconds.WithLabelValues(role).Observe(time.Since(start).Seconds())
	}
	requests.WithLabelValues("credentials", role, statusCode(err)).Inc()
	if err != nil {
		s.writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) credentials(r *http.Request) (*protocol.CredentialsResponse, error) {
	ctx := r.Context()
	c, err := s.resolve(r)
	if err != nil {
		return nil, err
	}
	var req protocol.CredentialsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || !req.Role.Valid() {
		return nil, fail(http.StatusBadRequest, "body must be {\"role\": restore|snapshot|mover|maintenance}")
	}
	// A claimed WorkspaceRestore authorizes the restore role in a running
	// Pod and selects what to restore.
	var restoreReq *wsnapv1.WorkspaceRestore
	if req.Role == protocol.RoleRestore && req.Request != "" {
		var err error
		if restoreReq, err = s.claimedRestore(ctx, c, req.Request); err != nil {
			return nil, err
		}
	} else if err := c.allowed(req.Role); err != nil {
		return nil, err
	}
	if restoreReq == nil && c.job == "restorer" && req.Role == protocol.RoleRestore {
		return nil, fail(http.StatusForbidden, "a restorer job must name its WorkspaceRestore")
	}
	if c.job == "mover" || c.job == "restorer" {
		if err := s.checkNoWorkloadOnVolume(ctx, c); err != nil {
			return nil, err
		}
	}
	var repo wsnapv1.SnapshotRepository
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: c.id.Repository}, &repo); err != nil {
		return nil, upstream(err, "snapshot repository %s/%s", c.id.Namespace, c.id.Repository)
	}
	var ws *wsnapv1.Workspace
	if c.job == "" {
		var err error
		ws, err = s.ensureWorkspace(ctx, c.id)
		if err != nil {
			return nil, err
		}
		if err := s.checkSingleWriter(ctx, c, ws); err != nil {
			return nil, err
		}
		if req.Role == protocol.RoleRestore && restoreReq == nil {
			if err := s.checkNoPendingRescue(ctx, c); err != nil {
				return nil, err
			}
		}
		// Protect the volume before handing out any credential, so data
		// the Pod writes is never reclaimed without a final snapshot.
		if err := s.trackVolume(ctx, c, req.Role == protocol.RoleSnapshot); err != nil {
			return nil, err
		}
	}
	// A restore may read another workspace (fork) or a pinned snapshot,
	// chosen by a WorkspaceRestore or by Workspace.spec.restoreFrom.
	target := c.id.Name
	var snapshotID, source string
	var restoreGen int64
	var rf *wsnapv1.RestoreSource
	switch {
	case restoreReq != nil:
		rf = &wsnapv1.RestoreSource{WorkspaceName: restoreReq.Spec.SourceWorkspace, SnapshotID: restoreReq.Spec.SnapshotID}
	case req.Role == protocol.RoleRestore && ws != nil && ws.Spec.RestoreFrom != nil && ws.Status.RestoreFromGeneration != ws.Generation:
		rf, restoreGen = ws.Spec.RestoreFrom, ws.Generation
	}
	if rf != nil {
		snapshotID = rf.SnapshotID
		if rf.WorkspaceName != "" && rf.WorkspaceName != c.id.Name {
			var src wsnapv1.Workspace
			if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: rf.WorkspaceName}, &src); err != nil {
				return nil, upstream(err, "restoreFrom workspace %s", rf.WorkspaceName)
			}
			if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: src.Spec.RepositoryName}, &repo); err != nil {
				return nil, upstream(err, "repository of workspace %s", rf.WorkspaceName)
			}
			target, source = rf.WorkspaceName, rf.WorkspaceName
		}
	}
	var secret corev1.Secret
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: repo.Namespace, Name: repo.Spec.Credentials.SecretName}, &secret); err != nil {
		return nil, upstream(err, "credentials secret")
	}
	backend, err := workspace.Backend(&repo, c.id.Namespace, target)
	if err != nil {
		return nil, fail(http.StatusFailedDependency, "%v", err)
	}
	provider, err := workspace.Provider(&repo)
	if err != nil {
		return nil, fail(http.StatusFailedDependency, "%v", err)
	}
	eng := workspace.Engine(&repo)
	creds, err := provider.Issue(ctx, credentials.Request{
		Role: req.Role, Namespace: c.id.Namespace, Workspace: target,
		Backend: backend, Secret: secret.Data,
		WriterDeletePrefixes: engine.WriterDeletePrefixes(eng),
		TTL:                  workspace.TTL(&repo),
	})
	if err != nil {
		// STS, IAM or Azure may be briefly down; agents retry 5xx.
		return nil, fail(http.StatusServiceUnavailable, "issue credential: %v", err)
	}
	password, err := credentials.RepositoryPassword(secret.Data[credentials.KeyMasterKey], c.id.Namespace, target)
	if err != nil {
		return nil, fail(http.StatusFailedDependency, "%v", err)
	}
	var latestID string
	var keep []string
	switch req.Role {
	case protocol.RoleRestore:
		if snapshotID == "" {
			// The recorded newest snapshot, of the source for a fork. A
			// deposed writer can still upload snapshots, but the broker never
			// records them, so they are never restored.
			var tws wsnapv1.Workspace
			if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: target}, &tws); err != nil {
				return nil, upstream(err, "workspace %s", target)
			}
			if last := tws.Status.LastSnapshot; last != nil {
				latestID = last.ID
			}
		}
	case protocol.RoleMaintenance:
		if keep, err = s.pinnedSnapshots(ctx, c.id); err != nil {
			return nil, err
		}
	}
	s.Log.Info("issued credential", "namespace", c.id.Namespace, "workspace", c.id.Name, "role", req.Role,
		"pod", c.pod.Name, "scoped", creds.Scoped, "expires", creds.ExpiresAt)
	return &protocol.CredentialsResponse{
		Namespace: c.id.Namespace, Workspace: c.id.Name, Role: req.Role,
		Engine: eng, Backend: backend, Credentials: creds, RepositoryPassword: password,
		Interval:  protocol.Duration{Duration: workspace.Interval(&repo)},
		KeepLast:  workspace.KeepLast(&repo),
		KeepDaily: workspace.KeepDaily(&repo),
		// Only an explicit setting: an unset field must not override the
		// agent's own --operation-timeout with the default.
		OperationTimeout: protocol.Duration{Duration: repo.Spec.OperationTimeout.Duration},
		SnapshotID:       snapshotID, SourceWorkspace: source, RestoreFromGeneration: restoreGen,
		LatestSnapshotID: latestID, KeepIDs: keep,
	}, nil
}

// maxSuperseded bounds Workspace.status.supersededSnapshots.
const maxSuperseded = 10

// pinnedSnapshots lists the snapshots of workspace id that maintenance
// must keep: its recorded newest snapshot, and every snapshot that a
// Workspace (fork or rollback) or an unfinished WorkspaceRestore pins.
func (s *Server) pinnedSnapshots(ctx context.Context, id workspace.Identity) ([]string, error) {
	var keep []string
	var list wsnapv1.WorkspaceList
	if err := s.Reader.List(ctx, &list, client.InNamespace(id.Namespace)); err != nil {
		return nil, upstream(err, "list workspaces")
	}
	for _, ws := range list.Items {
		if ws.Name == id.Name && ws.Status.LastSnapshot != nil {
			keep = append(keep, ws.Status.LastSnapshot.ID)
		}
		if ws.Name == id.Name {
			for _, s := range ws.Status.SupersededSnapshots {
				keep = append(keep, s.ID)
			}
		}
		if rf := ws.Spec.RestoreFrom; rf != nil && rf.SnapshotID != "" {
			src := rf.WorkspaceName
			if src == "" {
				src = ws.Name
			}
			if src == id.Name {
				keep = append(keep, rf.SnapshotID)
			}
		}
	}
	var restores wsnapv1.WorkspaceRestoreList
	if err := s.Reader.List(ctx, &restores, client.InNamespace(id.Namespace)); err != nil {
		return nil, upstream(err, "list workspace restores")
	}
	for _, r := range restores.Items {
		src := r.Spec.SourceWorkspace
		if src == "" {
			src = r.Spec.WorkspaceName
		}
		done := r.Status.Phase == wsnapv1.SnapshotSucceeded || r.Status.Phase == wsnapv1.SnapshotFailed
		if src == id.Name && r.Spec.SnapshotID != "" && !done {
			keep = append(keep, r.Spec.SnapshotID)
		}
	}
	return keep, nil
}

func (s *Server) ensureWorkspace(ctx context.Context, id workspace.Identity) (*wsnapv1.Workspace, error) {
	var ws wsnapv1.Workspace
	err := s.Reader.Get(ctx, client.ObjectKey{Namespace: id.Namespace, Name: id.Name}, &ws)
	if apierrors.IsNotFound(err) {
		ws = wsnapv1.Workspace{
			ObjectMeta: metav1.ObjectMeta{Namespace: id.Namespace, Name: id.Name,
				Labels: map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue}},
			Spec: wsnapv1.WorkspaceSpec{RepositoryName: id.Repository},
		}
		err = s.Client.Create(ctx, &ws)
		if err == nil {
			return &ws, nil
		}
		if !apierrors.IsAlreadyExists(err) {
			return nil, upstream(err, "create workspace")
		}
		// Lost the race: the winner (a user who set restoreFrom, or another
		// replica) has the real spec and status; ours is empty.
		err = s.Reader.Get(ctx, client.ObjectKey{Namespace: id.Namespace, Name: id.Name}, &ws)
	}
	if err != nil {
		return nil, upstream(err, "workspace %s", id.Name)
	}
	if ws.Spec.RepositoryName != id.Repository {
		return nil, fail(http.StatusConflict, "workspace %s uses repository %s, but the pod asks for %s", id.Name, ws.Spec.RepositoryName, id.Repository)
	}
	return &ws, nil
}

// checkSingleWriter allows one live writer per workspace. Two writers would
// interleave snapshots, and "latest" would flip between them. A new Pod
// waits (its sidecar retries) until the previous writer is gone, including
// while it takes its final snapshot, so the new Pod restores that snapshot.
func (s *Server) checkSingleWriter(ctx context.Context, c *caller, ws *wsnapv1.Workspace) error {
	w := ws.Status.WriterPod
	if w == nil || w.UID == string(c.pod.UID) {
		return nil
	}
	live, err := s.writerLive(ctx, ws.Namespace, w)
	if err != nil {
		return err
	}
	if live {
		return fail(http.StatusConflict, "workspace %s is written by pod %s; waiting until it stops", ws.Name, w.Name)
	}
	return nil
}

// stuckTerminationMargin: a Pod still terminating this long after its
// deletion deadline (metadata.deletionTimestamp already includes the
// grace period) is on a lost node; its kubelet will never finish it.
const stuckTerminationMargin = 2 * time.Minute

// writerLive reports whether the Pod ref still writes the workspace.
func (s *Server) writerLive(ctx context.Context, namespace string, ref *wsnapv1.PodRef) (bool, error) {
	var p corev1.Pod
	err := s.Reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, &p)
	if apierrors.IsNotFound(err) || (err == nil && string(p.UID) != ref.UID) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return stillWriting(&p, time.Now()), nil
}

// stillWriting reports whether a Pod can still write its volume.
func stillWriting(p *corev1.Pod, now time.Time) bool {
	if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
		return false
	}
	return p.DeletionTimestamp == nil || now.Sub(p.DeletionTimestamp.Time) <= stuckTerminationMargin
}

// checkNoPendingRescue holds a restore while a released volume of this
// workspace still awaits its rescue snapshot. Restoring earlier would start
// from an older snapshot, and the rescue would then roll the new Pod back.
func (s *Server) checkNoPendingRescue(ctx context.Context, c *caller) error {
	id := c.id
	ref := id.Namespace + "/" + id.Name
	own, _ := workspace.ResolveVolume(ctx, s.Reader, c.pod)
	var pvs corev1.PersistentVolumeList
	if err := s.Client.List(ctx, &pvs, client.MatchingFields{workspace.IndexWorkspaceRef: ref}); err != nil {
		return err
	}
	for _, pv := range pvs.Items {
		a := pv.Annotations
		if a[wsnapv1.AnnotationWorkspaceRef] != ref {
			continue
		}
		hasFinal := a[workspace.AnnotationWriterPod] != "" && a[workspace.AnnotationWriterPod] == a[workspace.AnnotationFinalPod]
		rescue := a[wsnapv1.AnnotationRescue]
		running := rescue == "running"
		awaiting := rescue == "" && !hasFinal && (pv.Status.Phase == corev1.VolumeReleased || s.claimGone(ctx, &pv))
		if running || awaiting {
			return fail(http.StatusConflict, "volume %s of workspace %s is being rescued; waiting", pv.Name, id.Name)
		}
		if rescue == "" && !hasFinal && pv.Status.Phase == corev1.VolumeBound && pv.Name != own.VolumeName {
			if err := s.checkStoppedWriterVolume(ctx, id, &pv); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkStoppedWriterVolume handles a volume that is still bound but whose
// writer stopped without a final snapshot: a Pod that failed (a hard
// eviction gives no grace period) keeps its ephemeral claim until it is
// deleted. Its data is newer than lastSnapshot. The broker asks for a
// snapshot of exactly that volume and makes restores wait for it. A
// failed rescue does not block forever: the failed request stays, the
// WsnapRescueRequestFailed alert fires (wsnap_rescue_requests_failed), and
// restores go on.
func (s *Server) checkStoppedWriterVolume(ctx context.Context, id workspace.Identity, pv *corev1.PersistentVolume) error {
	writer := pv.Annotations[workspace.AnnotationWriterPod]
	if writer == "" {
		return nil
	}
	var pods corev1.PodList
	if err := s.Reader.List(ctx, &pods, client.InNamespace(id.Namespace), client.MatchingLabels{wsnapv1.LabelWorkspace: id.Name}); err != nil {
		return upstream(err, "list workspace pods")
	}
	stuck := false
	for i := range pods.Items {
		p := &pods.Items[i]
		if string(p.UID) != writer {
			continue
		}
		if stillWriting(p, time.Now()) {
			return nil // still running: the single-writer rule applies
		}
		// Terminating far past its deadline (a lost node): its volume is
		// probably unreachable, so waiting for the rescue could take hours.
		// Ask for it anyway, but let the restore go on. When the node
		// comes back, the rescue lands in status.supersededSnapshots
		// (pinned), and the WsnapSupersededSnapshots alert fires.
		stuck = p.Status.Phase != corev1.PodSucceeded && p.Status.Phase != corev1.PodFailed
	}
	h := sha256.Sum256([]byte(pv.Name + "\x00" + writer))
	name := "wsnap-unsaved-" + hex.EncodeToString(h[:])[:10]
	var req wsnapv1.WorkspaceSnapshot
	err := s.Reader.Get(ctx, client.ObjectKey{Namespace: id.Namespace, Name: name}, &req)
	if apierrors.IsNotFound(err) {
		req = wsnapv1.WorkspaceSnapshot{
			ObjectMeta: metav1.ObjectMeta{Namespace: id.Namespace, Name: name,
				Labels:      map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue},
				Annotations: map[string]string{wsnapv1.AnnotationVolumeName: pv.Name}},
			Spec: wsnapv1.WorkspaceSnapshotSpec{WorkspaceName: id.Name},
		}
		if err := s.Client.Create(ctx, &req); err != nil && !apierrors.IsAlreadyExists(err) {
			return upstream(err, "create rescue request")
		}
		if stuck {
			s.Log.Info("restoring while a lost writer's volume holds unsaved data; its rescue will be kept as superseded",
				"workspace", id.Name, "pv", pv.Name, "request", name)
			return nil
		}
		return fail(http.StatusConflict, "volume %s of stopped pod holds unsaved data; saving it first (request %s)", pv.Name, name)
	}
	if stuck {
		return nil
	}
	if err != nil {
		return upstream(err, "rescue request %s", name)
	}
	switch req.Status.Phase {
	case wsnapv1.SnapshotSucceeded, wsnapv1.SnapshotFailed:
		return nil
	}
	return fail(http.StatusConflict, "volume %s of stopped pod holds unsaved data; waiting for request %s", pv.Name, name)
}

// trackVolume records the workspace volume, protects its PV and marks this
// Pod as the PV's latest writer.
func (s *Server) trackVolume(ctx context.Context, c *caller, consumeRestoreFrom bool) error {
	ref, err := workspace.ResolveVolume(ctx, s.Reader, c.pod)
	if err != nil {
		return volumeError(err)
	}
	if ref.Kind != workspace.KindEmptyDir && ref.VolumeName == "" {
		return fail(http.StatusConflict, "workspace claim %s is not bound to a volume yet", ref.ClaimName)
	}
	if err := s.checkNoActiveJob(ctx, c.id.Namespace, ref.VolumeName); err != nil {
		return err
	}
	// Claim the writer role with a compare-and-set: two Pods starting at
	// once both pass checkSingleWriter, but only one Update succeeds; the
	// other conflicts, re-reads, and sees the winner.
	me := &wsnapv1.PodRef{Name: c.pod.Name, UID: string(c.pod.UID)}
	var epoch int64
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var ws wsnapv1.Workspace
		if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: c.id.Name}, &ws); err != nil {
			return err
		}
		if w := ws.Status.WriterPod; w != nil && w.UID != me.UID {
			live, err := s.writerLive(ctx, ws.Namespace, w)
			if err != nil {
				return err
			}
			if live {
				return fail(http.StatusConflict, "workspace %s is written by pod %s; waiting until it stops", ws.Name, w.Name)
			}
		}
		// A snapshot credential means the volume is initialized (restored or
		// warm). A restoreFrom not used by then must not fire at some much
		// later cold restore and replace newer data.
		consume := consumeRestoreFrom && ws.Spec.RestoreFrom != nil && ws.Status.RestoreFromGeneration != ws.Generation
		sameWriter := ws.Status.WriterPod != nil && *ws.Status.WriterPod == *me
		epoch = ws.Status.WriterEpoch
		if !consume && sameWriter && ws.Status.Volume != nil && *ws.Status.Volume == ref {
			return nil
		}
		if consume {
			ws.Status.RestoreFromGeneration = ws.Generation
		}
		if !sameWriter {
			// A new epoch: data this Pod writes is newer than every snapshot
			// of earlier writers, whatever their volumes report later.
			ws.Status.WriterEpoch++
			epoch = ws.Status.WriterEpoch
		}
		ws.Status.WriterPod, ws.Status.Volume = me, &ref
		return s.Client.Status().Update(ctx, &ws)
	}); err != nil {
		return err
	}
	if err := workspace.Protect(ctx, s.Client, s.Reader, c.id, ref); err != nil {
		return fmt.Errorf("protect volume: %w", err)
	}
	if err := workspace.MarkPVAll(ctx, s.Client, s.Reader, ref.VolumeName, map[string]string{
		workspace.AnnotationWriterPod:   string(c.pod.UID),
		workspace.AnnotationWriterEpoch: strconv.FormatInt(epoch, 10),
	}); err != nil {
		return fmt.Errorf("mark volume writer: %w", err)
	}
	return nil
}

// checkNoWorkloadOnVolume makes a mover or restorer wait while a workspace
// Pod that has not stopped uses the Job's volume: its sidecar may be
// restoring into it or taking its final snapshot. It is the broker's half
// of "one agent per volume"; checkNoActiveJob is the other half.
func (s *Server) checkNoWorkloadOnVolume(ctx context.Context, c *caller) error {
	volume := c.jobObj.Annotations[wsnapv1.AnnotationVolumeName]
	if volume == "" {
		return nil
	}
	var pods corev1.PodList
	if err := s.Reader.List(ctx, &pods, client.InNamespace(c.id.Namespace), client.MatchingLabels{wsnapv1.LabelWorkspace: c.id.Name}); err != nil {
		return upstream(err, "list workspace pods")
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		ref, err := workspace.ResolveVolume(ctx, s.Reader, p)
		if err != nil {
			continue // not bound yet, or no usable volume: not this one
		}
		if ref.VolumeName == volume {
			return fail(http.StatusConflict, "pod %s uses volume %s; waiting until it stops", p.Name, volume)
		}
	}
	return nil
}

// checkNoActiveJob makes a writer wait while a wsnap Job (a mover or a
// restorer) works on the same volume. Both would write it, and a mover
// that finishes after the new writer started would mark the volume final
// without the new writer's data.
func (s *Server) checkNoActiveJob(ctx context.Context, namespace, volumeName string) error {
	if volumeName == "" {
		return nil
	}
	var jobs batchv1.JobList
	// Live: a Job created a moment ago may not be in the cache yet.
	if err := s.Reader.List(ctx, &jobs, client.InNamespace(namespace), client.MatchingLabels{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue}); err != nil {
		return err
	}
	for _, j := range jobs.Items {
		if j.Annotations[wsnapv1.AnnotationVolumeName] == volumeName && !jobDone(&j) {
			return fail(http.StatusConflict, "%s job %s is working on volume %s; waiting", j.Labels[wsnapv1.LabelJob], j.Name, volumeName)
		}
	}
	return nil
}

func jobDone(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (s *Server) updateStatus(ctx context.Context, id workspace.Identity, mutate func(*wsnapv1.WorkspaceStatus)) error {
	return s.updateStatusChecked(ctx, id, func(st *wsnapv1.WorkspaceStatus) error { mutate(st); return nil })
}

// updateStatusChecked is updateStatus with a mutate that can refuse: its
// check and its write see the same version of the status.
func (s *Server) updateStatusChecked(ctx context.Context, id workspace.Identity, mutate func(*wsnapv1.WorkspaceStatus) error) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var ws wsnapv1.Workspace
		if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: id.Namespace, Name: id.Name}, &ws); err != nil {
			return err
		}
		before := ws.Status.DeepCopy()
		if err := mutate(&ws.Status); err != nil {
			return err
		}
		if equality.Semantic.DeepEqual(before, &ws.Status) {
			return nil // no write for a no-op, e.g. every credential request
		}
		return s.Client.Status().Update(ctx, &ws)
	})
}

func (s *Server) handleReport(w http.ResponseWriter, r *http.Request) {
	err := s.report(r)
	requests.WithLabelValues("report", "", statusCode(err)).Inc()
	if err != nil {
		s.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) report(r *http.Request) error {
	ctx := r.Context()
	c, err := s.resolve(r)
	if err != nil {
		return err
	}
	var rep protocol.Report
	if err := json.NewDecoder(r.Body).Decode(&rep); err != nil || !rep.Role.Valid() {
		return fail(http.StatusBadRequest, "invalid report")
	}
	if !operationMatchesRole(rep.Operation, rep.Role) {
		return fail(http.StatusBadRequest, "operation %q cannot be reported with role %q", rep.Operation, rep.Role)
	}
	if err := c.allowed(rep.Role); err != nil && rep.Role != protocol.RoleRestore && !(c.job == "restorer" && rep.Role == protocol.RoleMover) {
		// A restore report arrives after the workload may have started.
		return err
	}
	podRef := &wsnapv1.PodRef{Name: c.pod.Name, UID: string(c.pod.UID)}
	now := metav1.Now()
	switch {
	case rep.Error != "":
		reports.WithLabelValues(string(rep.Operation), "error").Inc()
	case rep.Reused:
		reports.WithLabelValues(string(rep.Operation), "reused").Inc()
	default:
		reports.WithLabelValues(string(rep.Operation), "ok").Inc()
	}
	if rep.NoData {
		if c.job != "mover" || rep.Operation != protocol.OpSnapshot {
			return fail(http.StatusBadRequest, "only a mover reports a volume without data")
		}
		if err := s.markFinal(ctx, c, c.jobObj.Annotations[wsnapv1.AnnotationVolumeName]); err != nil {
			return err
		}
		return s.completeRequest(ctx, c, rep.Request, false, "", "", rep.Error)
	}
	if rep.Error != "" {
		s.Log.Info("agent reported failure", "workspace", c.id.Name, "operation", rep.Operation, "error", rep.Error)
		if err := s.updateStatus(ctx, c.id, func(st *wsnapv1.WorkspaceStatus) {
			st.LastError = fmt.Sprintf("%s %s by %s at %s: %s", rep.Role, rep.Operation, c.pod.Name, now.UTC().Format(time.RFC3339), rep.Error)
		}); err != nil {
			return err
		}
		return s.completeRequest(ctx, c, rep.Request, rep.Operation == protocol.OpRestore, "", "", rep.Error)
	}
	switch rep.Operation {
	case protocol.OpSnapshot:
		rec := wsnapv1.SnapshotRecord{ID: rep.SnapshotID, Time: metav1.NewTime(rep.EndTime), Pod: podRef, Final: rep.Final, Executor: "sidecar"}
		if c.job == "mover" || c.job == "restorer" {
			rec.Executor = "mover"
			rec.VolumeName = c.jobObj.Annotations[wsnapv1.AnnotationVolumeName]
			// The Job snapshots the volume's data, whose epoch the
			// controller recorded from the PV when it created the Job.
			rec.Epoch = workspace.Epoch(c.jobObj.Annotations)
		}
		if c.job == "" {
			// The reporting Pod's own volume, not status.volume, which may
			// already describe another Pod.
			ref, err := workspace.ResolveVolume(ctx, s.Reader, c.pod)
			if err != nil {
				return volumeError(err)
			}
			rec.VolumeName = ref.VolumeName
		}
		superseded := false
		if err := s.updateStatusChecked(ctx, c.id, func(st *wsnapv1.WorkspaceStatus) error {
			if c.job == "" {
				// Checked in the same read as the write: a writer change in
				// between would otherwise let a deposed writer's snapshot
				// become the newest.
				if st.WriterPod == nil || st.WriterPod.UID != string(c.pod.UID) {
					return fail(http.StatusConflict, "pod %s is not the writer of workspace %s; report refused", c.pod.Name, c.id.Name)
				}
				rec.Epoch = st.WriterEpoch
			}
			// Restores use lastSnapshot, so it must only move forward: a
			// rescue of an older writer's volume is kept in the repository
			// but does not replace a newer writer's snapshot.
			superseded = st.LastSnapshot != nil && rec.Epoch < st.LastSnapshot.Epoch
			if !superseded {
				st.LastSnapshot = &rec
			} else if !rep.Reused {
				st.SupersededSnapshots = append(st.SupersededSnapshots, rec)
				if n := len(st.SupersededSnapshots); n > maxSuperseded {
					st.SupersededSnapshots = st.SupersededSnapshots[n-maxSuperseded:]
				}
			}
			if !rep.Reused {
				st.SnapshotsSinceMaintenance++
			}
			st.LastError = ""
			return nil
		}); err != nil {
			return err
		}
		if superseded {
			s.Log.Info("snapshot of an older writer epoch kept in status.supersededSnapshots, not as the newest", "workspace", c.id.Name,
				"snapshot", rep.SnapshotID, "epoch", rec.Epoch)
		}
		if rep.Final {
			if err := s.markFinal(ctx, c, rec.VolumeName); err != nil {
				return err
			}
		}
		return s.completeRequest(ctx, c, rep.Request, false, rec.Executor, rep.SnapshotID, "")
	case protocol.OpRestore:
		executor := "sidecar"
		if c.job != "" {
			executor = c.job
		}
		if err := s.completeRequest(ctx, c, rep.Request, true, executor, rep.SnapshotID, ""); err != nil {
			return err
		}
		return s.updateStatus(ctx, c.id, func(st *wsnapv1.WorkspaceStatus) {
			st.LastRestore = &wsnapv1.RestoreRecord{SnapshotID: rep.SnapshotID, SourceWorkspace: rep.SourceWorkspace, Time: now, Pod: *podRef}
			if rep.RestoreFromGeneration > st.RestoreFromGeneration {
				st.RestoreFromGeneration = rep.RestoreFromGeneration
			}
			st.LastError = ""
		})
	case protocol.OpMaintenance:
		return s.updateStatus(ctx, c.id, func(st *wsnapv1.WorkspaceStatus) {
			st.LastMaintenance = &now
			st.SnapshotsSinceMaintenance = 0
			st.LastError = ""
		})
	}
	return fail(http.StatusBadRequest, "unknown operation %q", rep.Operation)
}

func operationMatchesRole(op protocol.Operation, role protocol.Role) bool {
	switch op {
	case protocol.OpSnapshot:
		return role == protocol.RoleSnapshot || role == protocol.RoleMover
	case protocol.OpRestore:
		return role == protocol.RoleRestore
	case protocol.OpMaintenance:
		return role == protocol.RoleMaintenance
	}
	return false
}

// markFinal records on the PV that its latest writer's data is in a
// snapshot. A sidecar covers only its own writes; a mover covers whatever
// the volume held, so it inherits the PV's writer.
func (s *Server) markFinal(ctx context.Context, c *caller, volumeName string) error {
	if volumeName == "" {
		return nil
	}
	var pv corev1.PersistentVolume
	if err := s.Reader.Get(ctx, client.ObjectKey{Name: volumeName}, &pv); err != nil {
		return client.IgnoreNotFound(err)
	}
	if ref := pv.Annotations[wsnapv1.AnnotationWorkspaceRef]; ref != c.id.Namespace+"/"+c.id.Name {
		return fail(http.StatusForbidden, "volume %s does not belong to workspace %s", volumeName, c.id.Name)
	}
	uid := string(c.pod.UID)
	if c.job != "" {
		// The writer recorded when the Job was created: the Job's snapshot
		// holds that writer's data and nothing written after.
		uid = c.jobObj.Annotations[workspace.AnnotationWriterPod]
		if uid == "" {
			uid = "mover"
		}
	}
	return workspace.MarkPV(ctx, s.Client, s.Reader, volumeName, workspace.AnnotationFinalPod, uid)
}

// claimedRestore returns the WorkspaceRestore the caller may serve: it is
// for the caller's workspace, Running, and claimed by this Pod (sidecar) or
// named by this Job (restorer).
func (s *Server) claimedRestore(ctx context.Context, c *caller, name string) (*wsnapv1.WorkspaceRestore, error) {
	var r wsnapv1.WorkspaceRestore
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: name}, &r); apierrors.IsNotFound(err) {
		return nil, fail(http.StatusForbidden, "restore request %s: %v", name, err)
	} else if err != nil {
		return nil, upstream(err, "restore request %s", name)
	}
	if r.Spec.WorkspaceName != c.id.Name || r.Status.Phase != wsnapv1.SnapshotRunning {
		return nil, fail(http.StatusForbidden, "restore request %s is not running for workspace %s", name, c.id.Name)
	}
	switch c.job {
	case "":
		if r.Status.ClaimedBy != string(c.pod.UID) {
			return nil, fail(http.StatusForbidden, "restore request %s is claimed by another pod", name)
		}
	case "restorer":
		if c.jobObj.Annotations[wsnapv1.AnnotationRequest] != name {
			return nil, fail(http.StatusForbidden, "job %s does not serve restore request %s", c.jobObj.Name, name)
		}
	default:
		return nil, fail(http.StatusForbidden, "%s jobs cannot restore", c.job)
	}
	return &r, nil
}

// requestObject loads a request of either kind. Both share the status type.
func (s *Server) requestObject(ctx context.Context, ns, name string, restore bool) (client.Object, *wsnapv1.WorkspaceSnapshotStatus, string, error) {
	if restore {
		var r wsnapv1.WorkspaceRestore
		err := s.Reader.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &r)
		return &r, &r.Status, r.Spec.WorkspaceName, err
	}
	var r wsnapv1.WorkspaceSnapshot
	err := s.Reader.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &r)
	return &r, &r.Status, r.Spec.WorkspaceName, err
}

// claimGone reports whether a Bound PV's claim is deleted or being
// deleted: the PV controller marks the PV Released only a moment later.
func (s *Server) claimGone(ctx context.Context, pv *corev1.PersistentVolume) bool {
	ref := pv.Spec.ClaimRef
	if ref == nil || pv.Status.Phase != corev1.VolumeBound {
		return false
	}
	var pvc corev1.PersistentVolumeClaim
	err := s.Reader.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, &pvc)
	return apierrors.IsNotFound(err) || (err == nil && (pvc.UID != ref.UID || pvc.DeletionTimestamp != nil))
}

// completeRequest records the outcome of a WorkspaceSnapshot (restore
// false) or WorkspaceRestore (restore true). The request must belong to the
// caller's workspace; terminal outcomes are never rewritten.
func (s *Server) completeRequest(ctx context.Context, c *caller, name string, restore bool, executor, snapshotID, errMsg string) error {
	if name == "" {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		obj, st, wsName, err := s.requestObject(ctx, c.id.Namespace, name, restore)
		if err != nil {
			return client.IgnoreNotFound(err)
		}
		if wsName != c.id.Name {
			return fail(http.StatusForbidden, "request %s belongs to workspace %s", name, wsName)
		}
		switch {
		case c.job == "" && st.ClaimedBy != string(c.pod.UID):
			return fail(http.StatusForbidden, "request %s was not claimed by pod %s", name, c.pod.Name)
		case c.job != "" && c.jobObj.Annotations[wsnapv1.AnnotationRequest] != name:
			return fail(http.StatusForbidden, "job %s does not serve request %s", c.jobObj.Name, name)
		}
		if st.Phase == wsnapv1.SnapshotSucceeded || st.Phase == wsnapv1.SnapshotFailed {
			return nil
		}
		now := metav1.Now()
		st.CompletionTime = &now
		if errMsg != "" {
			st.Phase, st.Message = wsnapv1.SnapshotFailed, errMsg
		} else {
			st.Phase, st.SnapshotID, st.Executor, st.Message = wsnapv1.SnapshotSucceeded, snapshotID, executor, ""
		}
		return s.Client.Status().Update(ctx, obj)
	})
}

// waitPoll bounds how long POST /v1/wait holds a request open. recheck is
// the fallback interval in case a notification was missed.
var (
	waitPoll = 50 * time.Second
	recheck  = 10 * time.Second
)

// handleWait long-polls for requests addressed to the caller's workspace.
// Only the workspace's current writer receives them, so a request is served
// by exactly one Pod. Claiming moves a request to Running with claimedBy
// set; the status update carries the cached resourceVersion, so two broker
// replicas cannot claim the same request.
func (s *Server) handleWait(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	c, err := s.resolve(r)
	if err == nil && c.job != "" {
		err = fail(http.StatusForbidden, "jobs do not wait for requests")
	}
	if err != nil {
		requests.WithLabelValues("wait", "", statusCode(err)).Inc()
		s.writeError(w, err)
		return
	}
	var wake <-chan struct{}
	if s.Hub != nil {
		ch, unsubscribe := s.Hub.Subscribe(HubKey(c.id.Namespace, c.id.Name))
		defer unsubscribe()
		wake = ch
	}
	deadline := time.NewTimer(waitPoll)
	defer deadline.Stop()
	var resp protocol.WaitResponse
	for {
		resp, err = s.claimRequests(ctx, c)
		if err != nil || len(resp.Requests)+len(resp.Restores) > 0 {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
		case <-wake:
			continue
		case <-time.After(recheck):
			continue
		}
		break
	}
	requests.WithLabelValues("wait", "", statusCode(err)).Inc()
	if err != nil {
		s.writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) claimRequests(ctx context.Context, c *caller) (protocol.WaitResponse, error) {
	var out protocol.WaitResponse
	var ws wsnapv1.Workspace
	if err := s.Client.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: c.id.Name}, &ws); err != nil {
		return out, client.IgnoreNotFound(err)
	}
	if ws.Status.WriterPod == nil || ws.Status.WriterPod.UID != string(c.pod.UID) {
		return out, nil
	}
	// A long poll can outlive its Pod: when a Pod's network namespace is
	// torn down, the TCP connection may never be closed, so the handler's
	// context stays alive. Claim nothing for a Pod that is gone or going.
	var live corev1.Pod
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.pod.Namespace, Name: c.pod.Name}, &live); err != nil || live.UID != c.pod.UID ||
		live.DeletionTimestamp != nil || live.Status.Phase != corev1.PodRunning {
		return out, nil
	}
	byWorkspace := []client.ListOption{client.InNamespace(c.id.Namespace), client.MatchingFields{workspace.IndexWorkspaceName: c.id.Name}}
	claim := func(obj client.Object, st *wsnapv1.WorkspaceSnapshotStatus) (bool, error) {
		if obj.GetAnnotations()[wsnapv1.AnnotationVolumeName] != "" {
			// Targets a specific (stopped writer's) volume: only a Job can
			// reach it; a sidecar would snapshot its own volume instead.
			return false, nil
		}
		if st.Phase == wsnapv1.SnapshotRunning && st.Executor == "sidecar" && st.ClaimedBy == string(c.pod.UID) {
			// Claimed by this Pod but maybe never delivered: the answer to
			// an earlier poll can be lost, or the sidecar restarted. Deliver
			// it again; the sidecar ignores names it already serves.
			return true, nil
		}
		if st.Phase != "" && st.Phase != wsnapv1.SnapshotPending {
			return false, nil
		}
		st.Phase, st.Executor, st.ClaimedBy = wsnapv1.SnapshotRunning, "sidecar", string(c.pod.UID)
		st.Message = "claimed by pod " + c.pod.Name
		if err := s.Client.Status().Update(ctx, obj); err != nil {
			if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
				return false, nil // someone else claimed or deleted it
			}
			return false, err
		}
		return true, nil
	}
	var snaps wsnapv1.WorkspaceSnapshotList
	if err := s.Client.List(ctx, &snaps, byWorkspace...); err != nil {
		return out, err
	}
	for i := range snaps.Items {
		if ok, err := claim(&snaps.Items[i], &snaps.Items[i].Status); err != nil {
			return out, err
		} else if ok {
			out.Requests = append(out.Requests, snaps.Items[i].Name)
		}
	}
	var restores wsnapv1.WorkspaceRestoreList
	if err := s.Client.List(ctx, &restores, byWorkspace...); err != nil {
		return out, err
	}
	for i := range restores.Items {
		if ok, err := claim(&restores.Items[i], &restores.Items[i].Status); err != nil {
			return out, err
		} else if ok {
			out.Restores = append(out.Restores, restores.Items[i].Name)
		}
	}
	return out, nil
}
