// Package broker serves credentials to wsnap agents. An agent proves which
// Pod it runs in with a projected service account token. The broker maps
// the Pod to its workspace, decides which roles the Pod may have, and
// returns a credential scoped to that workspace and role.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
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
	ident, err := s.Auth.Authenticate(ctx, token)
	if err != nil {
		return nil, fail(http.StatusUnauthorized, "%v", err)
	}
	var pod corev1.Pod
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: ident.Namespace, Name: ident.PodName}, &pod); err != nil {
		return nil, fail(http.StatusForbidden, "pod %s/%s: %v", ident.Namespace, ident.PodName, err)
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
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.pod.Namespace, Name: owner.Name}, &j); err != nil {
		return nil, fail(http.StatusForbidden, "job %s: %v", owner.Name, err)
	}
	if j.UID != owner.UID || j.Labels[wsnapv1.LabelManagedBy] != wsnapv1.ManagedByValue || j.Labels[wsnapv1.LabelJob] != job {
		return nil, fail(http.StatusForbidden, "job %s is not a wsnap %s job", j.Name, job)
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
	return c, nil
}

// checkWorkloadIsolation refuses Pods whose workload containers could
// request credentials themselves. The agent's token must be mounted only
// into the sidecar.
func (s *Server) checkWorkloadIsolation(pod *corev1.Pod) error {
	tokenVolumes := map[string]bool{}
	for _, v := range pod.Spec.Volumes {
		if v.Projected == nil {
			continue
		}
		for _, src := range v.Projected.Sources {
			if src.ServiceAccountToken != nil && src.ServiceAccountToken.Audience == s.Audience {
				tokenVolumes[v.Name] = true
			}
		}
	}
	if ptr.Deref(pod.Spec.ShareProcessNamespace, false) || pod.Spec.HostPID {
		return fail(http.StatusForbidden, "pod %s shares a process namespace; the workload could read the agent's secrets through /proc", pod.Name)
	}
	sidecar := workspace.SidecarName(pod)
	for _, ec := range pod.Spec.EphemeralContainers {
		if ec.TargetContainerName == sidecar {
			return fail(http.StatusForbidden, "ephemeral container %q shares the %q container's process namespace", ec.Name, sidecar)
		}
		for _, m := range ec.VolumeMounts {
			if tokenVolumes[m.Name] {
				return fail(http.StatusForbidden, "ephemeral container %q mounts the wsnap token", ec.Name)
			}
		}
	}
	for _, list := range [][]corev1.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
		for _, ctr := range list {
			if ctr.Name == sidecar {
				continue
			}
			for _, m := range ctr.VolumeMounts {
				if tokenVolumes[m.Name] {
					return fail(http.StatusForbidden, "container %q mounts the wsnap token volume %q; mount it only into the %q container", ctr.Name, m.Name, sidecar)
				}
			}
		}
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
	var repo wsnapv1.SnapshotRepository
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: c.id.Repository}, &repo); err != nil {
		return nil, fail(http.StatusFailedDependency, "snapshot repository %s/%s: %v", c.id.Namespace, c.id.Repository, err)
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
			if err := s.checkNoPendingRescue(ctx, c.id); err != nil {
				return nil, err
			}
		}
		// Protect the volume before handing out any credential, so data
		// the Pod writes is never reclaimed without a final snapshot.
		if err := s.trackVolume(ctx, c); err != nil {
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
				return nil, fail(http.StatusFailedDependency, "restoreFrom workspace %s: %v", rf.WorkspaceName, err)
			}
			if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: src.Spec.RepositoryName}, &repo); err != nil {
				return nil, fail(http.StatusFailedDependency, "repository of workspace %s: %v", rf.WorkspaceName, err)
			}
			target, source = rf.WorkspaceName, rf.WorkspaceName
		}
	}
	var secret corev1.Secret
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: repo.Namespace, Name: repo.Spec.Credentials.SecretName}, &secret); err != nil {
		return nil, fail(http.StatusFailedDependency, "credentials secret: %v", err)
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
		return nil, fail(http.StatusBadGateway, "issue credential: %v", err)
	}
	password, err := credentials.RepositoryPassword(secret.Data[credentials.KeyMasterKey], c.id.Namespace, target)
	if err != nil {
		return nil, fail(http.StatusFailedDependency, "%v", err)
	}
	s.Log.Info("issued credential", "namespace", c.id.Namespace, "workspace", c.id.Name, "role", req.Role,
		"pod", c.pod.Name, "scoped", creds.Scoped, "expires", creds.ExpiresAt)
	return &protocol.CredentialsResponse{
		Namespace: c.id.Namespace, Workspace: c.id.Name, Role: req.Role,
		Engine: eng, Backend: backend, Credentials: creds, RepositoryPassword: password,
		Interval:   protocol.Duration{Duration: workspace.Interval(&repo)},
		KeepLast:   workspace.KeepLast(&repo),
		SnapshotID: snapshotID, SourceWorkspace: source, RestoreFromGeneration: restoreGen,
	}, nil
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
		if err := s.Client.Create(ctx, &ws); err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, fail(http.StatusInternalServerError, "create workspace: %v", err)
		}
		return &ws, nil
	}
	if err != nil {
		return nil, err
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
func (s *Server) checkNoPendingRescue(ctx context.Context, id workspace.Identity) error {
	ref := id.Namespace + "/" + id.Name
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
	}
	return nil
}

// trackVolume records the workspace volume, protects its PV and marks this
// Pod as the PV's latest writer.
func (s *Server) trackVolume(ctx context.Context, c *caller) error {
	ref, err := workspace.ResolveVolume(ctx, s.Reader, c.pod)
	if err != nil {
		return fail(http.StatusFailedDependency, "%v", err)
	}
	if err := s.checkNoActiveJob(ctx, c.id.Namespace, ref.VolumeName); err != nil {
		return err
	}
	// Claim the writer role with a compare-and-set: two Pods starting at
	// once both pass checkSingleWriter, but only one Update succeeds; the
	// other conflicts, re-reads, and sees the winner.
	me := &wsnapv1.PodRef{Name: c.pod.Name, UID: string(c.pod.UID)}
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
		if ws.Status.WriterPod != nil && *ws.Status.WriterPod == *me && ws.Status.Volume != nil && *ws.Status.Volume == ref {
			return nil
		}
		ws.Status.WriterPod, ws.Status.Volume = me, &ref
		return s.Client.Status().Update(ctx, &ws)
	}); err != nil {
		return err
	}
	if err := workspace.Protect(ctx, s.Client, s.Reader, c.id, ref); err != nil {
		return fmt.Errorf("protect volume: %w", err)
	}
	if err := workspace.MarkPV(ctx, s.Client, s.Reader, ref.VolumeName, workspace.AnnotationWriterPod, string(c.pod.UID)); err != nil {
		return fmt.Errorf("mark volume writer: %w", err)
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
	if err := s.Client.List(ctx, &jobs, client.InNamespace(namespace), client.MatchingLabels{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue}); err != nil {
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
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var ws wsnapv1.Workspace
		if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: id.Namespace, Name: id.Name}, &ws); err != nil {
			return err
		}
		before := ws.Status.DeepCopy()
		mutate(&ws.Status)
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
		}
		if c.job == "" {
			// The reporting Pod's own volume, not status.volume, which may
			// already describe another Pod.
			ref, err := workspace.ResolveVolume(ctx, s.Reader, c.pod)
			if err != nil {
				return fail(http.StatusFailedDependency, "%v", err)
			}
			rec.VolumeName = ref.VolumeName
		}
		if err := s.updateStatus(ctx, c.id, func(st *wsnapv1.WorkspaceStatus) {
			st.LastSnapshot = &rec
			if !rep.Reused {
				st.SnapshotsSinceMaintenance++
			}
			st.LastError = ""
		}); err != nil {
			return err
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
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: name}, &r); err != nil {
		return nil, fail(http.StatusForbidden, "restore request %s: %v", name, err)
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
