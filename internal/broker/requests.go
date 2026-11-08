package broker

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/protocol"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

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
	case wsnapv1.JobRestorer:
		if c.jobObj.Annotations[wsnapv1.AnnotationRequest] != name {
			return nil, fail(http.StatusForbidden, "job %s does not serve restore request %s", c.jobObj.Name, name)
		}
	case wsnapv1.JobWorker:
		if r.Status.ClaimedBy != string(c.pod.UID) {
			return nil, fail(http.StatusForbidden, "restore request %s is claimed by another worker", name)
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
		case c.job == wsnapv1.JobWorker && st.ClaimedBy != string(c.pod.UID):
			return fail(http.StatusForbidden, "request %s was not claimed by worker %s", name, c.pod.Name)
		case c.job != "" && c.job != wsnapv1.JobWorker && c.jobObj.Annotations[wsnapv1.AnnotationRequest] != name:
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
	if err == nil && c.job != "" && c.job != wsnapv1.JobWorker {
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
	writerUID := string(c.pod.UID)
	livePod := c.pod
	if c.job == wsnapv1.JobWorker {
		writerUID = c.jobObj.Annotations[wsnapv1.AnnotationTargetPodUID]
		livePod = c.workloadPod
	}
	if ws.Status.WriterPod == nil || ws.Status.WriterPod.UID != writerUID || livePod == nil {
		return out, nil
	}
	// A long poll can outlive its Pod: when a Pod's network namespace is
	// torn down, the TCP connection may never be closed, so the handler's
	// context stays alive. Claim nothing for a Pod that is gone or going.
	var live corev1.Pod
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: livePod.Namespace, Name: livePod.Name}, &live); err != nil || live.UID != livePod.UID ||
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
		executor := wsnapv1.ExecutorSidecar
		if c.job == wsnapv1.JobWorker {
			executor = wsnapv1.ExecutorWorker
		}
		if st.Phase == wsnapv1.SnapshotRunning && st.Executor == executor && st.ClaimedBy == string(c.pod.UID) {
			// Claimed by this Pod but maybe never delivered: the answer to
			// an earlier poll can be lost, or the sidecar restarted. Deliver
			// it again; the sidecar ignores names it already serves.
			return true, nil
		}
		if st.Phase != "" && st.Phase != wsnapv1.SnapshotPending {
			return false, nil
		}
		st.Phase, st.Executor, st.ClaimedBy = wsnapv1.SnapshotRunning, executor, string(c.pod.UID)
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
