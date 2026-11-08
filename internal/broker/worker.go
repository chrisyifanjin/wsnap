package broker

import (
	"context"
	"encoding/json"
	"net/http"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/protocol"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// handleWorkerState lets a separate worker end only after its workload Pod
// has stopped. A Job terminated for some other reason must not claim that
// its snapshot is final while the workload can still write.
func (s *Server) handleWorkerState(w http.ResponseWriter, r *http.Request) {
	c, err := s.resolve(r)
	if err != nil {
		s.writeError(w, err)
		return
	}
	if c.job != "worker" {
		s.writeError(w, fail(http.StatusForbidden, "only a worker job may read worker state"))
		return
	}
	// Gone is not stopped: a force-deleted Pod's object disappears while
	// the kubelet still runs its containers for up to their grace period.
	// The worker waits that long before it marks a snapshot final.
	gone := c.workloadPod == nil
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(protocol.WorkerState{Stop: gone || workloadStopped(c.workloadPod), Gone: gone})
}

// workloadStopped reports whether every workload container has exited.
func workloadStopped(p *corev1.Pod) bool {
	if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
		return true
	}
	if p.DeletionTimestamp == nil || len(p.Status.ContainerStatuses) < len(p.Spec.Containers) {
		return false
	}
	for _, cs := range p.Status.ContainerStatuses {
		if cs.State.Terminated == nil {
			return false
		}
	}
	return true
}

// bindWorkerEpoch stores the epoch in Workspace status, not editable Job
// metadata. A late report from an old worker can then be ordered against a
// newer writer without trusting the Job's annotations.
func (s *Server) bindWorkerEpoch(ctx context.Context, c *caller) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var ws wsnapv1.Workspace
		if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: c.id.Namespace, Name: c.id.Name}, &ws); err != nil {
			return err
		}
		uid := c.jobObj.Annotations[wsnapv1.AnnotationTargetPodUID]
		if ws.Status.WriterPod == nil || ws.Status.WriterPod.UID != uid || ws.Status.WriterEpoch == 0 {
			return fail(http.StatusConflict, "worker target pod %s has not claimed the writer epoch", uid)
		}
		for i := range ws.Status.Jobs {
			ref := &ws.Status.Jobs[i]
			if ref.UID != string(c.jobObj.UID) {
				continue
			}
			if ref.WriterEpoch == ws.Status.WriterEpoch {
				c.workerEpoch = ref.WriterEpoch
				return nil
			}
			if ref.WriterEpoch != 0 {
				return fail(http.StatusConflict, "worker job %s belongs to an older writer epoch", c.jobObj.Name)
			}
			ref.WriterEpoch = ws.Status.WriterEpoch
			if err := s.Client.Status().Update(ctx, &ws); err != nil {
				return err
			}
			c.workerEpoch = ref.WriterEpoch
			return nil
		}
		return fail(http.StatusForbidden, "worker job %s is no longer registered", c.jobObj.Name)
	})
}
