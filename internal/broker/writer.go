package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"time"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

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
		hasFinal := workspace.HasFinalSnapshot(a)
		rescue := a[wsnapv1.AnnotationRescue]
		running := rescue == wsnapv1.RescueRunning
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
	selfJob := ""
	if c.job == wsnapv1.JobWorker && c.jobObj != nil {
		selfJob = string(c.jobObj.UID)
	}
	if err := s.checkNoActiveJob(ctx, c.id.Namespace, ref.VolumeName, selfJob); err != nil {
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
func (s *Server) checkNoActiveJob(ctx context.Context, namespace, volumeName, selfJobUID string) error {
	if volumeName == "" {
		return nil
	}
	var jobs batchv1.JobList
	// Live: a Job created a moment ago may not be in the cache yet.
	if err := s.Reader.List(ctx, &jobs, client.InNamespace(namespace), client.MatchingLabels{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue}); err != nil {
		return err
	}
	for _, j := range jobs.Items {
		if (selfJobUID == "" || string(j.UID) != selfJobUID) && j.Annotations[wsnapv1.AnnotationVolumeName] == volumeName && !jobDone(&j) {
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
