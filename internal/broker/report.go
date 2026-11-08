package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/protocol"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

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
	if err := c.allowed(rep.Role); err != nil && rep.Role != protocol.RoleRestore && !(c.job == wsnapv1.JobRestorer && rep.Role == protocol.RoleMover) {
		// A restore report arrives after the workload may have started.
		return err
	}
	podRef := &wsnapv1.PodRef{Name: c.pod.Name, UID: string(c.pod.UID)}
	if c.job == wsnapv1.JobWorker {
		podRef = &wsnapv1.PodRef{Name: c.workerRef.TargetPodName, UID: c.workerRef.TargetPodUID}
	}
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
		if c.job != wsnapv1.JobMover || rep.Operation != protocol.OpSnapshot {
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
		return s.recordSnapshot(ctx, c, rep, podRef)
	case protocol.OpRestore:
		return s.recordRestore(ctx, c, rep, podRef, now)
	case protocol.OpMaintenance:
		return s.updateStatus(ctx, c.id, func(st *wsnapv1.WorkspaceStatus) {
			st.LastMaintenance = &now
			st.SnapshotsSinceMaintenance = 0
			st.LastError = ""
		})
	}
	return fail(http.StatusBadRequest, "unknown operation %q", rep.Operation)
}

// snapshotRecord describes a reported snapshot: who took it, of which
// volume, and in which writer epoch (a sidecar's epoch is filled in from
// Workspace status, in the same read as the write).
func (s *Server) snapshotRecord(ctx context.Context, c *caller, rep protocol.Report, podRef *wsnapv1.PodRef) (wsnapv1.SnapshotRecord, error) {
	rec := wsnapv1.SnapshotRecord{ID: rep.SnapshotID, Time: metav1.NewTime(rep.EndTime), Pod: podRef, Final: rep.Final, Executor: wsnapv1.ExecutorSidecar}
	switch c.job {
	case wsnapv1.JobWorker:
		if c.workerEpoch == 0 {
			return rec, fail(http.StatusConflict, "worker epoch has not been recorded")
		}
		rec.Executor = wsnapv1.ExecutorWorker
		rec.VolumeName = c.jobObj.Annotations[wsnapv1.AnnotationVolumeName]
		rec.Epoch = c.workerEpoch
	case wsnapv1.JobMover, wsnapv1.JobRestorer:
		rec.Executor = wsnapv1.ExecutorMover
		rec.VolumeName = c.jobObj.Annotations[wsnapv1.AnnotationVolumeName]
		// The Job snapshots the volume's data, whose epoch the controller
		// recorded from the PV when it created the Job.
		rec.Epoch = workspace.Epoch(c.jobObj.Annotations)
	case "":
		// The reporting Pod's own volume, not status.volume, which may
		// already describe another Pod.
		ref, err := workspace.ResolveVolume(ctx, s.Reader, c.pod)
		if err != nil {
			return rec, volumeError(err)
		}
		rec.VolumeName = ref.VolumeName
	}
	return rec, nil
}

func (s *Server) recordSnapshot(ctx context.Context, c *caller, rep protocol.Report, podRef *wsnapv1.PodRef) error {
	rec, err := s.snapshotRecord(ctx, c, rep, podRef)
	if err != nil {
		return err
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
		superseded = recordInEpochOrder(st, rec, rep.Reused)
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
}

// recordInEpochOrder makes rec the newest snapshot unless it belongs to an
// older writer epoch. Restores use lastSnapshot, so it must only move
// forward: a rescue of an older writer's volume is kept (pinned, in
// supersededSnapshots) but does not replace a newer writer's snapshot.
// It reports whether rec was superseded.
func recordInEpochOrder(st *wsnapv1.WorkspaceStatus, rec wsnapv1.SnapshotRecord, reused bool) bool {
	if st.LastSnapshot == nil || rec.Epoch >= st.LastSnapshot.Epoch {
		st.LastSnapshot = &rec
		return false
	}
	if !reused {
		st.SupersededSnapshots = append(st.SupersededSnapshots, rec)
		if n := len(st.SupersededSnapshots); n > maxSuperseded {
			st.SupersededSnapshots = st.SupersededSnapshots[n-maxSuperseded:]
		}
	}
	return true
}

func (s *Server) recordRestore(ctx context.Context, c *caller, rep protocol.Report, podRef *wsnapv1.PodRef, now metav1.Time) error {
	executor := wsnapv1.ExecutorSidecar
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
		if c.job == wsnapv1.JobWorker {
			uid = c.workerRef.TargetPodUID // the annotation is editable
		}
		if uid == "" {
			uid = workspace.FinalByMover
		}
	}
	return workspace.MarkPV(ctx, s.Client, s.Reader, volumeName, workspace.AnnotationFinalPod, uid)
}
