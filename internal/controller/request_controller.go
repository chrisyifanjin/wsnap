package controller

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
)

// RequestTimeout fails a request that no executor took within this time.
var RequestTimeout = 10 * time.Minute

// RunningTimeout fails a request a sidecar claimed but did not finish.
var RunningTimeout = 2 * time.Hour

// RequestReconciler routes WorkspaceSnapshot requests (Restore false) or
// WorkspaceRestore requests (Restore true) the same way. If the
// workspace's writer Pod runs, its sidecar claims the request through the
// broker's /v1/wait long poll and does the work in the Pod. If no Pod runs
// and the workspace volume is a PVC that nobody mounts, a Job does it on
// the idle volume (mover or restorer). Otherwise the request fails with the
// reason.
type RequestReconciler struct {
	client.Client
	Reader  client.Reader
	Jobs    JobConfig
	Restore bool
}

// requestObj is a request with its object methods.
type requestObj interface {
	client.Object
	wsnapv1.Request
}

func (r *RequestReconciler) newObj() requestObj {
	if r.Restore {
		return &wsnapv1.WorkspaceRestore{}
	}
	return &wsnapv1.WorkspaceSnapshot{}
}

func (r *RequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	obj := r.newObj()
	if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	sr := request{obj}
	switch sr.Status().Phase {
	case wsnapv1.SnapshotSucceeded, wsnapv1.SnapshotFailed:
		return r.cleanupFinishedJob(ctx, sr)
	}
	if st := sr.Status(); st.Phase == wsnapv1.SnapshotRunning && (st.Executor == "sidecar" || st.Executor == "worker") &&
		time.Since(sr.GetCreationTimestamp().Time) > RunningTimeout {
		return ctrl.Result{}, r.finish(ctx, sr, fmt.Sprintf("the live executor did not finish within %s", RunningTimeout))
	}
	// Only a request that still waits for an executor times out here: a
	// running Job is bounded by its activeDeadlineSeconds, and cutting off
	// a running restore would leave the request Failed while it completes.
	if sr.Status().Phase != wsnapv1.SnapshotRunning && time.Since(sr.GetCreationTimestamp().Time) > RequestTimeout {
		// Stop a Job that is still running: a request that reads Failed
		// must not keep changing the volume, and a retry must not run a
		// second Job beside it.
		bg := metav1.DeletePropagationBackground
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: sr.GetNamespace(), Name: requestJobName(sr)}}
		if err := r.Delete(ctx, job, &client.DeleteOptions{PropagationPolicy: &bg}); client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.finish(ctx, sr, fmt.Sprintf("no executor finished within %s", RequestTimeout))
	}
	recheck := ctrl.Result{RequeueAfter: time.Until(sr.GetCreationTimestamp().Add(RequestTimeout)) + time.Second}

	if sr.Status().Phase == wsnapv1.SnapshotRunning && sr.Status().Executor == r.jobKind() {
		return r.checkJob(ctx, sr, recheck)
	}
	targeted := sr.obj.GetAnnotations()[wsnapv1.AnnotationVolumeName] != "" && !r.Restore
	// Live: routing on a stale writer could start a Job beside a sidecar.
	var ws wsnapv1.Workspace
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: sr.GetNamespace(), Name: sr.GetWorkspaceName()}, &ws); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.finish(ctx, sr, "workspace not found")
		}
		return ctrl.Result{}, err
	}
	if targeted {
		// A specific volume (a stopped writer's): only a Job can reach it.
		return r.startJob(ctx, sr, &ws, recheck)
	}
	if st := sr.Status(); st.Phase == wsnapv1.SnapshotRunning && (st.Executor == "sidecar" || st.Executor == "worker") {
		// A terminating live executor still finishes what it claimed.
		alive, err := r.claimantAlive(ctx, sr.GetNamespace(), ws.Name, st.ClaimedBy)
		if err != nil {
			return ctrl.Result{}, err
		}
		if alive {
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
	}
	live, err := r.liveWriter(ctx, &ws)
	if err != nil {
		return ctrl.Result{}, err
	}
	if live {
		// The sidecar claims it (Pending → Running) through /v1/wait.
		if sr.Status().Phase == "" {
			sr.Status().Phase = wsnapv1.SnapshotPending
			return recheck, r.Status().Update(ctx, sr.obj)
		}
		return recheck, nil
	}
	if sr.Status().Phase == wsnapv1.SnapshotRunning {
		// A sidecar claimed it, but its Pod stopped before reporting.
		// Snapshots and restores are idempotent (a restorer finishes any
		// interrupted swap first), so route the request again; the request
		// timeout bounds the retries.
		st := sr.Status()
		st.Phase, st.ClaimedBy, st.Executor = wsnapv1.SnapshotPending, "", ""
		st.Message = "the pod that claimed the request stopped before reporting; routing it again"
		return ctrl.Result{Requeue: true}, r.Status().Update(ctx, sr.obj)
	}
	return r.startJob(ctx, sr, &ws, recheck)
}

// claimantAlive reports whether the Pod with UID uid of workspace wsName
// still exists and has not stopped. It reads live.
func (r *RequestReconciler) claimantAlive(ctx context.Context, namespace, wsName, uid string) (bool, error) {
	if uid == "" {
		return false, nil
	}
	// The claimant is a workspace Pod (sidecar) or a worker Job's Pod.
	for _, sel := range []client.MatchingLabels{{wsnapv1.LabelWorkspace: wsName}, {wsnapv1.LabelJob: "worker", wsnapv1.LabelJobWorkspace: wsName}} {
		var pods corev1.PodList
		if err := r.Reader.List(ctx, &pods, client.InNamespace(namespace), sel); err != nil {
			return false, err
		}
		for _, p := range pods.Items {
			if string(p.UID) == uid {
				return p.Status.Phase != corev1.PodSucceeded && p.Status.Phase != corev1.PodFailed, nil
			}
		}
	}
	return false, nil
}

// cleanupFinishedJob releases a request's PVC as soon as the agent exits.
// A completed Job's Pod still counts as a PVC user until the Job is removed;
// leaving it for the one-hour TTL can block claim deletion and reprovisioning.
// Wait for the Job to finish because the agent reports before its final local
// cleanup (for example, a restorer writes the initialized marker).
func (r *RequestReconciler) cleanupFinishedJob(ctx context.Context, sr request) (ctrl.Result, error) {
	if sr.Status().Executor != r.jobKind() {
		return ctrl.Result{}, nil
	}
	var job batchv1.Job
	err := r.Reader.Get(ctx, types.NamespacedName{Namespace: sr.GetNamespace(), Name: requestJobName(sr)}, &job)
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if job.Labels[wsnapv1.LabelJob] != r.jobKind() || job.Annotations[wsnapv1.AnnotationRequest] != sr.GetName() {
		// Not this request's Job (see createJob for same-name Jobs wsnap
		// did not create). Leave it; retrying would never succeed.
		return ctrl.Result{}, nil
	}
	if !jobFinished(&job) {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	bg := metav1.DeletePropagationBackground
	return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, &job, &client.DeleteOptions{PropagationPolicy: &bg}))
}

// request wraps a request object for brevity.
type request struct{ obj requestObj }

func (q request) Status() *wsnapv1.WorkspaceSnapshotStatus { return q.obj.GetRequestStatus() }
func (q request) GetWorkspaceName() string                 { return q.obj.GetWorkspaceName() }
func (q request) GetNamespace() string                     { return q.obj.GetNamespace() }
func (q request) GetName() string                          { return q.obj.GetName() }
func (q request) GetUID() types.UID                        { return q.obj.GetUID() }
func (q request) GetCreationTimestamp() metav1.Time        { return q.obj.GetCreationTimestamp() }

func (r *RequestReconciler) jobKind() string {
	if r.Restore {
		return "restorer"
	}
	return "mover"
}

func (r *RequestReconciler) liveWriter(ctx context.Context, ws *wsnapv1.Workspace) (bool, error) {
	w := ws.Status.WriterPod
	if w == nil {
		return false, nil
	}
	var pod corev1.Pod
	err := r.Reader.Get(ctx, types.NamespacedName{Namespace: ws.Namespace, Name: w.Name}, &pod)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return string(pod.UID) == w.UID && pod.DeletionTimestamp == nil && pod.Status.Phase == corev1.PodRunning, nil
}

func (r *RequestReconciler) startJob(ctx context.Context, sr request, ws *wsnapv1.Workspace, recheck ctrl.Result) (ctrl.Result, error) {
	vol := ws.Status.Volume
	if pvName := sr.obj.GetAnnotations()[wsnapv1.AnnotationVolumeName]; pvName != "" && !r.Restore {
		var pv corev1.PersistentVolume
		if err := r.Reader.Get(ctx, types.NamespacedName{Name: pvName}, &pv); err != nil {
			if apierrors.IsNotFound(err) {
				return ctrl.Result{}, r.finish(ctx, sr, fmt.Sprintf("volume %s no longer exists", pvName))
			}
			return ctrl.Result{}, err
		}
		if pv.Annotations[wsnapv1.AnnotationWorkspaceRef] != ws.Namespace+"/"+ws.Name || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != ws.Namespace {
			return ctrl.Result{}, r.finish(ctx, sr, fmt.Sprintf("volume %s is not bound to a claim of workspace %s", pvName, ws.Name))
		}
		vol = &wsnapv1.VolumeRef{Kind: workspace.KindPVC, ClaimName: pv.Spec.ClaimRef.Name,
			ClaimUID: string(pv.Spec.ClaimRef.UID), VolumeName: pvName, VolumeUID: string(pv.UID)}
	}
	if vol == nil || vol.ClaimName == "" || vol.Kind == workspace.KindEmptyDir {
		return ctrl.Result{}, r.finish(ctx, sr, "no pod is running and the workspace has no persistent volume")
	}
	var claim corev1.PersistentVolumeClaim
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: ws.Namespace, Name: vol.ClaimName}, &claim); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.finish(ctx, sr, fmt.Sprintf("volume claim %s no longer exists", vol.ClaimName))
		}
		return ctrl.Result{}, err
	}
	if claim.DeletionTimestamp != nil {
		return ctrl.Result{}, r.finish(ctx, sr, fmt.Sprintf("volume claim %s is being deleted", vol.ClaimName))
	}
	if err := workspace.ValidateBinding(ctx, r.Reader, ws.Namespace, *vol); err != nil {
		return ctrl.Result{}, r.finish(ctx, sr, fmt.Sprintf("volume claim %s changed identity or binding: %v", vol.ClaimName, err))
	}
	// The snapshot and restore reconcilers run at the same time in the
	// leader. Without this lock both could pass otherJobOnVolume and start
	// a mover beside a restorer on one volume.
	unlock := volumeLocks.lock(claim.Spec.VolumeName)
	defer unlock()
	// A Pod without a sidecar may still mount the claim; a mover on
	// another node could not attach a ReadWriteOnce volume, and writes
	// from that Pod would race the snapshot.
	// Live list: the Pod cache holds only Pods with the workspace label.
	var pods corev1.PodList
	if err := r.Reader.List(ctx, &pods, client.InNamespace(ws.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	for _, p := range pods.Items {
		// wsnap's own Job Pods (this request's mover, a rescue) are not
		// foreign writers.
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed || p.Labels[wsnapv1.LabelJob] != "" {
			continue
		}
		if !mountsClaim(&p, vol.ClaimName) {
			continue
		}
		if p.Labels[wsnapv1.LabelWorkspace] != "" {
			// A wsnap Pod that is starting (its sidecar may be restoring)
			// or stopping (its sidecar takes the final snapshot). A Job now
			// would work under the sidecar; wait for the Pod.
			st := sr.Status()
			if msg := "waiting for pod " + p.Name; st.Message != msg {
				st.Phase, st.Message = wsnapv1.SnapshotPending, msg
				if err := r.Status().Update(ctx, sr.obj); err != nil {
					return ctrl.Result{}, err
				}
			}
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		return ctrl.Result{}, r.finish(ctx, sr, fmt.Sprintf("pod %s mounts the volume but has no running sidecar", p.Name))
	}
	// One wsnap Job per volume: two restorers, or a restorer and a mover,
	// would share .wsnap/staging and interleave their writes.
	if busy, err := r.otherJobOnVolume(ctx, ws.Namespace, claim.Spec.VolumeName, requestJobName(sr)); err != nil {
		return ctrl.Result{}, err
	} else if busy != "" {
		st := sr.Status()
		if st.Message != "waiting for "+busy {
			st.Phase, st.Message = wsnapv1.SnapshotPending, "waiting for "+busy
			if err := r.Status().Update(ctx, sr.obj); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	var pv corev1.PersistentVolume
	if err := r.Reader.Get(ctx, types.NamespacedName{Name: claim.Spec.VolumeName}, &pv); client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, err
	}
	var repo wsnapv1.SnapshotRepository
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: ws.Namespace, Name: ws.Spec.RepositoryName}, &repo); err != nil {
		return ctrl.Result{}, err
	}
	kind := r.jobKind()
	job := r.Jobs.newAgentJob(kind, requestJobName(sr), ws, &repo,
		[]string{kind, "--dir=" + dataMountPath, "--request=" + sr.GetName()},
		[]corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: vol.ClaimName}}}},
		[]corev1.VolumeMount{{Name: "workspace", MountPath: dataMountPath}})
	// The writer recorded now is the one whose data the Job captures.
	job.Annotations = map[string]string{wsnapv1.AnnotationVolumeName: claim.Spec.VolumeName, wsnapv1.AnnotationRequest: sr.GetName(),
		workspace.AnnotationClaimUID: vol.ClaimUID, workspace.AnnotationPVUID: vol.VolumeUID,
		workspace.AnnotationWriterPod:   pv.Annotations[workspace.AnnotationWriterPod],
		workspace.AnnotationWriterEpoch: pv.Annotations[workspace.AnnotationWriterEpoch]}
	// Mark the request Running before the Job exists: the broker lets a
	// restorer serve only a Running request, and a fast Job Pod must not
	// arrive first. checkJob recreates the Job if this create fails.
	st := sr.Status()
	if st.Phase != wsnapv1.SnapshotRunning {
		st.Phase, st.Executor, st.Message = wsnapv1.SnapshotRunning, kind, kind+" job "+job.Name
		if err := r.Status().Update(ctx, sr.obj); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := createJob(ctx, r.Client, r.Reader, job, ws.Name); err != nil {
		return ctrl.Result{}, err
	}
	return recheck, nil
}

func (r *RequestReconciler) checkJob(ctx context.Context, sr request, recheck ctrl.Result) (ctrl.Result, error) {
	var job batchv1.Job
	// Live read: a cache that has not seen the new Job yet must not send
	// this request down the recreate path.
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: sr.GetNamespace(), Name: requestJobName(sr)}, &job); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		// Not created yet (a failed create after the status update), or
		// deleted: create it, re-checking that the volume is still idle.
		var ws wsnapv1.Workspace
		if err := r.Get(ctx, types.NamespacedName{Namespace: sr.GetNamespace(), Name: sr.GetWorkspaceName()}, &ws); err != nil {
			return ctrl.Result{}, r.finish(ctx, sr, "workspace not found")
		}
		return r.startJob(ctx, sr, &ws, recheck)
	}
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return ctrl.Result{}, r.finish(ctx, sr, r.jobKind()+" job failed: "+c.Message)
		}
	}
	// Success is recorded by the Job's report through the broker.
	return recheck, nil
}

// otherJobOnVolume names another unfinished wsnap Job on the volume, or "".
func (r *RequestReconciler) otherJobOnVolume(ctx context.Context, namespace, volumeName, self string) (string, error) {
	if volumeName == "" {
		return "", nil
	}
	var jobs batchv1.JobList
	if err := r.Reader.List(ctx, &jobs, client.InNamespace(namespace), client.MatchingLabels{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue}); err != nil {
		return "", err
	}
	for i := range jobs.Items {
		j := &jobs.Items[i]
		if j.Name != self && j.Annotations[wsnapv1.AnnotationVolumeName] == volumeName && !jobFinished(j) {
			return j.Name, nil
		}
	}
	return "", nil
}

// mountsClaim reports whether p uses the claim: as a persistentVolumeClaim
// volume, or as a generic ephemeral volume, whose claim is named
// <pod>-<volume>.
func mountsClaim(p *corev1.Pod, claim string) bool {
	for _, v := range p.Spec.Volumes {
		if (v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == claim) ||
			(v.Ephemeral != nil && p.Name+"-"+v.Name == claim) {
			return true
		}
	}
	return false
}

// stripedMutex serializes work per key within this process, with fixed
// memory: keys share one of a fixed set of locks.
type stripedMutex [64]sync.Mutex

func (k *stripedMutex) lock(key string) (unlock func()) {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	m := &k[h.Sum32()%uint32(len(k))]
	m.Lock()
	return m.Unlock
}

// volumeLocks covers "check for another Job on the volume, then create
// one". Reconcilers run only in the leader, so a process-wide lock is
// enough; the check reads the API server, so it sees a Job created a
// moment ago under the same lock.
var volumeLocks stripedMutex

// finish marks a request failed. Successes are recorded by the broker.
func (r *RequestReconciler) finish(ctx context.Context, sr request, msg string) error {
	now := metav1.Now()
	st := sr.Status()
	st.Phase, st.Message, st.CompletionTime = wsnapv1.SnapshotFailed, msg, &now
	return r.Status().Update(ctx, sr.obj)
}

// requestJobName differs per kind, so a snapshot and a restore request
// with the same name never share a Job.
func requestJobName(sr request) string {
	kind := "s"
	if _, ok := sr.obj.(*wsnapv1.WorkspaceRestore); ok {
		kind = "r"
	}
	return "wsnap-req" + kind + "-" + shortHash(sr.GetNamespace(), sr.GetName(), string(sr.GetUID()))
}

// SetupWithManager registers the reconciler and watches its Jobs.
func (r *RequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	name := "workspacesnapshot"
	if r.Restore {
		name = "workspacerestore"
	}
	kind := r.jobKind()
	return ctrl.NewControllerManagedBy(mgr).Named(name).
		For(r.newObj()).
		Watches(&batchv1.Job{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
			name := o.GetAnnotations()[wsnapv1.AnnotationRequest]
			if name == "" || o.GetLabels()[wsnapv1.LabelJob] != kind {
				return nil
			}
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: o.GetNamespace(), Name: name}}}
		})).
		Complete(r)
}
