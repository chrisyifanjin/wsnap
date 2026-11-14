package controller

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
)

// PVReconciler handles protected PVs after their claim is deleted.
//
// Deleting a Sandbox, or a Pod with a generic ephemeral volume, deletes the
// PVC. A PVC that is being deleted cannot be mounted, so a Job cannot
// snapshot it. Instead, wsnap switched the PV to Retain earlier: the PV goes
// to Released with its data intact. If the last writer took a final
// snapshot, the reconciler restores the original reclaim policy and
// Kubernetes reclaims the volume. If not, it binds the PV to a new "rescue"
// PVC, runs a mover Job on it, and reclaims the volume afterwards.
type PVReconciler struct {
	client.Client
	Reader   client.Reader
	Jobs     JobConfig
	Recorder events.EventRecorder
}

func (r *PVReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	var pv corev1.PersistentVolume
	if err := r.Get(ctx, req.NamespacedName, &pv); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if pv.Labels[wsnapv1.LabelProtected] != "true" || pv.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}
	ns, name, ok := strings.Cut(pv.Annotations[wsnapv1.AnnotationWorkspaceRef], "/")
	if !ok {
		return ctrl.Result{}, nil
	}
	phase := pv.Annotations[wsnapv1.AnnotationRescue]
	// wsnapv1.RescueSkippedNewerWriter is only set by earlier versions.
	if phase == wsnapv1.RescueFailed || phase == wsnapv1.RescueSkippedNewerWriter {
		return ctrl.Result{}, nil // kept for an operator; never retried on its own
	}
	// A PV bound to its rescue claim is mid-rescue whatever the annotation
	// says, so a lost annotation update cannot strand it.
	boundToRescue := pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Name == rescueClaimName(pv.Name) && pv.Spec.ClaimRef.Namespace == ns
	if phase == wsnapv1.RescueRunning || (boundToRescue && phase != wsnapv1.RescueDone) {
		// An empty phase on a PV bound to its rescue claim: an operator
		// removed "failed" to retry (or an annotation update was lost).
		return r.checkRescue(ctx, &pv, ns, name, phase == "")
	}
	if phase == wsnapv1.RescueDone && boundToRescue && pv.Status.Phase != corev1.VolumeReleased {
		// A previous reconcile may have recorded completion but stopped before
		// deleting the rescue claim. Retry that cleanup using the claim UID.
		var cur corev1.PersistentVolume
		if err := r.Reader.Get(ctx, types.NamespacedName{Name: pv.Name}, &cur); err != nil {
			return ctrl.Result{}, err
		}
		if cur.UID != pv.UID || cur.Annotations[wsnapv1.AnnotationRescue] != wsnapv1.RescueDone {
			return ctrl.Result{}, fmt.Errorf("volume %s changed before rescue cleanup", pv.Name)
		}
		if err := r.deleteRescueClaim(ctx, &cur, ns); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if pv.Status.Phase != corev1.VolumeReleased {
		return ctrl.Result{}, nil
	}
	if phase == wsnapv1.RescueDone {
		return ctrl.Result{}, r.reclaim(ctx, &pv)
	}

	if workspace.HasFinalSnapshot(pv.Annotations) {
		logger.Info("released volume has a final snapshot; reclaiming", "pv", pv.Name)
		return ctrl.Result{}, r.reclaim(ctx, &pv)
	}

	var ws wsnapv1.Workspace
	// Live read: reclaiming deletes the data, so a Workspace the cache has
	// not seen yet must not count as deleted.
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &ws); err != nil {
		if apierrors.IsNotFound(err) {
			// The Workspace was deleted on purpose; nothing to keep.
			return ctrl.Result{}, r.reclaim(ctx, &pv)
		}
		return ctrl.Result{}, err
	}
	// Rescued even when a newer writer exists: the mover's snapshot
	// carries this volume's writer epoch, and the broker never lets an
	// older epoch replace lastSnapshot, which restores use. The older data
	// stays in the repository, reachable by snapshot ID.
	logger.Info("released volume has no final snapshot; starting rescue", "pv", pv.Name, "workspace", ws.Name)
	return ctrl.Result{}, r.startRescue(ctx, &pv, &ws)
}

func (r *PVReconciler) setRescue(ctx context.Context, pv *corev1.PersistentVolume, phase string) error {
	patch := client.MergeFromWithOptions(pv.DeepCopy(), client.MergeFromWithOptimisticLock{})
	pv.Annotations[wsnapv1.AnnotationRescue] = phase
	return r.Patch(ctx, pv, patch)
}

// deleteRescueClaim uses the PV's pre-bound UID so a namespace user cannot
// replace the claim under the same name and have the controller delete it.
func (r *PVReconciler) deleteRescueClaim(ctx context.Context, pv *corev1.PersistentVolume, namespace string) error {
	ref := pv.Spec.ClaimRef
	if ref == nil || ref.Namespace != namespace || ref.Name != rescueClaimName(pv.Name) || ref.UID == "" {
		return fmt.Errorf("volume %s has no complete rescue claim identity; keeping it", pv.Name)
	}
	var claim corev1.PersistentVolumeClaim
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, &claim); err != nil {
		return client.IgnoreNotFound(err)
	}
	if claim.UID != ref.UID {
		return fmt.Errorf("rescue claim %s/%s changed UID; keeping volume %s", namespace, ref.Name, pv.Name)
	}
	return client.IgnoreNotFound(r.Delete(ctx, &claim, &client.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &ref.UID},
	}))
}

func (r *PVReconciler) startRescue(ctx context.Context, pv *corev1.PersistentVolume, ws *wsnapv1.Workspace) error {
	var repo wsnapv1.SnapshotRepository
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: ws.Namespace, Name: ws.Spec.RepositoryName}, &repo); err != nil {
		return fmt.Errorf("snapshot repository: %w", err)
	}
	claimName := rescueClaimName(pv.Name)
	storageClass := pv.Spec.StorageClassName
	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: ws.Namespace, Name: claimName,
			Labels: map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue}},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      pv.Spec.AccessModes,
			VolumeMode:       pv.Spec.VolumeMode,
			StorageClassName: &storageClass,
			// volumeName prevents dynamic provisioning of a new volume.
			VolumeName: pv.Name,
			Resources:  corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: pv.Spec.Capacity[corev1.ResourceStorage]}},
		},
	}
	createErr := r.Create(ctx, claim)
	if createErr != nil && !apierrors.IsAlreadyExists(createErr) {
		return fmt.Errorf("create rescue claim: %w", createErr)
	}
	// Read the claim live. A same-name PVC might have been created by a
	// namespace user; never mount it or let it provision another PV. On a
	// retry, the PV must already record this precise claim UID.
	var actual corev1.PersistentVolumeClaim
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: ws.Namespace, Name: claimName}, &actual); err != nil {
		return fmt.Errorf("read rescue claim: %w", err)
	}
	claimSize := actual.Spec.Resources.Requests[corev1.ResourceStorage]
	volumeSize := pv.Spec.Capacity[corev1.ResourceStorage]
	if actual.UID == "" || actual.DeletionTimestamp != nil || actual.Labels[wsnapv1.LabelManagedBy] != wsnapv1.ManagedByValue ||
		actual.Spec.VolumeName != pv.Name || actual.Spec.StorageClassName == nil || *actual.Spec.StorageClassName != storageClass ||
		!slices.Equal(actual.Spec.AccessModes, claim.Spec.AccessModes) ||
		!sameVolumeMode(actual.Spec.VolumeMode, claim.Spec.VolumeMode) ||
		claimSize.Cmp(volumeSize) != 0 {
		return fmt.Errorf("rescue claim %s/%s does not match volume %s; keeping the volume", ws.Namespace, claimName, pv.Name)
	}
	if apierrors.IsAlreadyExists(createErr) && (pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != ws.Namespace ||
		pv.Spec.ClaimRef.Name != claimName || pv.Spec.ClaimRef.UID != actual.UID) {
		return fmt.Errorf("rescue claim %s/%s already exists without matching PV claim UID; keeping the volume", ws.Namespace, claimName)
	}
	// A Released PV keeps a claimRef to the deleted claim. Pre-bind it to
	// the rescue claim's UID so a replacement under that name cannot inherit
	// this PV after a controller restart.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cur corev1.PersistentVolume
		if err := r.Get(ctx, types.NamespacedName{Name: pv.Name}, &cur); err != nil {
			return err
		}
		changed := false
		if cur.UID != pv.UID || cur.Annotations[wsnapv1.AnnotationWorkspaceRef] != ws.Namespace+"/"+ws.Name {
			return fmt.Errorf("volume %s changed identity or workspace during rescue", pv.Name)
		}
		if ref := cur.Spec.ClaimRef; ref != nil && ref.Name == claimName && ref.Namespace == ws.Namespace && ref.UID != actual.UID {
			return fmt.Errorf("volume %s is pre-bound to a different rescue claim UID", pv.Name)
		}
		if ref := cur.Spec.ClaimRef; ref == nil || ref.Name != claimName || ref.Namespace != ws.Namespace {
			if cur.Status.Phase != corev1.VolumeReleased {
				return fmt.Errorf("volume %s is no longer released; refusing rescue pre-bind", pv.Name)
			}
			cur.Spec.ClaimRef = &corev1.ObjectReference{Kind: "PersistentVolumeClaim", APIVersion: "v1", Namespace: ws.Namespace, Name: claimName, UID: actual.UID}
			changed = true
		}
		if cur.Annotations[wsnapv1.AnnotationRescue] != wsnapv1.RescueRunning {
			cur.Annotations[wsnapv1.AnnotationRescue] = wsnapv1.RescueRunning
			changed = true
		}
		if !changed {
			return nil
		}
		return r.Update(ctx, &cur)
	}); err != nil {
		return fmt.Errorf("pre-bind volume: %w", err)
	}
	job := r.Jobs.newAgentJob(wsnapv1.JobMover, moverJobName(pv.Name), ws, &repo,
		[]string{"mover", "--dir=" + dataMountPath},
		[]corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claimName}}}},
		[]corev1.VolumeMount{{Name: "workspace", MountPath: dataMountPath}})
	job.Annotations = map[string]string{wsnapv1.AnnotationVolumeName: pv.Name,
		workspace.AnnotationClaimUID: string(actual.UID), workspace.AnnotationPVUID: string(pv.UID),
		workspace.AnnotationWriterPod:   pv.Annotations[workspace.AnnotationWriterPod],
		workspace.AnnotationWriterEpoch: pv.Annotations[workspace.AnnotationWriterEpoch]}
	if err := createJob(ctx, r.Client, r.Reader, job, ws.Name); err != nil {
		return fmt.Errorf("create mover job: %w", err)
	}
	r.Recorder.Eventf(pv, nil, corev1.EventTypeNormal, "RescueStarted", "Rescue", "mover job %s/%s snapshots this volume", ws.Namespace, job.Name)
	return nil
}

func sameVolumeMode(a, b *corev1.PersistentVolumeMode) bool {
	mode := func(p *corev1.PersistentVolumeMode) corev1.PersistentVolumeMode {
		if p == nil {
			return corev1.PersistentVolumeFilesystem // Kubernetes' default
		}
		return *p
	}
	return mode(a) == mode(b)
}

func (r *PVReconciler) checkRescue(ctx context.Context, pv *corev1.PersistentVolume, ns, name string, retry bool) (ctrl.Result, error) {
	var job batchv1.Job
	// Read live: the cache may not have the Job this reconciler created a
	// moment ago, and a stale "not found" must never undo the rescue.
	err := r.Reader.Get(ctx, types.NamespacedName{Namespace: ns, Name: moverJobName(pv.Name)}, &job)
	if apierrors.IsNotFound(err) {
		// Never created (a failed create) or deleted early: create it again.
		// Every step of startRescue is idempotent.
		var ws wsnapv1.Workspace
		if err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &ws); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, r.startRescue(ctx, pv, &ws)
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete:
			// A Complete Job is not proof: its report could have been lost.
			// Only the broker's final mark (set when it recorded the
			// snapshot, or the mover's "nothing to save") lets the volume
			// go. Read live: the mark lands moments before the Job ends.
			var cur corev1.PersistentVolume
			if err := r.Reader.Get(ctx, types.NamespacedName{Name: pv.Name}, &cur); err != nil {
				return ctrl.Result{}, err
			}
			if cur.UID != pv.UID || job.Annotations[workspace.AnnotationPVUID] != string(cur.UID) ||
				cur.Spec.ClaimRef == nil || job.Annotations[workspace.AnnotationClaimUID] != string(cur.Spec.ClaimRef.UID) {
				return ctrl.Result{}, fmt.Errorf("mover job %s/%s no longer matches volume %s and its rescue claim", ns, job.Name, pv.Name)
			}
			want := cmp.Or(job.Annotations[workspace.AnnotationWriterPod], workspace.FinalByMover)
			if cur.Annotations[workspace.AnnotationFinalPod] != want && retry {
				return r.restartRescue(ctx, pv, &job)
			}
			if cur.Annotations[workspace.AnnotationFinalPod] != want {
				r.Recorder.Eventf(pv, nil, corev1.EventTypeWarning, "wsnapv1.RescueFailed", "Rescue",
					"mover job %s/%s completed, but the broker recorded no final snapshot of this volume; keeping it", ns, job.Name)
				return ctrl.Result{}, r.setRescue(ctx, &cur, wsnapv1.RescueFailed)
			}
			log.FromContext(ctx).Info("rescue finished", "pv", pv.Name, "job", job.Name)
			// Delete before marking the rescue done. A crash after the mark
			// could otherwise strand a bound claim with no retry path.
			if err := r.deleteRescueClaim(ctx, &cur, ns); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.setRescue(ctx, &cur, wsnapv1.RescueDone); err != nil {
				return ctrl.Result{}, err
			}
			// The deleted rescue claim releases the PV again; the next
			// reconcile sees wsnapv1.RescueDone and reclaims it.
			bg := metav1.DeletePropagationBackground
			if err := r.Delete(ctx, &job, &client.DeleteOptions{PropagationPolicy: &bg}); client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		case batchv1.JobFailed:
			if retry {
				return r.restartRescue(ctx, pv, &job)
			}
			r.Recorder.Eventf(pv, nil, corev1.EventTypeWarning, "wsnapv1.RescueFailed", "Rescue", "mover job %s/%s failed; keeping the volume", ns, job.Name)
			return ctrl.Result{}, r.setRescue(ctx, pv, wsnapv1.RescueFailed)
		}
	}
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// restartRescue deletes a finished mover Job so the next reconcile creates
// a new one: the operator's retry after a failed rescue.
func (r *PVReconciler) restartRescue(ctx context.Context, pv *corev1.PersistentVolume, job *batchv1.Job) (ctrl.Result, error) {
	r.Recorder.Eventf(pv, nil, corev1.EventTypeNormal, "RescueRetried", "Rescue", "starting a new mover job; the previous one (%s) did not save the volume", job.Name)
	fg := metav1.DeletePropagationForeground
	if err := r.Delete(ctx, job, &client.DeleteOptions{PropagationPolicy: &fg, Preconditions: &metav1.Preconditions{UID: &job.UID}}); client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, err
	}
	if err := r.setRescue(ctx, pv, wsnapv1.RescueRunning); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// reclaim restores the original reclaim policy. For Delete, the PV
// controller then deletes the volume. A PV that was Retain to begin with is
// left Released, as its owner intended.
func (r *PVReconciler) reclaim(ctx context.Context, pv *corev1.PersistentVolume) error {
	// Reconcile starts from a cached object. A different controller or an
	// operator may have rebound or repurposed the PV since that read. A
	// reclaim patch based on the old object could otherwise restore Delete
	// and remove protection from a newly owned volume.
	var cur corev1.PersistentVolume
	if err := r.Reader.Get(ctx, types.NamespacedName{Name: pv.Name}, &cur); err != nil {
		return err
	}
	if cur.UID != pv.UID || cur.ResourceVersion != pv.ResourceVersion {
		return fmt.Errorf("volume %s changed before reclaim; retrying with its current state", pv.Name)
	}
	original := corev1.PersistentVolumeReclaimPolicy(cur.Annotations[wsnapv1.AnnotationOriginalReclaimPolicy])
	if original == "" {
		original = corev1.PersistentVolumeReclaimDelete
	}
	patch := client.MergeFromWithOptions(cur.DeepCopy(), client.MergeFromWithOptimisticLock{})
	delete(cur.Labels, wsnapv1.LabelProtected)
	// A Retain PV can later be cleaned and bound to another workspace.
	// Its old ownership and writer marks must not follow the asset.
	for _, key := range []string{wsnapv1.AnnotationWorkspaceRef, wsnapv1.AnnotationOriginalReclaimPolicy,
		wsnapv1.AnnotationRescue, workspace.AnnotationWriterPod, workspace.AnnotationWriterEpoch, workspace.AnnotationFinalPod} {
		delete(cur.Annotations, key)
	}
	cur.Spec.PersistentVolumeReclaimPolicy = original
	return r.Patch(ctx, &cur, patch)
}

// SetupWithManager registers the reconciler. It also watches mover Jobs so
// a finished rescue is noticed without polling.
func (r *PVReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).Named("workspace-pv").
		For(&corev1.PersistentVolume{}).
		Watches(&batchv1.Job{}, jobToPV()).
		Complete(r)
}
