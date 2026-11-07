package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
)

// Rescue phases stored in the wsnap.dev/rescue annotation of a PV.
const (
	RescueRunning = "running"
	RescueDone    = "done"
	RescueFailed  = "failed"
	// RescueSkipped was set by earlier versions when a newer writer
	// existed. Writer epochs made the skip unnecessary; such PVs are still
	// left for an operator.
	RescueSkipped = "skipped-newer-writer"
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
	Recorder record.EventRecorder
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
	// RescueSkipped is only set by earlier versions.
	if phase == RescueFailed || phase == RescueSkipped {
		return ctrl.Result{}, nil // kept for an operator; never retried on its own
	}
	// A PV bound to its rescue claim is mid-rescue whatever the annotation
	// says, so a lost annotation update cannot strand it.
	boundToRescue := pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Name == rescueClaimName(pv.Name) && pv.Spec.ClaimRef.Namespace == ns
	if phase == RescueRunning || (boundToRescue && phase != RescueDone) {
		return r.checkRescue(ctx, &pv, ns, name)
	}
	if pv.Status.Phase != corev1.VolumeReleased {
		return ctrl.Result{}, nil
	}
	if phase == RescueDone {
		return ctrl.Result{}, r.reclaim(ctx, &pv)
	}

	writer := pv.Annotations[workspace.AnnotationWriterPod]
	if writer != "" && writer == pv.Annotations[workspace.AnnotationFinalPod] {
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
	patch := client.MergeFrom(pv.DeepCopy())
	pv.Annotations[wsnapv1.AnnotationRescue] = phase
	return r.Patch(ctx, pv, patch)
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
	if err := r.Create(ctx, claim); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create rescue claim: %w", err)
	}
	// A Released PV keeps a claimRef to the deleted claim. Pointing it at
	// the rescue claim without a UID pre-binds it, and the PV controller
	// binds the two.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cur corev1.PersistentVolume
		if err := r.Get(ctx, types.NamespacedName{Name: pv.Name}, &cur); err != nil {
			return err
		}
		changed := false
		// Keep an existing pre-bind to the rescue claim untouched: once
		// bound, it carries the claim's UID.
		if ref := cur.Spec.ClaimRef; ref == nil || ref.Name != claimName || ref.Namespace != ws.Namespace {
			cur.Spec.ClaimRef = &corev1.ObjectReference{Kind: "PersistentVolumeClaim", APIVersion: "v1", Namespace: ws.Namespace, Name: claimName}
			changed = true
		}
		if cur.Annotations[wsnapv1.AnnotationRescue] != RescueRunning {
			cur.Annotations[wsnapv1.AnnotationRescue] = RescueRunning
			changed = true
		}
		if !changed {
			return nil
		}
		return r.Update(ctx, &cur)
	}); err != nil {
		return fmt.Errorf("pre-bind volume: %w", err)
	}
	job := r.Jobs.newAgentJob("mover", moverJobName(pv.Name), ws, &repo,
		[]string{"mover", "--dir=" + dataMountPath},
		[]corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claimName}}}},
		[]corev1.VolumeMount{{Name: "workspace", MountPath: dataMountPath}})
	job.Annotations = map[string]string{wsnapv1.AnnotationVolumeName: pv.Name,
		workspace.AnnotationWriterPod:   pv.Annotations[workspace.AnnotationWriterPod],
		workspace.AnnotationWriterEpoch: pv.Annotations[workspace.AnnotationWriterEpoch]}
	if err := createJob(ctx, r.Client, r.Reader, job, ws.Name); err != nil {
		return fmt.Errorf("create mover job: %w", err)
	}
	r.Recorder.Eventf(pv, corev1.EventTypeNormal, "RescueStarted", "mover job %s/%s snapshots this volume", ws.Namespace, job.Name)
	return nil
}

func (r *PVReconciler) checkRescue(ctx context.Context, pv *corev1.PersistentVolume, ns, name string) (ctrl.Result, error) {
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
			log.FromContext(ctx).Info("rescue finished", "pv", pv.Name, "job", job.Name)
			if err := r.setRescue(ctx, pv, RescueDone); err != nil {
				return ctrl.Result{}, err
			}
			// Deleting the rescue claim releases the PV again; the next
			// reconcile sees RescueDone and reclaims it.
			bg := metav1.DeletePropagationBackground
			if err := r.Delete(ctx, &job, &client.DeleteOptions{PropagationPolicy: &bg}); client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}
			claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: rescueClaimName(pv.Name)}}
			return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, claim))
		case batchv1.JobFailed:
			r.Recorder.Eventf(pv, corev1.EventTypeWarning, "RescueFailed", "mover job %s/%s failed; keeping the volume", ns, job.Name)
			return ctrl.Result{}, r.setRescue(ctx, pv, RescueFailed)
		}
	}
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// reclaim restores the original reclaim policy. For Delete, the PV
// controller then deletes the volume. A PV that was Retain to begin with is
// left Released, as its owner intended.
func (r *PVReconciler) reclaim(ctx context.Context, pv *corev1.PersistentVolume) error {
	original := corev1.PersistentVolumeReclaimPolicy(pv.Annotations[wsnapv1.AnnotationOriginalReclaimPolicy])
	if original == "" {
		original = corev1.PersistentVolumeReclaimDelete
	}
	patch := client.MergeFrom(pv.DeepCopy())
	delete(pv.Labels, wsnapv1.LabelProtected)
	pv.Spec.PersistentVolumeReclaimPolicy = original
	return r.Patch(ctx, pv, patch)
}

// SetupWithManager registers the reconciler. It also watches mover Jobs so
// a finished rescue is noticed without polling.
func (r *PVReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).Named("workspace-pv").
		For(&corev1.PersistentVolume{}).
		Watches(&batchv1.Job{}, jobToPV()).
		Complete(r)
}
