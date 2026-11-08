package controller

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
)

// WorkerReconciler runs one separate snapshot worker beside a PVC-backed
// workload Pod. The Pod is scheduled first; the worker is pinned to that
// node, so a ReadWriteOnce claim can be mounted by both Pods.
type WorkerReconciler struct {
	client.Client
	Reader client.Reader
	Jobs   JobConfig
}

func workerJobName(namespace string, pod *corev1.Pod) string {
	return "wsnap-worker-" + shortHash(namespace, string(pod.UID))
}

func (r *WorkerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var pod corev1.Pod
	if err := r.Reader.Get(ctx, req.NamespacedName, &pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if pod.Labels[wsnapv1.LabelWorkspace] == "" || pod.Spec.NodeName == "" ||
		pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return ctrl.Result{}, nil
	}
	var ws wsnapv1.Workspace
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: pod.Labels[wsnapv1.LabelWorkspace]}, &ws); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}
	if ws.Spec.ExecutionMode() != wsnapv1.ExecutionWorker {
		return ctrl.Result{}, nil
	}
	if ws.Spec.RepositoryName != pod.Labels[wsnapv1.LabelRepository] {
		return ctrl.Result{}, fmt.Errorf("workspace %s repository does not match pod %s", ws.Name, pod.Name)
	}
	vol, err := workspace.ResolveVolume(ctx, r.Reader, &pod)
	if err != nil {
		return ctrl.Result{}, err
	}
	if vol.ClaimName == "" {
		return ctrl.Result{}, fmt.Errorf("workspace %s uses worker mode but pod %s has no PVC", ws.Name, pod.Name)
	}
	if vol.VolumeName == "" {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	var claim corev1.PersistentVolumeClaim
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: vol.ClaimName}, &claim); err != nil {
		return ctrl.Result{}, err
	}
	for _, mode := range claim.Spec.AccessModes {
		if mode == corev1.ReadWriteOncePod {
			return ctrl.Result{}, fmt.Errorf("workspace %s cannot use worker mode with ReadWriteOncePod claim %s", ws.Name, claim.Name)
		}
	}
	name := workerJobName(pod.Namespace, &pod)
	var existing batchv1.Job
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: name}, &existing); err == nil {
		if existing.Annotations[wsnapv1.AnnotationTargetPodUID] != string(pod.UID) ||
			existing.Annotations[wsnapv1.AnnotationVolumeName] != vol.VolumeName {
			return ctrl.Result{}, fmt.Errorf("worker job %s does not match pod %s and its volume", name, pod.Name)
		}
		if !jobFinished(&existing) {
			return ctrl.Result{}, nil
		}
		// The worker was evicted or failed while its workload still runs.
		// Foreground deletion lets the old Pod release its mount before a
		// replacement Job with the same name is created.
		fg := metav1.DeletePropagationForeground
		if err := r.Delete(ctx, &existing, &client.DeleteOptions{PropagationPolicy: &fg}); client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	} else if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	var repo wsnapv1.SnapshotRepository
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: ws.Namespace, Name: ws.Spec.RepositoryName}, &repo); err != nil {
		return ctrl.Result{}, err
	}
	job := r.Jobs.newAgentJob("worker", name, &ws, &repo,
		[]string{"worker", "--dir=" + dataMountPath, "--pod-uid=" + string(pod.UID), "--final-timeout=50s"},
		[]corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: vol.ClaimName}}}},
		[]corev1.VolumeMount{{Name: "workspace", MountPath: dataMountPath}})
	job.Annotations = map[string]string{
		wsnapv1.AnnotationTargetPodName: pod.Name,
		wsnapv1.AnnotationTargetPodUID:  string(pod.UID),
		wsnapv1.AnnotationVolumeName:    vol.VolumeName,
		workspace.AnnotationClaimUID:    vol.ClaimUID,
		workspace.AnnotationPVUID:       vol.VolumeUID,
		workspace.AnnotationWriterPod:   string(pod.UID),
	}
	job.Spec.ActiveDeadlineSeconds = nil // a live worker may outlive a two-hour one-shot Job
	job.Spec.Template.Spec.NodeName = pod.Spec.NodeName
	job.Spec.Template.Spec.TerminationGracePeriodSeconds = ptr.To[int64](90)
	if err := createJob(ctx, r.Client, r.Reader, job, ws.Name); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *WorkerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).Named("workspace-worker").
		For(&corev1.Pod{}, builder.WithPredicates(podRelevant)).
		Watches(&batchv1.Job{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
			if o.GetLabels()[wsnapv1.LabelJob] != "worker" {
				return nil
			}
			name := o.GetAnnotations()[wsnapv1.AnnotationTargetPodName]
			if name == "" {
				return nil
			}
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: o.GetNamespace(), Name: name}}}
		})).Complete(r)
}
