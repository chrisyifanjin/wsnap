// Package controller contains the wsnap reconcilers.
package controller

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
)

// PodReconciler watches workload Pods (those with the wsnap.dev/workspace
// label). It creates the Workspace, protects the PV behind the workspace
// volume and records the Pod as the PV's writer.
//
// The broker does the same when it issues credentials. The reconciler
// covers Pods whose sidecar never asks: a warm-resumed Pod whose volume
// already holds data skips the restore, and its first snapshot comes only
// after one interval. Writes in between still count.
type PodReconciler struct {
	client.Client
	// Reader reads PVCs and PVs without the cache, which holds only
	// wsnap-labeled objects.
	Reader client.Reader
}

func (r *PodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// A terminating Pod is taking its final snapshot. Marking it as writer
	// again could overwrite a newer Pod's mark.
	if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return ctrl.Result{}, nil
	}
	id, err := workspace.FromPod(&pod)
	if err != nil {
		logger.Info("ignoring pod with invalid workspace labels", "error", err.Error())
		return ctrl.Result{}, nil
	}
	if err := r.ensureWorkspace(ctx, id); err != nil {
		return ctrl.Result{}, err
	}
	ref, err := workspace.ResolveVolume(ctx, r.Reader, &pod)
	if err != nil {
		logger.Info("cannot resolve workspace volume", "error", err.Error())
		return ctrl.Result{}, nil
	}
	if ref.Kind != workspace.KindEmptyDir && ref.VolumeName == "" {
		// WaitForFirstConsumer binds the claim only after scheduling. An
		// unschedulable Pod would make a short requeue a hot loop.
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if err := workspace.Protect(ctx, r.Client, r.Reader, id, ref); err != nil {
		return ctrl.Result{}, err
	}
	if pod.Spec.NodeName != "" {
		if err := workspace.MarkPV(ctx, r.Client, r.Reader, ref.VolumeName, workspace.AnnotationWriterPod, string(pod.UID)); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var ws wsnapv1.Workspace
		if err := r.Get(ctx, client.ObjectKey{Namespace: id.Namespace, Name: id.Name}, &ws); err != nil {
			return err
		}
		if ws.Status.Volume != nil && *ws.Status.Volume == ref {
			return nil
		}
		// status.volume describes the writer's volume; a Pod waiting for
		// the writer to stop must not overwrite it.
		if w := ws.Status.WriterPod; w != nil && w.UID != string(pod.UID) {
			return nil
		}
		ws.Status.Volume = &ref
		return r.Status().Update(ctx, &ws)
	})
}

func (r *PodReconciler) ensureWorkspace(ctx context.Context, id workspace.Identity) error {
	ws := wsnapv1.Workspace{
		ObjectMeta: metav1.ObjectMeta{Namespace: id.Namespace, Name: id.Name,
			Labels: map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue}},
		Spec: wsnapv1.WorkspaceSpec{RepositoryName: id.Repository},
	}
	if err := r.Create(ctx, &ws); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// SetupWithManager registers the reconciler. Pods change status often
// (probes, IPs, container states); only scheduling, phase, deletion and
// label changes matter here, and each reconcile costs live API reads.
func (r *PodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).Named("workspace-pod").
		For(&corev1.Pod{}, builder.WithPredicates(podRelevant)).
		Complete(r)
}

var podRelevant = predicate.Funcs{
	CreateFunc:  func(event.CreateEvent) bool { return true },
	DeleteFunc:  func(event.DeleteEvent) bool { return false },
	GenericFunc: func(event.GenericEvent) bool { return false },
	UpdateFunc: func(e event.UpdateEvent) bool {
		o, ok1 := e.ObjectOld.(*corev1.Pod)
		n, ok2 := e.ObjectNew.(*corev1.Pod)
		if !ok1 || !ok2 {
			return true
		}
		return o.Spec.NodeName != n.Spec.NodeName ||
			o.Status.Phase != n.Status.Phase ||
			(o.DeletionTimestamp == nil) != (n.DeletionTimestamp == nil) ||
			o.Labels[wsnapv1.LabelWorkspace] != n.Labels[wsnapv1.LabelWorkspace] ||
			o.Labels[wsnapv1.LabelRepository] != n.Labels[wsnapv1.LabelRepository]
	},
}
