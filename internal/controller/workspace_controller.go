package controller

import (
	"context"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
)

// WorkspaceReconciler starts maintenance Jobs. Maintenance needs the delete
// permission, which no sidecar ever gets, so it always runs in a Job.
type WorkspaceReconciler struct {
	client.Client
	Reader client.Reader
	Jobs   JobConfig
}

func (r *WorkspaceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var ws wsnapv1.Workspace
	if err := r.Get(ctx, req.NamespacedName, &ws); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if ws.Status.LastSnapshot == nil {
		return ctrl.Result{}, nil
	}
	var repo wsnapv1.SnapshotRepository
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: ws.Namespace, Name: ws.Spec.RepositoryName}, &repo); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	every := repo.Spec.Retention.MaintenanceEvery
	if every <= 0 || ws.Status.SnapshotsSinceMaintenance < every {
		return ctrl.Result{}, nil
	}
	// One maintenance Job at a time per workspace.
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.InNamespace(ws.Namespace), client.MatchingLabels{
		wsnapv1.LabelJob: "maintenance", wsnapv1.LabelJobWorkspace: ws.Name,
	}); err != nil {
		return ctrl.Result{}, err
	}
	for i := range jobs.Items {
		if !jobFinished(&jobs.Items[i]) {
			return ctrl.Result{}, nil
		}
	}
	job := r.Jobs.newAgentJob("maintenance", maintenanceJobName(&ws), &ws, &repo, []string{"maintain"}, nil, nil)
	if err := r.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, err
	}
	log.FromContext(ctx).Info("started maintenance", "workspace", ws.Name, "job", job.Name)
	return ctrl.Result{}, nil
}

// SetupWithManager registers the reconciler.
func (r *WorkspaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).Named("workspace").For(&wsnapv1.Workspace{}).Complete(r)
}

// jobFinished reports whether a Job reached Complete or Failed. Counting
// failures against backoffLimit misses Jobs stopped by activeDeadlineSeconds.
func jobFinished(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == "True" {
			return true
		}
	}
	return false
}

// jobToPV maps a mover Job to the PV it rescues.
func jobToPV() handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
		if o.GetLabels()[wsnapv1.LabelJob] != "mover" {
			return nil
		}
		pv := o.GetAnnotations()[wsnapv1.AnnotationVolumeName]
		if pv == "" {
			return nil
		}
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: pv}}}
	})
}
