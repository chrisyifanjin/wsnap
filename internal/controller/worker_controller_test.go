package controller

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
)

func workerReconciler(t *testing.T, claim *corev1.PersistentVolumeClaim) (*WorkerReconciler, *corev1.Pod, func() *wsnapv1.Workspace) {
	t.Helper()
	ws := wsWith(nil, nil)
	ws.Spec.Execution.Mode = wsnapv1.ExecutionWorker
	pod := runningPod("app", "app-uid", claim.Name)
	pod.Labels = map[string]string{wsnapv1.LabelWorkspace: "ws1", wsnapv1.LabelRepository: "repo"}
	pod.Spec.NodeName = "node-a"
	rr, c := requestEnv(t, ws, pod, claim)
	r := &WorkerReconciler{Client: c, Reader: c, Jobs: rr.Jobs}
	get := func() *wsnapv1.Workspace {
		var out wsnapv1.Workspace
		_ = c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "ws1"}, &out)
		return &out
	}
	return r, pod, get
}

func TestWorkerJobSharesTheWorkloadsNodeAndClaim(t *testing.T) {
	r, pod, ws := workerReconciler(t, boundClaim())
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "app"}}); err != nil {
		t.Fatal(err)
	}
	var job batchv1.Job
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: workerJobName(testNS, pod)}, &job); err != nil {
		t.Fatal(err)
	}
	if job.Spec.Template.Spec.NodeName != "node-a" {
		t.Errorf("worker not pinned to the workload's node: %q", job.Spec.Template.Spec.NodeName)
	}
	ref := workspace.JobRef(&job)
	if ref.ClaimName != "data" || ref.ClaimUID != "uid-data" || ref.VolumeName != "pv-1" || ref.VolumeUID != "uid-pv-1" ||
		ref.TargetPodName != "app" || ref.TargetPodUID != "app-uid" {
		t.Fatalf("incomplete worker identity: %+v", ref)
	}
	if jobs := ws().Status.Jobs; len(jobs) != 1 || jobs[0] != ref {
		t.Fatalf("worker Job not registered with its identity: %+v", jobs)
	}
}

func TestWorkerRefusesReadWriteOncePodClaim(t *testing.T) {
	claim := boundClaim()
	claim.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod}
	r, _, _ := workerReconciler(t, claim)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "app"}}); err == nil {
		t.Fatal("a ReadWriteOncePod claim got a worker")
	}
	var jobs batchv1.JobList
	_ = r.List(context.Background(), &jobs)
	if len(jobs.Items) != 0 {
		t.Fatalf("created %d Jobs", len(jobs.Items))
	}
}
