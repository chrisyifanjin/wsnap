package controller

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
)

// The controller crashed (or the status update failed) between creating a
// worker Job and registering it. The broker refuses an unregistered Job,
// so a later reconcile must replace it, not leave it waiting forever.
func TestUnregisteredWorkerJobIsReplaced(t *testing.T) {
	r, pod, ws := workerReconciler(t, boundClaim())
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "app"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	// Simulate the lost registration.
	w := ws()
	w.Status.Jobs = nil
	if err := r.Status().Update(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Logf("reconcile %d: %v", i, err)
		}
	}
	var job batchv1.Job
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: workerJobName(testNS, pod)}, &job); err != nil {
		t.Fatal(err)
	}
	if len(ws().Status.Jobs) == 0 {
		t.Errorf("worker Job %s exists but is never registered (the broker refuses it forever)", job.Name)
	}
}

// A worker Pod that claimed a request dies (OOM, eviction) while its
// workload runs; the Job starts a new Pod. The request must go back to
// Pending for the new Pod, since redelivery goes only to the claimant.
func TestDeadWorkerClaimIsRerouted(t *testing.T) {
	sr := snapReq(time.Now(), wsnapv1.SnapshotRunning, "worker")
	sr.Status.ClaimedBy = "dead-worker-pod"
	app := runningPod("app", "app-uid", "data")
	app.Labels = map[string]string{wsnapv1.LabelWorkspace: "ws1", wsnapv1.LabelRepository: "repo"}
	deadWorker := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "wsnap-worker-x-1", UID: "dead-worker-pod",
		Labels: map[string]string{wsnapv1.LabelJob: "worker", wsnapv1.LabelJobWorkspace: "ws1"}},
		Status: corev1.PodStatus{Phase: corev1.PodFailed}}
	ws := wsWith(pvcVol, &wsnapv1.PodRef{Name: "app", UID: "app-uid"})
	ws.Spec.Execution.Mode = wsnapv1.ExecutionWorker
	r, c := requestEnv(t, sr, app, deadWorker, ws, boundClaim())
	reconcileReq(t, r, c)
	got := reconcileReq(t, r, c)
	if got.Status.Phase == wsnapv1.SnapshotRunning && got.Status.ClaimedBy == "dead-worker-pod" {
		t.Errorf("request still Running, claimed by the dead worker Pod (stuck until RunningTimeout=%s): %+v", RunningTimeout, got.Status)
	}
}
