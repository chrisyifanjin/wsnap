package controller

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
)

func TestStateCollector(t *testing.T) {
	failing := testWorkspace(func(ws *wsnapv1.Workspace) { ws.Status.LastError = "snapshot failed" })
	ok := testWorkspace(func(ws *wsnapv1.Workspace) {
		ws.Name = "ws2"
		ws.Status.SupersededSnapshots = []wsnapv1.SnapshotRecord{{ID: "old"}}
	})
	failedRescue := &wsnapv1.WorkspaceSnapshot{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "wsnap-unsaved-x",
		Labels:      map[string]string{wsnapv1.LabelManagedBy: wsnapv1.ManagedByValue},
		Annotations: map[string]string{wsnapv1.AnnotationVolumeName: "pv-a"}},
		Status: wsnapv1.WorkspaceSnapshotStatus{Phase: wsnapv1.SnapshotFailed}}
	pv := func(name, phase string) *corev1.PersistentVolume {
		return &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: name,
			Labels: map[string]string{wsnapv1.LabelProtected: "true"}, Annotations: map[string]string{wsnapv1.AnnotationRescue: phase}}}
	}
	_, c := newPVReconciler(t, failing, ok, failedRescue, pv("a", wsnapv1.RescueFailed), pv("b", wsnapv1.RescueRunning), pv("c", wsnapv1.RescueDone))
	want := `
# HELP wsnap_rescue_requests_failed Failed snapshot requests for a stopped writer's volume (wsnap-unsaved-*): its data may be in no snapshot. Stays until the request is deleted.
# TYPE wsnap_rescue_requests_failed gauge
wsnap_rescue_requests_failed 1
# HELP wsnap_volumes_rescue Protected PVs by rescue phase: running (a mover Job saves it) or failed (kept for an operator).
# TYPE wsnap_volumes_rescue gauge
wsnap_volumes_rescue{phase="failed"} 1
wsnap_volumes_rescue{phase="running"} 1
# HELP wsnap_workspaces_failing Workspaces whose last snapshot, restore or maintenance failed (status.lastError is set).
# TYPE wsnap_workspaces_failing gauge
wsnap_workspaces_failing 1
# HELP wsnap_workspaces_with_superseded_snapshots Workspaces whose status.supersededSnapshots is not empty: an older writer's data was saved after newer history and is not what restores use.
# TYPE wsnap_workspaces_with_superseded_snapshots gauge
wsnap_workspaces_with_superseded_snapshots 1
`
	if err := testutil.CollectAndCompare(StateCollector{Reader: c}, strings.NewReader(want), "wsnap_workspaces_failing", "wsnap_volumes_rescue", "wsnap_rescue_requests_failed", "wsnap_workspaces_with_superseded_snapshots"); err != nil {
		t.Fatal(err)
	}
}
