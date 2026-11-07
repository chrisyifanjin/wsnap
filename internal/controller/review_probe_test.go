package controller

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
)

// PROBE: the writer Pod uses a generic ephemeral volume and still exists
// (starting up: phase Pending while the sidecar restores; or terminating
// while it takes its final snapshot). The request reconciler does not see
// it as a Pod that mounts the claim and starts a Job on the same volume.
func TestProbeJobStartedBesideEphemeralWriter(t *testing.T) {
	for _, tc := range []struct {
		name  string
		phase corev1.PodPhase
		del   bool
	}{{"writer starting (Pending)", corev1.PodPending, false}, {"writer terminating", corev1.PodRunning, true}} {
		t.Run(tc.name, func(t *testing.T) {
			vol := &wsnapv1.VolumeRef{Kind: workspace.KindEphemeral, ClaimName: "p-workspace", VolumeName: "pv-1"}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "p", UID: "u",
				Labels: map[string]string{wsnapv1.LabelWorkspace: "ws1"}},
				Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{
					Ephemeral: &corev1.EphemeralVolumeSource{}}}}},
				Status: corev1.PodStatus{Phase: tc.phase}}
			if tc.del {
				now := metav1.Now()
				pod.DeletionTimestamp = &now
				pod.Finalizers = []string{"x"}
			}
			claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "p-workspace"},
				Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-1"}}
			r, c := requestEnv(t, wsWith(vol, &wsnapv1.PodRef{Name: "p", UID: "u"}), pod, claim, snapReq(time.Now(), "", ""))
			got := reconcileReq(t, r, c)
			var job batchv1.Job
			err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: requestJobName(request{snapReq(time.Now(), "", "")})}, &job)
			t.Logf("phase=%q executor=%q msg=%q jobCreated=%v", got.Status.Phase, got.Status.Executor, got.Status.Message, err == nil)
			if err == nil {
				t.Errorf("BUG: mover Job created on volume pv-1 while writer pod p (with its sidecar) still mounts it")
			}
		})
	}
}
