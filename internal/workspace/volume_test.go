package workspace

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
)

func TestResolveVolumeRequiresClaimAndPVIdentity(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "agent"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}}}}
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "data", UID: "claim-1"},
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-1"}}
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-1", UID: "volume-1"},
		Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Namespace: "team-a", Name: "data", UID: "claim-1"}}}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(claim, pv).Build()
	ref, err := ResolveVolume(ctx, c, pod)
	if err != nil || ref.ClaimUID != "claim-1" || ref.VolumeUID != "volume-1" {
		t.Fatalf("resolved volume = %+v, error %v", ref, err)
	}
	if err := ValidateBinding(ctx, c, pod.Namespace, ref); err != nil {
		t.Fatalf("bound identity rejected: %v", err)
	}
	for _, tc := range []struct {
		name string
		edit func(*corev1.PersistentVolumeClaim, *corev1.PersistentVolume)
	}{
		{"claim replaced under same name", func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) { pvc.UID = "claim-2" }},
		{"claim moved to another PV", func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) { pvc.Spec.VolumeName = "pv-2" }},
		{"PV replaced under same name", func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) { pv.UID = "volume-2" }},
		{"PV bound to another claim", func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) { pv.Spec.ClaimRef.UID = "claim-2" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			otherClaim, otherPV := claim.DeepCopy(), pv.DeepCopy()
			tc.edit(otherClaim, otherPV)
			if err := CheckBinding(pod.Namespace, ref, otherClaim, otherPV); !errors.Is(err, ErrBadVolume) {
				t.Fatalf("binding error = %v, want ErrBadVolume", err)
			}
		})
	}
	deleting := claim.DeepCopy()
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	if err := CheckBinding(pod.Namespace, ref, deleting, pv); err != nil {
		t.Fatalf("bound claim cannot take a final snapshot while deleting: %v", err)
	}
	// Protection must also reject a PV that is already owned by another
	// Workspace, even if its Kubernetes claim is otherwise valid.
	var current corev1.PersistentVolume
	if err := c.Get(ctx, client.ObjectKey{Name: "pv-1"}, &current); err != nil {
		t.Fatal(err)
	}
	current.Annotations = map[string]string{wsnapv1.AnnotationWorkspaceRef: "team-a/other"}
	if err := c.Update(ctx, &current); err != nil {
		t.Fatal(err)
	}
	if err := Protect(ctx, c, c, Identity{Namespace: "team-a", Name: "mine"}, ref); !errors.Is(err, ErrBadVolume) {
		t.Fatalf("foreign workspace protection error = %v", err)
	}
}
