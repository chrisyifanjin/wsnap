package webhook

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
)

func pvcPod(claim string) *corev1.Pod {
	return labeledPod(func(p *corev1.Pod) {
		p.Spec.Volumes[0].VolumeSource = corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim}}
	})
}

func TestWorkerModeAddsOnlyAGate(t *testing.T) {
	p := pvcPod("data")
	wcfg := cfg
	wcfg.ExecutionMode = wsnapv1.ExecutionWorker
	if err := Mutate(p, wcfg); err != nil {
		t.Fatal(err)
	}
	if len(p.Spec.InitContainers) != 1 || p.Spec.InitContainers[0].Name != "wsnap-gate" || p.Spec.InitContainers[0].RestartPolicy != nil {
		t.Fatalf("want one plain gate init container, got %+v", p.Spec.InitContainers)
	}
	for _, v := range p.Spec.Volumes {
		if v.Projected != nil {
			t.Fatalf("worker-mode workload got a token volume: %+v", v)
		}
	}
	if m := p.Spec.InitContainers[0].VolumeMounts; len(m) != 1 || !m[0].ReadOnly {
		t.Errorf("gate must mount only the workspace, read-only: %+v", m)
	}
}

func TestWorkerModeRefusesUnsharableVolumes(t *testing.T) {
	wcfg := cfg
	wcfg.ExecutionMode = wsnapv1.ExecutionWorker
	if err := Mutate(labeledPod(), wcfg); err == nil {
		t.Error("an emptyDir workspace was admitted in worker mode")
	}
	rwop := labeledPod(func(p *corev1.Pod) {
		p.Spec.Volumes[0].VolumeSource = corev1.VolumeSource{Ephemeral: &corev1.EphemeralVolumeSource{
			VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod}}}}}
	})
	if err := Mutate(rwop, wcfg); err == nil {
		t.Error("a ReadWriteOncePod ephemeral claim was admitted in worker mode")
	}
}

func handle(t *testing.T, h *Handler, p *corev1.Pod) admission.Response {
	t.Helper()
	raw, _ := json.Marshal(p)
	return h.Handle(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Namespace: p.Namespace, Operation: admissionv1.Create, Object: runtime.RawExtension{Raw: raw}}})
}

func TestWorkerModeRefusesReadWriteOncePodClaims(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = wsnapv1.AddToScheme(scheme)
	claim := func(name string, mode corev1.PersistentVolumeAccessMode) *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name},
			Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{mode}}}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&wsnapv1.Workspace{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "ws"},
			Spec: wsnapv1.WorkspaceSpec{RepositoryName: "repo", Execution: wsnapv1.WorkspaceExecutionSpec{Mode: wsnapv1.ExecutionWorker}}},
		claim("rwop", corev1.ReadWriteOncePod), claim("rwo", corev1.ReadWriteOnce)).Build()
	h := &Handler{Config: cfg, Reader: c, Decoder: admission.NewDecoder(scheme)}
	if r := handle(t, h, pvcPod("rwop")); r.Allowed || !strings.Contains(r.Result.Message, "ReadWriteOncePod") {
		t.Errorf("ReadWriteOncePod claim: allowed=%v %s", r.Allowed, r.Result.Message)
	}
	if r := handle(t, h, pvcPod("rwo")); !r.Allowed {
		t.Errorf("ReadWriteOnce claim refused: %s", r.Result.Message)
	}
	if r := handle(t, h, pvcPod("not-created-yet")); !r.Allowed {
		t.Errorf("a claim created after the Pod was refused: %s", r.Result.Message)
	}
}

func TestWorkerModeReplacesABroughtGate(t *testing.T) {
	p := pvcPod("data")
	p.Spec.InitContainers = []corev1.Container{{Name: "wsnap-gate", Image: "busybox", Command: []string{"true"}}}
	wcfg := cfg
	wcfg.ExecutionMode = wsnapv1.ExecutionWorker
	if err := Mutate(p, wcfg); err != nil {
		t.Fatal(err)
	}
	if len(p.Spec.InitContainers) != 1 || p.Spec.InitContainers[0].Image != cfg.AgentImage || p.Spec.InitContainers[0].Command != nil {
		t.Fatalf("a gate the Pod brought was kept: %+v", p.Spec.InitContainers)
	}
}
