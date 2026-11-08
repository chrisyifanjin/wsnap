package workspace

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

func TestCheckWorkerIsolation(t *testing.T) {
	token := func(name, audience string) corev1.Volume {
		return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
			Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Audience: audience, Path: "t"}}}}}}
	}
	pod := func(mutate func(*corev1.Pod)) *corev1.Pod {
		p := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "agent"}}}}
		mutate(p)
		return p
	}
	for _, tc := range []struct {
		name          string
		pod           *corev1.Pod
		allowAPIToken bool
		wantErr       bool
	}{
		{"clean", pod(func(*corev1.Pod) {}), false, false},
		{"shared process namespace", pod(func(p *corev1.Pod) { p.Spec.ShareProcessNamespace = ptr.To(true) }), false, true},
		{"wsnap token projected, even unmounted", pod(func(p *corev1.Pod) { p.Spec.Volumes = []corev1.Volume{token("w", "wsnap")} }), false, true},
		{"API token mounted by an ephemeral container", pod(func(p *corev1.Pod) {
			p.Spec.Volumes = []corev1.Volume{token("kube-api", "")}
			p.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{
				Name: "debug", VolumeMounts: []corev1.VolumeMount{{Name: "kube-api", MountPath: "/t"}}}}}
		}), false, true},
		{"API token allowed by flag", pod(func(p *corev1.Pod) {
			p.Spec.Volumes = []corev1.Volume{token("kube-api", "")}
			p.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "kube-api", MountPath: "/t"}}
		}), true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckWorkerIsolation(tc.pod, "wsnap", tc.allowAPIToken)
			if (err != nil) != tc.wantErr {
				t.Fatalf("CheckWorkerIsolation() = %v, want error %v", err, tc.wantErr)
			}
		})
	}
}

func TestHasFinalSnapshot(t *testing.T) {
	for _, tc := range []struct {
		writer, final string
		want          bool
	}{
		{"pod-a", "pod-a", true},
		{"pod-b", "pod-a", false}, // a newer writer after the final snapshot
		{"", "", false},           // never written by a workspace Pod
		{"pod-a", "", false},
	} {
		a := map[string]string{AnnotationWriterPod: tc.writer, AnnotationFinalPod: tc.final}
		if got := HasFinalSnapshot(a); got != tc.want {
			t.Errorf("HasFinalSnapshot(writer %q, final %q) = %v, want %v", tc.writer, tc.final, got, tc.want)
		}
	}
}
