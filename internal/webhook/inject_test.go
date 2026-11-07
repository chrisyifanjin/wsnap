package webhook

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
)

var cfg = Config{AgentImage: "agent:1", BrokerURL: "http://b", Audience: "wsnap"}

func labeledPod(mutate ...func(*corev1.Pod)) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns",
			Labels: map[string]string{wsnapv1.LabelWorkspace: "ws", wsnapv1.LabelRepository: "repo"}},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "agent", Image: "any/agent:latest",
				VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: "/home/agent"}}}},
			Volumes: []corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
		},
	}
	for _, m := range mutate {
		m(p)
	}
	return p
}

func TestInjectsSidecarIntoAnyAgentImage(t *testing.T) {
	p := labeledPod(func(p *corev1.Pod) {
		p.Spec.InitContainers = []corev1.Container{{Name: "setup", Image: "busybox"}}
		p.Annotations = map[string]string{AnnotationQuiesceTimeout: "5s"}
	})
	if err := Mutate(p, cfg); err != nil {
		t.Fatal(err)
	}
	sc := p.Spec.InitContainers[0]
	if sc.Name != "wsnap" || *sc.RestartPolicy != corev1.ContainerRestartPolicyAlways || sc.StartupProbe == nil {
		t.Fatalf("sidecar not injected first as a native sidecar: %+v", p.Spec.InitContainers)
	}
	if p.Spec.InitContainers[1].Name != "setup" {
		t.Error("the user's init container was not kept after the sidecar")
	}
	args := strings.Join(sc.Args, " ")
	if !strings.Contains(args, "--quiesce-timeout=5s") || !strings.Contains(args, "--broker-url=http://b") {
		t.Errorf("args = %s", args)
	}
	if *p.Spec.AutomountServiceAccountToken || *p.Spec.TerminationGracePeriodSeconds < 60 {
		t.Errorf("automount %v grace %d", *p.Spec.AutomountServiceAccountToken, *p.Spec.TerminationGracePeriodSeconds)
	}
	if sc.SecurityContext == nil || !*sc.SecurityContext.ReadOnlyRootFilesystem || sc.SecurityContext.Capabilities.Drop[0] != "ALL" {
		t.Errorf("injected sidecar is not hardened: %+v", sc.SecurityContext)
	}
	if p.Spec.Containers[0].Image != "any/agent:latest" || len(p.Spec.Containers[0].VolumeMounts) != 1 {
		t.Error("the workload container was changed")
	}
	before := len(p.Spec.InitContainers)
	if err := Mutate(p, cfg); err != nil || len(p.Spec.InitContainers) != before {
		t.Errorf("second mutation not idempotent: %v, %d init containers", err, len(p.Spec.InitContainers))
	}
}

func TestTLSInjection(t *testing.T) {
	p := labeledPod()
	c := cfg
	c.BrokerURL, c.CAConfigMap = "https://b", "wsnap-ca"
	if err := Mutate(p, c); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(p.Spec.InitContainers[0].Args, " "), "--ca-file=/var/run/wsnap-ca/ca.crt") || !hasVolume(p, caVolume) {
		t.Error("CA not wired into the sidecar")
	}
}

// TestFailClosed: Pods that cannot be protected are refused.
func TestFailClosed(t *testing.T) {
	cases := map[string]*corev1.Pod{
		"no workspace volume": labeledPod(func(p *corev1.Pod) { p.Spec.Volumes = nil }),
		"API token mounted":   labeledPod(func(p *corev1.Pod) { p.Spec.AutomountServiceAccountToken = ptr.To(true) }),
		"invalid workspace":   labeledPod(func(p *corev1.Pod) { p.Labels[wsnapv1.LabelWorkspace] = "Not_A_Label" }),
		"no repository":       labeledPod(func(p *corev1.Pod) { delete(p.Labels, wsnapv1.LabelRepository) }),
		"sidecar as a regular container": labeledPod(func(p *corev1.Pod) {
			p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "wsnap"})
		}),
		"opt-out without a sidecar": labeledPod(func(p *corev1.Pod) { p.Annotations = map[string]string{AnnotationInject: "false"} }),
		"own sidecar without startupProbe": labeledPod(func(p *corev1.Pod) {
			p.Spec.InitContainers = []corev1.Container{{Name: "wsnap", RestartPolicy: ptr.To(corev1.ContainerRestartPolicyAlways)}}
		}),
		"workload mounts the token": labeledPod(func(p *corev1.Pod) {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: "tok", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
				Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Audience: "wsnap", Path: "t"}}}}}})
			p.Spec.Containers[0].VolumeMounts = append(p.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "tok", MountPath: "/t"})
		}),
		"bad final timeout":    labeledPod(func(p *corev1.Pod) { p.Annotations = map[string]string{AnnotationFinalTimeout: "soon"} }),
		"shared PID namespace": labeledPod(func(p *corev1.Pod) { p.Spec.ShareProcessNamespace = ptr.To(true) }),
		"host PID namespace":   labeledPod(func(p *corev1.Pod) { p.Spec.HostPID = true }),
	}
	for name, p := range cases {
		t.Run(name, func(t *testing.T) {
			if err := Mutate(p, cfg); err == nil {
				t.Fatal("admitted a pod that cannot be protected")
			}
		})
	}
}

func TestOwnSidecarIsValidatedNotReplaced(t *testing.T) {
	p := labeledPod(func(p *corev1.Pod) {
		p.Spec.InitContainers = []corev1.Container{{Name: "wsnap", Image: "my/agent", RestartPolicy: ptr.To(corev1.ContainerRestartPolicyAlways),
			StartupProbe: &corev1.Probe{}}}
	})
	if err := Mutate(p, cfg); err != nil {
		t.Fatal(err)
	}
	if len(p.Spec.InitContainers) != 1 || p.Spec.InitContainers[0].Image != "my/agent" {
		t.Errorf("own sidecar replaced: %+v", p.Spec.InitContainers)
	}
}

func TestOtherAudienceTokensAreAllowed(t *testing.T) {
	p := labeledPod(func(p *corev1.Pod) {
		p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: "vault", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
			Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Audience: "vault", Path: "t"}}}}}})
		p.Spec.Containers[0].VolumeMounts = append(p.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "vault", MountPath: "/v"})
	})
	if err := Mutate(p, cfg); err != nil {
		t.Fatalf("a token for another audience was refused: %v", err)
	}
}

func TestEphemeralContainersCannotReachTheSidecar(t *testing.T) {
	p := labeledPod()
	if err := Mutate(p, cfg); err != nil {
		t.Fatal(err)
	}
	debug := func(target string, mounts ...corev1.VolumeMount) *corev1.Pod {
		q := p.DeepCopy()
		q.Spec.EphemeralContainers = []corev1.EphemeralContainer{{TargetContainerName: target,
			EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug", VolumeMounts: mounts}}}
		return q
	}
	if err := ValidateEphemeral(debug("agent"), "wsnap", "wsnap"); err != nil {
		t.Errorf("debugging the workload was refused: %v", err)
	}
	if err := ValidateEphemeral(debug("wsnap"), "wsnap", "wsnap"); err == nil {
		t.Error("an ephemeral container may share the sidecar's process namespace")
	}
	if err := ValidateEphemeral(debug("agent", corev1.VolumeMount{Name: tokenVolume, MountPath: "/t"}), "wsnap", "wsnap"); err == nil {
		t.Error("an ephemeral container may mount the wsnap token")
	}
}
