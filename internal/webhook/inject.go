// Package webhook injects the wsnap sidecar into workspace Pods and rejects
// Pods that could not be protected. Admission fails closed: a Pod labeled
// wsnap.dev/workspace is either made safe or refused, never admitted
// unprotected.
package webhook

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
)

// Annotations users set on the Pod template.
const (
	// AnnotationInject "false" means the Pod brings its own sidecar; the
	// webhook only validates it.
	AnnotationInject = "wsnap.dev/inject"
	// AnnotationQuiesceTimeout enables the quiesce handshake, e.g. "10s".
	AnnotationQuiesceTimeout = "wsnap.dev/quiesce-timeout"
	// AnnotationFinalTimeout bounds the final snapshot, e.g. "50s".
	AnnotationFinalTimeout = "wsnap.dev/final-timeout"
	// AnnotationWorkloadGrace is how long the workload may take to exit
	// after SIGTERM, e.g. "30s". Native sidecars stop after the workload,
	// inside the same grace period, so the final snapshot only gets what
	// the workload leaves.
	AnnotationWorkloadGrace = "wsnap.dev/workload-grace"
)

// Config is what injection needs from the controller.
type Config struct {
	AgentImage string
	BrokerURL  string
	Audience   string
	// CAConfigMap names the ConfigMap with the broker's CA; empty for HTTP.
	CAConfigMap string
	// AllowAPIToken admits workload containers that mount an API server
	// token (--allow-automounted-tokens).
	AllowAPIToken bool
}

const (
	tokenVolume = "wsnap-token"
	stateVolume = "wsnap-state"
	caVolume    = "wsnap-ca"
	dataPath    = "/workspace"

	defaultFinalTimeout = 50 * time.Second
	// defaultWorkloadGrace is the workload's own shutdown budget.
	defaultWorkloadGrace = 30 * time.Second
	// graceMargin is kept between the final snapshot's deadline and the
	// kubelet's SIGKILL.
	graceMargin = 10 * time.Second
)

// Mutate injects the sidecar into pod, or explains why pod cannot be
// admitted. It is idempotent, so reinvocation is safe.
func Mutate(pod *corev1.Pod, cfg Config) error {
	if _, err := workspace.FromPod(pod); err != nil {
		return err
	}
	volName := pod.Annotations[wsnapv1.AnnotationVolume]
	if volName == "" {
		volName = wsnapv1.DefaultVolumeName
	}
	if !hasVolume(pod, volName) {
		return fmt.Errorf("pod has no volume %q to protect (set annotation %s to the workspace volume's name)", volName, wsnapv1.AnnotationVolume)
	}
	if ptr.Deref(pod.Spec.ShareProcessNamespace, false) || pod.Spec.HostPID {
		return fmt.Errorf("shareProcessNamespace and hostPID let the workload read the sidecar's environment and files through /proc; turn them off")
	}
	if a := pod.Spec.AutomountServiceAccountToken; a != nil && *a {
		return fmt.Errorf("automountServiceAccountToken is true: untrusted containers would hold an API token; remove it or set it to false")
	}
	pod.Spec.AutomountServiceAccountToken = ptr.To(false)

	finalTimeout := defaultFinalTimeout
	if v := pod.Annotations[AnnotationFinalTimeout]; v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return fmt.Errorf("annotation %s=%q is not a positive duration", AnnotationFinalTimeout, v)
		}
		finalTimeout = d
	}
	var quiesce time.Duration
	if v := pod.Annotations[AnnotationQuiesceTimeout]; v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			return fmt.Errorf("annotation %s=%q is not a duration", AnnotationQuiesceTimeout, v)
		}
		quiesce = d
	}
	workloadGrace := defaultWorkloadGrace
	if v := pod.Annotations[AnnotationWorkloadGrace]; v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			return fmt.Errorf("annotation %s=%q is not a duration", AnnotationWorkloadGrace, v)
		}
		workloadGrace = d
	}
	needGrace := int64((workloadGrace + finalTimeout + graceMargin) / time.Second)
	if g := pod.Spec.TerminationGracePeriodSeconds; g == nil || *g < needGrace {
		pod.Spec.TerminationGracePeriodSeconds = ptr.To(needGrace)
	}

	sidecarName := workspace.SidecarName(pod)
	existing := findContainer(pod.Spec.InitContainers, sidecarName)
	if existing == nil && findContainer(pod.Spec.Containers, sidecarName) != nil {
		return fmt.Errorf("container %q is a regular container; the wsnap sidecar must be a native sidecar (an init container with restartPolicy: Always) so it restores before the workload starts", sidecarName)
	}
	if pod.Annotations[AnnotationInject] == "false" || existing != nil {
		return validate(pod, sidecarName, cfg.Audience, cfg.AllowAPIToken)
	}

	ensureVolume(pod, corev1.Volume{Name: tokenVolume, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
		Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
			Audience: cfg.Audience, ExpirationSeconds: ptr.To[int64](600), Path: "token"}}}}}})
	ensureVolume(pod, corev1.Volume{Name: stateVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
	args := []string{"sidecar", "--dir=" + dataPath, "--broker-url=" + cfg.BrokerURL,
		"--final-timeout=" + finalTimeout.String()}
	if quiesce > 0 {
		args = append(args, "--quiesce-timeout="+quiesce.String())
	}
	mounts := []corev1.VolumeMount{
		{Name: volName, MountPath: dataPath},
		{Name: tokenVolume, MountPath: "/var/run/secrets/wsnap", ReadOnly: true},
		{Name: stateVolume, MountPath: "/var/lib/wsnap"},
	}
	if cfg.CAConfigMap != "" {
		ensureVolume(pod, corev1.Volume{Name: caVolume, VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: cfg.CAConfigMap}}}})
		mounts = append(mounts, corev1.VolumeMount{Name: caVolume, MountPath: "/var/run/wsnap-ca", ReadOnly: true})
		args = append(args, "--ca-file=/var/run/wsnap-ca/ca.crt")
	}
	sidecar := corev1.Container{
		Name:          sidecarName,
		Image:         cfg.AgentImage,
		Args:          args,
		RestartPolicy: ptr.To(corev1.ContainerRestartPolicyAlways),
		// The workload starts only after the volume is restored. A failed
		// probe restarts the sidecar, which starts the restore again from
		// zero, so the limit (4 hours) must exceed any wait for a previous
		// writer or a rescue (a mover Job has 2 hours) plus the restore.
		StartupProbe: &corev1.Probe{
			ProbeHandler:     corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"/usr/local/bin/wsnap-agent", "ready", "--state-dir=/var/lib/wsnap"}}},
			PeriodSeconds:    1,
			FailureThreshold: 4 * 3600,
		},
		VolumeMounts: mounts,
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("50m"), corev1.ResourceMemory: resource.MustParse("64Mi")}},
		SecurityContext: AgentSecurityContext(),
	}
	// First, so the restore finishes before any other init container (which
	// might write to the workspace) starts.
	pod.Spec.InitContainers = append([]corev1.Container{sidecar}, pod.Spec.InitContainers...)
	return validate(pod, sidecarName, cfg.Audience, cfg.AllowAPIToken)
}

// AgentSecurityContext hardens the agent container. Its root filesystem
// is read-only, so even a path trick from the workload cannot replace the
// agent or engine binaries; it keeps only the capabilities needed to read
// every workspace file and restore owners and modes.
func AgentSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		// Always root, whatever the Pod-level runAsUser: the agent must read
		// and restore every file, and every agent on a volume (sidecar,
		// mover, restorer) must be the same uid to trust .wsnap.
		RunAsUser:                ptr.To[int64](0),
		RunAsGroup:               ptr.To[int64](0),
		RunAsNonRoot:             ptr.To(false),
		ReadOnlyRootFilesystem:   ptr.To(true),
		AllowPrivilegeEscalation: ptr.To(false),
		Capabilities: &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
			Add:  []corev1.Capability{"CHOWN", "DAC_OVERRIDE", "DAC_READ_SEARCH", "FOWNER", "FSETID"},
		},
	}
}

// validate checks a Pod that already has its sidecar.
func validate(pod *corev1.Pod, sidecarName, audience string, allowAPIToken bool) error {
	sc := findContainer(pod.Spec.InitContainers, sidecarName)
	if sc == nil {
		return fmt.Errorf("annotation %s=false, but the pod has no %q init container", AnnotationInject, sidecarName)
	}
	if sc.RestartPolicy == nil || *sc.RestartPolicy != corev1.ContainerRestartPolicyAlways {
		return fmt.Errorf("container %q must have restartPolicy: Always (a native sidecar)", sidecarName)
	}
	if sc.StartupProbe == nil {
		return fmt.Errorf("container %q needs a startupProbe, or the workload starts before the restore finishes", sidecarName)
	}
	return workspace.CheckIsolation(pod, audience, allowAPIToken)
}

// ValidateEphemeral refuses ephemeral containers (kubectl debug) that could
// reach the agent's secrets: one that mounts the token volume, or one that
// targets the sidecar and so shares its process namespace.
func ValidateEphemeral(pod *corev1.Pod, audience string, allowAPIToken bool) error {
	return workspace.CheckIsolation(pod, audience, allowAPIToken)
}

func hasVolume(pod *corev1.Pod, name string) bool {
	for _, v := range pod.Spec.Volumes {
		if v.Name == name {
			return true
		}
	}
	return false
}

func ensureVolume(pod *corev1.Pod, v corev1.Volume) {
	if !hasVolume(pod, v.Name) {
		pod.Spec.Volumes = append(pod.Spec.Volumes, v)
	}
}

func findContainer(list []corev1.Container, name string) *corev1.Container {
	for i := range list {
		if list[i].Name == name {
			return &list[i]
		}
	}
	return nil
}
