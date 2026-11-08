// Package webhook injects the wsnap sidecar into workspace Pods and rejects
// Pods that could not be protected. Admission fails closed: a Pod labeled
// wsnap.dev/workspace is either made safe or refused, never admitted
// unprotected.
package webhook

import (
	"fmt"
	"slices"
	"time"

	"github.com/chrisyifanjin/wsnap/internal/agentpod"

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
	// ExecutionMode comes from Workspace.spec.execution.mode. A missing
	// Workspace uses the legacy sidecar mode.
	ExecutionMode string
	// CAConfigMap names the ConfigMap with the broker's CA; empty for HTTP.
	CAConfigMap string
	// CABundle returns the CA of a broker that manages its own
	// certificate. Agents get it inline in WSNAP_CA_PEM. CAConfigMap wins
	// when both are set.
	CABundle func() []byte
	// AllowAPIToken admits workload containers that mount an API server
	// token (--allow-automounted-tokens).
	AllowAPIToken bool
}

const (
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
	if cfg.ExecutionMode == wsnapv1.ExecutionWorker {
		return mutateWorkerPod(pod, cfg)
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
	if !cfg.AllowAPIToken {
		removeAPITokens(pod)
	}

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
		if existing != nil && !agentpod.HasCA(existing) {
			// A Pod's own sidecar cannot know a CA that the broker made.
			existing.Env = append(existing.Env, cfg.trust().Env()...)
		}
		return validate(pod, sidecarName, cfg.Audience, cfg.AllowAPIToken)
	}

	ensureVolume(pod, agentpod.TokenVolumeFor(cfg.Audience))
	ensureVolume(pod, agentpod.StateVolumeSpec())
	args := []string{"sidecar", "--dir=" + agentpod.DataPath, "--broker-url=" + cfg.BrokerURL,
		"--final-timeout=" + finalTimeout.String()}
	if quiesce > 0 {
		args = append(args, "--quiesce-timeout="+quiesce.String())
	}
	mounts := append([]corev1.VolumeMount{{Name: volName, MountPath: agentpod.DataPath}}, agentpod.AgentMounts()...)
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
		SecurityContext: agentpod.SecurityContext(),
	}
	trust := cfg.trust()
	if v, ok := trust.Volume(); ok {
		ensureVolume(pod, v)
	}
	trust.Apply(&sidecar)
	// First, so the restore finishes before any other init container (which
	// might write to the workspace) starts.
	pod.Spec.InitContainers = append([]corev1.Container{sidecar}, pod.Spec.InitContainers...)
	return validate(pod, sidecarName, cfg.Audience, cfg.AllowAPIToken)
}

func (cfg Config) trust() agentpod.Trust {
	return agentpod.Trust{ConfigMap: cfg.CAConfigMap, Bundle: cfg.CABundle}
}

// mutateWorkerPod gates the workload on a separate worker's restore. The
// gate has no credential or API token; it only reads a per-Pod readiness
// marker after the worker has claimed the workspace writer epoch.
func mutateWorkerPod(pod *corev1.Pod, cfg Config) error {
	volName := pod.Annotations[wsnapv1.AnnotationVolume]
	if volName == "" {
		volName = wsnapv1.DefaultVolumeName
	}
	var vol *corev1.Volume
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].Name == volName {
			vol = &pod.Spec.Volumes[i]
			break
		}
	}
	if vol == nil || (vol.PersistentVolumeClaim == nil && vol.Ephemeral == nil) {
		return fmt.Errorf("worker mode requires volume %q to be a persistentVolumeClaim or generic ephemeral PVC", volName)
	}
	if vol.Ephemeral != nil && vol.Ephemeral.VolumeClaimTemplate != nil {
		for _, mode := range vol.Ephemeral.VolumeClaimTemplate.Spec.AccessModes {
			if mode == corev1.ReadWriteOncePod {
				return fmt.Errorf("worker mode cannot share a ReadWriteOncePod claim between Pods")
			}
		}
	}
	if ptr.Deref(pod.Spec.ShareProcessNamespace, false) || pod.Spec.HostPID {
		return fmt.Errorf("worker mode does not allow shareProcessNamespace or hostPID")
	}
	if a := pod.Spec.AutomountServiceAccountToken; a != nil && *a {
		return fmt.Errorf("automountServiceAccountToken is true: untrusted containers would hold an API token")
	}
	pod.Spec.AutomountServiceAccountToken = ptr.To(false)
	if !cfg.AllowAPIToken {
		removeAPITokens(pod)
	}
	if findContainer(pod.Spec.Containers, agentpod.GateName) != nil {
		return fmt.Errorf("container name %q is reserved for the worker readiness gate", agentpod.GateName)
	}
	// Always wsnap's own gate, first: a gate the Pod brings could let the
	// workload start before the worker restored the volume.
	pod.Spec.InitContainers = slices.DeleteFunc(pod.Spec.InitContainers, func(c corev1.Container) bool { return c.Name == agentpod.GateName })
	gate := agentpod.Gate(cfg.AgentImage, volName)
	pod.Spec.InitContainers = append([]corev1.Container{gate}, pod.Spec.InitContainers...)
	return workspace.CheckWorkerIsolation(pod, cfg.Audience, cfg.AllowAPIToken)
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

// removeAPITokens takes API server tokens out of the Pod. The
// ServiceAccount admission plugin runs before mutating webhooks, so when a
// Pod does not set automountServiceAccountToken itself, its
// kube-api-access volume is already mounted into every container by the
// time this webhook sets the field to false. Removing the projected volumes
// that carry an API token (no audience), and their mounts, gives the Pod
// what automountServiceAccountToken: false would have given it.
func removeAPITokens(pod *corev1.Pod) {
	drop := map[string]bool{}
	pod.Spec.Volumes = slices.DeleteFunc(pod.Spec.Volumes, func(v corev1.Volume) bool {
		if v.Projected == nil {
			return false
		}
		for _, src := range v.Projected.Sources {
			if src.ServiceAccountToken != nil && src.ServiceAccountToken.Audience == "" {
				drop[v.Name] = true
				return true
			}
		}
		return false
	})
	if len(drop) == 0 {
		return
	}
	strip := func(cs []corev1.Container) {
		for i := range cs {
			cs[i].VolumeMounts = slices.DeleteFunc(cs[i].VolumeMounts, func(m corev1.VolumeMount) bool { return drop[m.Name] })
		}
	}
	strip(pod.Spec.InitContainers)
	strip(pod.Spec.Containers)
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
