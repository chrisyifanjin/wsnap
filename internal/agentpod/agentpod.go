// Package agentpod owns what every wsnap agent container looks like,
// whether the webhook injects it as a sidecar or the controller runs it in
// a Job: its security context, its token and state volumes, how it trusts
// the broker's CA, and the worker-mode gate. The webhook and the
// controllers both build agents from here, so the rules cannot drift.
package agentpod

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

// Volume names and mount paths inside agent containers.
const (
	TokenVolume = "wsnap-token"
	StateVolume = "wsnap-state"
	CAVolume    = "wsnap-ca"

	DataPath  = "/workspace"
	TokenPath = "/var/run/secrets/wsnap"
	StatePath = "/var/lib/wsnap"
	CAPath    = "/var/run/wsnap-ca"

	// TokenExpiry is the projected token lifetime in seconds. Kubernetes
	// refreshes the file before it expires; 600 is the minimum it accepts.
	TokenExpiry = 600

	// EnvCAPEM carries an inline broker CA (see Trust).
	EnvCAPEM = "WSNAP_CA_PEM"

	// GateName is the worker-mode init container in the workload Pod.
	GateName = "wsnap-gate"
)

// SecurityContext hardens an agent container. Its root filesystem is
// read-only, so even a path trick from the workload cannot replace the
// agent or engine binaries; it keeps only the capabilities needed to read
// every workspace file and restore owners and modes.
func SecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		// Always root, whatever the Pod-level runAsUser: the agent must read
		// and restore every file, and every agent on a volume (sidecar,
		// worker, mover, restorer) must be the same uid to trust .wsnap.
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

// TokenVolumeFor projects a service account token for the broker's
// audience. Only agent containers may mount it.
func TokenVolumeFor(audience string) corev1.Volume {
	return corev1.Volume{Name: TokenVolume, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
		Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
			Audience: audience, ExpirationSeconds: ptr.To[int64](TokenExpiry), Path: "token"}}}}}}
}

// StateVolumeSpec is the agent's private scratch space: engine
// configuration and caches, never shared with the workload.
func StateVolumeSpec() corev1.Volume {
	return corev1.Volume{Name: StateVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}
}

// AgentMounts are the token and state mounts every agent container has.
func AgentMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{Name: TokenVolume, MountPath: TokenPath, ReadOnly: true},
		{Name: StateVolume, MountPath: StatePath},
	}
}

// Trust says how an agent learns the broker's CA. An operator's ConfigMap
// (key ca.crt, in the workload namespace) wins; otherwise a CA that the
// broker manages itself is passed inline. Both empty: a plain-HTTP broker.
type Trust struct {
	ConfigMap string
	Bundle    func() []byte
}

// Volume returns the ConfigMap volume that Apply mounts, if any.
func (t Trust) Volume() (corev1.Volume, bool) {
	if t.ConfigMap == "" {
		return corev1.Volume{}, false
	}
	return corev1.Volume{Name: CAVolume, VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
		LocalObjectReference: corev1.LocalObjectReference{Name: t.ConfigMap}}}}, true
}

// Apply gives an agent container the CA: a ConfigMap mount and --ca-file,
// or the inline bundle in WSNAP_CA_PEM. The caller adds Volume to the Pod.
func (t Trust) Apply(c *corev1.Container) {
	if t.ConfigMap != "" {
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: CAVolume, MountPath: CAPath, ReadOnly: true})
		c.Args = append(c.Args, "--ca-file="+CAPath+"/ca.crt")
		return
	}
	c.Env = append(c.Env, t.Env()...)
}

// Env returns the inline CA, or nothing when a ConfigMap or no CA is used.
func (t Trust) Env() []corev1.EnvVar {
	if t.ConfigMap != "" || t.Bundle == nil {
		return nil
	}
	if b := t.Bundle(); len(b) > 0 {
		return []corev1.EnvVar{{Name: EnvCAPEM, Value: string(b)}}
	}
	return nil
}

// HasCA reports whether a container already names a CA, as a sidecar that
// a Pod brings itself may.
func HasCA(c *corev1.Container) bool {
	for _, e := range c.Env {
		if e.Name == EnvCAPEM || e.Name == "WSNAP_CA_FILE" {
			return true
		}
	}
	for _, a := range c.Args {
		if strings.HasPrefix(a, "--ca-file") {
			return true
		}
	}
	return false
}

// Gate is the worker-mode init container. It holds no credential: it
// waits until the worker has restored the volume and refreshes a marker
// file for this Pod (see agent.WaitWorker), and only reads the volume.
func Gate(image, workspaceVolume string) corev1.Container {
	return corev1.Container{
		Name:  GateName,
		Image: image,
		Args:  []string{"wait-worker", "--dir=" + DataPath, "--pod-uid=$(WSNAP_POD_UID)"},
		Env: []corev1.EnvVar{{Name: "WSNAP_POD_UID", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}}},
		VolumeMounts:    []corev1.VolumeMount{{Name: workspaceVolume, MountPath: DataPath, ReadOnly: true}},
		SecurityContext: SecurityContext(),
	}
}
