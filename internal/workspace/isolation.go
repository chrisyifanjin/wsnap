package workspace

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
)

// CheckIsolation refuses a workspace Pod whose workload containers could
// reach the agent's secrets or the Kubernetes API. The webhook (at
// admission) and the broker (at every request) apply the same rules:
//
//   - no shared process namespace (it exposes the sidecar's /proc);
//   - no ephemeral container that targets the sidecar;
//   - no workload container (init, regular or ephemeral) that mounts a
//     volume the sidecar mounts, except the workspace volume and ConfigMap
//     volumes: the wsnap token, and the sidecar's state directory, which
//     holds the engine's configuration with storage credentials;
//   - unless allowAPIToken, no workload container that mounts a projected
//     service account token for the API server (no audience), as a copied
//     "kube-api-access" volume would.
func CheckIsolation(pod *corev1.Pod, audience string, allowAPIToken bool) error {
	if ptr.Deref(pod.Spec.ShareProcessNamespace, false) || pod.Spec.HostPID {
		return fmt.Errorf("shareProcessNamespace and hostPID let the workload read the sidecar's environment and files through /proc; turn them off")
	}
	sidecar := SidecarName(pod)
	workspaceVolume := pod.Annotations[wsnapv1.AnnotationVolume]
	if workspaceVolume == "" {
		workspaceVolume = wsnapv1.DefaultVolumeName
	}
	private := map[string]string{} // volume name -> why it is private
	apiToken := map[string]bool{}
	for _, v := range pod.Spec.Volumes {
		if v.Projected == nil {
			continue
		}
		for _, src := range v.Projected.Sources {
			if t := src.ServiceAccountToken; t != nil {
				switch t.Audience {
				case audience:
					private[v.Name] = "the wsnap token"
				case "":
					apiToken[v.Name] = true
				}
			}
		}
	}
	configMaps := map[string]bool{}
	for _, v := range pod.Spec.Volumes {
		if v.ConfigMap != nil {
			configMaps[v.Name] = true
		}
	}
	for _, c := range pod.Spec.InitContainers {
		if c.Name != sidecar {
			continue
		}
		for _, m := range c.VolumeMounts {
			if m.Name != workspaceVolume && !configMaps[m.Name] && private[m.Name] == "" {
				private[m.Name] = "a volume of the " + sidecar + " container"
			}
		}
	}
	check := func(kind, name string, mounts []corev1.VolumeMount) error {
		for _, m := range mounts {
			if why := private[m.Name]; why != "" {
				return fmt.Errorf("%s %q mounts volume %q (%s); only the %q container may", kind, name, m.Name, why, sidecar)
			}
			if apiToken[m.Name] && !allowAPIToken {
				return fmt.Errorf("%s %q mounts volume %q, an API server token; untrusted containers must not hold one", kind, name, m.Name)
			}
		}
		return nil
	}
	for _, list := range [][]corev1.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
		for _, c := range list {
			if c.Name == sidecar {
				continue
			}
			if err := check("container", c.Name, c.VolumeMounts); err != nil {
				return err
			}
		}
	}
	for _, ec := range pod.Spec.EphemeralContainers {
		if ec.TargetContainerName == sidecar {
			return fmt.Errorf("ephemeral container %q targets %q and would share its process namespace", ec.Name, sidecar)
		}
		if err := check("ephemeral container", ec.Name, ec.VolumeMounts); err != nil {
			return err
		}
	}
	return nil
}
