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
	if sharesProcesses(pod) {
		return fmt.Errorf("shareProcessNamespace and hostPID let the workload read the sidecar's environment and files through /proc; turn them off")
	}
	sidecar := SidecarName(pod)
	workspaceVolume := pod.Annotations[wsnapv1.AnnotationVolume]
	if workspaceVolume == "" {
		workspaceVolume = wsnapv1.DefaultVolumeName
	}
	wsnapTokens, apiTokens := tokenVolumes(pod, audience)
	private := map[string]string{} // volume name -> why it is private
	for name := range wsnapTokens {
		private[name] = "the wsnap token"
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
	for _, ec := range pod.Spec.EphemeralContainers {
		if ec.TargetContainerName == sidecar {
			return fmt.Errorf("ephemeral container %q targets %q and would share its process namespace", ec.Name, sidecar)
		}
	}
	return eachWorkloadMount(pod, sidecar, func(kind, name string, m corev1.VolumeMount) error {
		if why := private[m.Name]; why != "" {
			return fmt.Errorf("%s %q mounts volume %q (%s); only the %q container may", kind, name, m.Name, why, sidecar)
		}
		return apiTokenError(kind, name, m, apiTokens, allowAPIToken)
	})
}

// CheckWorkerIsolation applies to a workload Pod whose snapshot agent runs
// in a separate Job. No container in this Pod may receive a wsnap token:
// only the controller-created worker Job may authenticate as an agent.
func CheckWorkerIsolation(pod *corev1.Pod, audience string, allowAPIToken bool) error {
	if sharesProcesses(pod) {
		return fmt.Errorf("shareProcessNamespace and hostPID are not allowed in worker mode")
	}
	wsnapTokens, apiTokens := tokenVolumes(pod, audience)
	if len(wsnapTokens) > 0 {
		return fmt.Errorf("workload Pod may not project a wsnap token in worker mode")
	}
	return eachWorkloadMount(pod, "", func(kind, name string, m corev1.VolumeMount) error {
		return apiTokenError(kind, name, m, apiTokens, allowAPIToken)
	})
}

func sharesProcesses(pod *corev1.Pod) bool {
	return ptr.Deref(pod.Spec.ShareProcessNamespace, false) || pod.Spec.HostPID
}

// tokenVolumes finds the Pod's projected service account tokens: those
// for the broker's audience, and those for the API server (no audience),
// as a copied "kube-api-access" volume would be.
func tokenVolumes(pod *corev1.Pod, audience string) (wsnapTokens, apiTokens map[string]bool) {
	wsnapTokens, apiTokens = map[string]bool{}, map[string]bool{}
	for _, v := range pod.Spec.Volumes {
		if v.Projected == nil {
			continue
		}
		for _, src := range v.Projected.Sources {
			if t := src.ServiceAccountToken; t != nil {
				switch t.Audience {
				case audience:
					wsnapTokens[v.Name] = true
				case "":
					apiTokens[v.Name] = true
				}
			}
		}
	}
	return wsnapTokens, apiTokens
}

func apiTokenError(kind, name string, m corev1.VolumeMount, apiTokens map[string]bool, allowAPIToken bool) error {
	if apiTokens[m.Name] && !allowAPIToken {
		return fmt.Errorf("%s %q mounts volume %q, an API server token; untrusted containers must not hold one", kind, name, m.Name)
	}
	return nil
}

// eachWorkloadMount calls fn for every volume mount of every container
// (init, regular and ephemeral) except the agent container named skip.
func eachWorkloadMount(pod *corev1.Pod, skip string, fn func(kind, name string, m corev1.VolumeMount) error) error {
	for _, list := range [][]corev1.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
		for _, c := range list {
			if c.Name == skip {
				continue
			}
			for _, m := range c.VolumeMounts {
				if err := fn("container", c.Name, m); err != nil {
					return err
				}
			}
		}
	}
	for _, ec := range pod.Spec.EphemeralContainers {
		for _, m := range ec.VolumeMounts {
			if err := fn("ephemeral container", ec.Name, m); err != nil {
				return err
			}
		}
	}
	return nil
}
