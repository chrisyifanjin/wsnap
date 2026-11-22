package controller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
)

// ReleaseResult lists what ReleaseProtectedVolumes did.
type ReleaseResult struct {
	Released []string
	// Skipped holds PVs whose data is not in a snapshot yet, with the
	// reason. Releasing them could delete the only copy.
	Skipped map[string]string
}

// ReleaseProtectedVolumes restores the original reclaim policy of every PV
// wsnap protected, for uninstalling wsnap. A Released PV that still awaits
// its rescue snapshot is skipped unless force is set, because releasing it
// lets Kubernetes delete data that exists nowhere else.
func ReleaseProtectedVolumes(ctx context.Context, c client.Client, force bool) (ReleaseResult, error) {
	res := ReleaseResult{Skipped: map[string]string{}}
	var pvs corev1.PersistentVolumeList
	if err := c.List(ctx, &pvs, client.MatchingLabels{wsnapv1.LabelProtected: "true"}); err != nil {
		return res, err
	}
	for i := range pvs.Items {
		pv := &pvs.Items[i]
		a := pv.Annotations
		hasFinal := workspace.HasFinalSnapshot(a)
		rescue := a[wsnapv1.AnnotationRescue]
		if !hasFinal && rescue != wsnapv1.RescueDone && !force {
			if pv.Status.Phase == corev1.VolumeReleased {
				res.Skipped[pv.Name] = fmt.Sprintf("released without a final snapshot (rescue %q); its data is in no snapshot", rescue)
			} else {
				// Still in use: once released with the original policy,
				// Kubernetes would delete it before any final snapshot.
				res.Skipped[pv.Name] = "in use and its latest writes are in no final snapshot; stop the workspace first"
			}
			continue
		}
		if !force {
			active, err := activePodOnVolume(ctx, c, pv)
			if err != nil {
				return res, fmt.Errorf("check pods for volume %s: %w", pv.Name, err)
			}
			if active != "" {
				res.Skipped[pv.Name] = fmt.Sprintf("pod %s may still write this volume; stop it before releasing", active)
				continue
			}
		}
		original := corev1.PersistentVolumeReclaimPolicy(a[wsnapv1.AnnotationOriginalReclaimPolicy])
		if original == "" {
			original = corev1.PersistentVolumeReclaimDelete
		}
		// A writer can start after List. Refuse to remove protection if it
		// changes the PV's writer/final markers before this patch reaches the
		// API server; the operator can stop it and rerun the command.
		patch := client.MergeFromWithOptions(pv.DeepCopy(), client.MergeFromWithOptimisticLock{})
		delete(pv.Labels, wsnapv1.LabelProtected)
		pv.Spec.PersistentVolumeReclaimPolicy = original
		if err := c.Patch(ctx, pv, patch); err != nil {
			return res, fmt.Errorf("release %s: %w", pv.Name, err)
		}
		res.Released = append(res.Released, pv.Name)
	}
	return res, nil
}

// A final snapshot is proof only after the writer has stopped. A Pod can
// still mount the claim while its sidecar reports that final snapshot.
func activePodOnVolume(ctx context.Context, c client.Client, pv *corev1.PersistentVolume) (string, error) {
	claim := pv.Spec.ClaimRef
	if claim == nil || claim.Namespace == "" || claim.Name == "" {
		return "", fmt.Errorf("volume has no claim namespace and name")
	}
	ns, name, ok := strings.Cut(pv.Annotations[wsnapv1.AnnotationWorkspaceRef], "/")
	if !ok || ns != claim.Namespace || len(validation.IsDNS1123Label(name)) > 0 {
		return "", fmt.Errorf("volume's workspace reference does not match its claim namespace")
	}
	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(claim.Namespace)); err != nil {
		return "", err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if string(pod.UID) == pv.Annotations[workspace.AnnotationWriterPod] && pod.UID != "" {
			return pod.Namespace + "/" + pod.Name, nil
		}
		if workspace.MountsClaim(pod, claim.Name) {
			return pod.Namespace + "/" + pod.Name, nil
		}
	}
	return "", nil
}
