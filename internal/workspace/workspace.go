// Package workspace holds the logic the broker and the controllers share:
// finding a Pod's workspace and volume, protecting the volume's data, and
// turning a SnapshotRepository into a per-workspace backend.
package workspace

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/credentials"
	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

// Volume kinds recorded in Workspace.status.volume.kind.
const (
	KindPVC       = "persistentVolumeClaim"
	KindEphemeral = "ephemeral"
	KindEmptyDir  = "emptyDir"
)

// Identity is the workspace a Pod belongs to.
type Identity struct {
	Namespace  string
	Name       string
	Repository string
}

// FromPod reads the workspace labels of a workload Pod.
func FromPod(pod *corev1.Pod) (Identity, error) {
	id := Identity{
		Namespace:  pod.Namespace,
		Name:       pod.Labels[wsnapv1.LabelWorkspace],
		Repository: pod.Labels[wsnapv1.LabelRepository],
	}
	if id.Name == "" {
		return id, fmt.Errorf("pod %s/%s has no %s label", pod.Namespace, pod.Name, wsnapv1.LabelWorkspace)
	}
	if errs := validation.IsDNS1123Label(id.Name); len(errs) > 0 {
		return id, fmt.Errorf("label %s=%q: %s", wsnapv1.LabelWorkspace, id.Name, strings.Join(errs, "; "))
	}
	if id.Repository == "" {
		return id, fmt.Errorf("pod %s/%s has no %s label", pod.Namespace, pod.Name, wsnapv1.LabelRepository)
	}
	return id, nil
}

// SidecarName returns the name of the container that runs the agent.
func SidecarName(pod *corev1.Pod) string {
	if n := pod.Annotations[wsnapv1.AnnotationSidecar]; n != "" {
		return n
	}
	return wsnapv1.DefaultSidecarName
}

// ResolveVolume finds the volume that holds the workspace data. For
// PVC-backed volumes it reads the claim to learn the bound PV, if any.
func ResolveVolume(ctx context.Context, r client.Reader, pod *corev1.Pod) (wsnapv1.VolumeRef, error) {
	name := pod.Annotations[wsnapv1.AnnotationVolume]
	if name == "" {
		name = wsnapv1.DefaultVolumeName
	}
	var vol *corev1.Volume
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].Name == name {
			vol = &pod.Spec.Volumes[i]
		}
	}
	if vol == nil {
		return wsnapv1.VolumeRef{}, fmt.Errorf("%w: pod %s/%s has no volume %q (set annotation %s)", ErrBadVolume, pod.Namespace, pod.Name, name, wsnapv1.AnnotationVolume)
	}
	var ref wsnapv1.VolumeRef
	switch {
	case vol.PersistentVolumeClaim != nil:
		ref = wsnapv1.VolumeRef{Kind: KindPVC, ClaimName: vol.PersistentVolumeClaim.ClaimName}
	case vol.Ephemeral != nil:
		// Kubernetes names generic ephemeral volume claims <pod>-<volume>.
		ref = wsnapv1.VolumeRef{Kind: KindEphemeral, ClaimName: pod.Name + "-" + vol.Name}
	case vol.EmptyDir != nil:
		return wsnapv1.VolumeRef{Kind: KindEmptyDir}, nil
	default:
		return wsnapv1.VolumeRef{}, fmt.Errorf("%w: volume %q must be a persistentVolumeClaim, ephemeral or emptyDir volume", ErrBadVolume, name)
	}
	var pvc corev1.PersistentVolumeClaim
	err := r.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: ref.ClaimName}, &pvc)
	if apierrors.IsNotFound(err) {
		return ref, nil
	}
	if err != nil {
		return ref, err
	}
	ref.VolumeName = pvc.Spec.VolumeName
	return ref, nil
}

// Protect switches the PV behind ref to the Retain reclaim policy, so its
// data survives the deletion of the claim until wsnap has a final snapshot.
// It records the original policy and the workspace on the PV. Protect is
// idempotent.
func Protect(ctx context.Context, c client.Client, r client.Reader, id Identity, ref wsnapv1.VolumeRef) error {
	if ref.VolumeName == "" {
		return nil
	}
	var pv corev1.PersistentVolume
	if err := r.Get(ctx, client.ObjectKey{Name: ref.VolumeName}, &pv); err != nil {
		return client.IgnoreNotFound(err)
	}
	wsRef := id.Namespace + "/" + id.Name
	if pv.Labels[wsnapv1.LabelProtected] == "true" && pv.Annotations[wsnapv1.AnnotationWorkspaceRef] == wsRef {
		return nil
	}
	patch := client.MergeFrom(pv.DeepCopy())
	if pv.Labels == nil {
		pv.Labels = map[string]string{}
	}
	if pv.Annotations == nil {
		pv.Annotations = map[string]string{}
	}
	pv.Labels[wsnapv1.LabelProtected] = "true"
	pv.Annotations[wsnapv1.AnnotationWorkspaceRef] = wsRef
	if _, ok := pv.Annotations[wsnapv1.AnnotationOriginalReclaimPolicy]; !ok {
		pv.Annotations[wsnapv1.AnnotationOriginalReclaimPolicy] = string(pv.Spec.PersistentVolumeReclaimPolicy)
	}
	pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
	return c.Patch(ctx, &pv, patch)
}

// Per-PV bookkeeping. A released PV is safe to reclaim only when the last
// Pod that wrote to it also took a final snapshot of it.
const (
	AnnotationWriterPod = "wsnap.dev/writer-pod-uid"
	AnnotationFinalPod  = "wsnap.dev/final-pod-uid"
	// AnnotationWriterEpoch is the Workspace writerEpoch of the last Pod
	// that the broker admitted as writer on this volume.
	AnnotationWriterEpoch = "wsnap.dev/writer-epoch"
)

// ErrBadVolume marks a Pod whose workspace volume wsnap cannot use, as
// opposed to an API error while resolving it.
var ErrBadVolume = errors.New("unusable workspace volume")

// MarkPV sets one bookkeeping annotation on the PV.
func MarkPV(ctx context.Context, c client.Client, r client.Reader, volumeName, key, value string) error {
	return MarkPVAll(ctx, c, r, volumeName, map[string]string{key: value})
}

// MarkPVAll sets several bookkeeping annotations on the PV in one patch.
func MarkPVAll(ctx context.Context, c client.Client, r client.Reader, volumeName string, marks map[string]string) error {
	if volumeName == "" {
		return nil
	}
	var pv corev1.PersistentVolume
	if err := r.Get(ctx, client.ObjectKey{Name: volumeName}, &pv); err != nil {
		return client.IgnoreNotFound(err)
	}
	patch := client.MergeFrom(pv.DeepCopy())
	changed := false
	for k, v := range marks {
		if pv.Annotations[k] != v {
			if pv.Annotations == nil {
				pv.Annotations = map[string]string{}
			}
			pv.Annotations[k], changed = v, true
		}
	}
	if !changed {
		return nil
	}
	return c.Patch(ctx, &pv, patch)
}

// Epoch parses a writer epoch annotation; a missing or invalid one is 0.
func Epoch(annotations map[string]string) int64 {
	n, err := strconv.ParseInt(annotations[AnnotationWriterEpoch], 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// Backend resolves the repository backend for one workspace. Every
// workspace gets its own path (or Azure container), which is what lets a
// credential provider scope a credential to one workspace.
func Backend(repo *wsnapv1.SnapshotRepository, namespace, name string) (protocol.Backend, error) {
	b := repo.Spec.Backend
	wsPath := func(prefix string) string { return strings.Trim(path.Join(prefix, namespace, name), "/") }
	switch {
	case b.S3 != nil:
		endpoint := b.S3.Endpoint
		if endpoint == "" {
			endpoint = "s3.amazonaws.com"
		}
		return protocol.Backend{S3: &protocol.S3Backend{
			Endpoint: endpoint, Bucket: b.S3.Bucket, Prefix: wsPath(b.S3.Prefix), Region: b.S3.Region, Insecure: b.S3.Insecure,
		}}, nil
	case b.GCS != nil:
		return protocol.Backend{GCS: &protocol.GCSBackend{Bucket: b.GCS.Bucket, Prefix: wsPath(b.GCS.Prefix)}}, nil
	case b.Azure != nil:
		az := &protocol.AzureBackend{Account: b.Azure.Account, Container: b.Azure.Container, EndpointSuffix: b.Azure.EndpointSuffix, AccessTier: b.Azure.AccessTier}
		if repo.Spec.Credentials.Provider == string(credentials.ProviderAzureSAS) {
			az.Container = credentials.AzureWorkspaceContainer(b.Azure.Container, namespace, name)
			az.Prefix = strings.Trim(b.Azure.Prefix, "/")
		} else {
			az.Prefix = wsPath(b.Azure.Prefix)
		}
		return protocol.Backend{Azure: az}, nil
	case b.Filesystem != nil:
		return protocol.Backend{Filesystem: &protocol.FilesystemBackend{Path: "/" + wsPath(b.Filesystem.Path)}}, nil
	}
	return protocol.Backend{}, fmt.Errorf("snapshot repository %s/%s has no backend", repo.Namespace, repo.Name)
}

// Provider returns the credential provider of a repository.
func Provider(repo *wsnapv1.SnapshotRepository) (credentials.Provider, error) {
	spec := repo.Spec.Credentials
	switch credentials.ProviderType(spec.Provider) {
	case "", credentials.ProviderStatic:
		return credentials.Static{}, nil
	case credentials.ProviderS3STS:
		p := credentials.S3STS{}
		if spec.STS != nil {
			p.Endpoint, p.RoleARN, p.Region = spec.STS.Endpoint, spec.STS.RoleARN, spec.STS.Region
		}
		if p.Endpoint == "" && repo.Spec.Backend.S3 != nil && repo.Spec.Backend.S3.Endpoint != "" &&
			repo.Spec.Backend.S3.Endpoint != "s3.amazonaws.com" {
			// S3-compatible stores serve STS on the S3 endpoint.
			scheme := "https://"
			if repo.Spec.Backend.S3.Insecure {
				scheme = "http://"
			}
			p.Endpoint = scheme + repo.Spec.Backend.S3.Endpoint
		}
		return p, nil
	case credentials.ProviderGCSDownscoped:
		return credentials.GCSDownscoped{}, nil
	case credentials.ProviderAzureSAS:
		p := credentials.AzureSAS{}
		if spec.AzureSAS != nil {
			p.ServiceURL = spec.AzureSAS.ServiceURL
		}
		return p, nil
	}
	return nil, fmt.Errorf("unknown credential provider %q", spec.Provider)
}

// Engine returns the protocol engine of a repository.
func Engine(repo *wsnapv1.SnapshotRepository) protocol.Engine {
	return protocol.Engine{Type: protocol.EngineType(repo.Spec.Engine.Type), Command: repo.Spec.Engine.Command}
}

// Interval returns the snapshot interval with its default.
func Interval(repo *wsnapv1.SnapshotRepository) time.Duration {
	if d := repo.Spec.Schedule.Interval.Duration; d > 0 {
		return d
	}
	return 15 * time.Minute
}

// KeepLast returns the retention with its default.
func KeepLast(repo *wsnapv1.SnapshotRepository) int {
	if n := repo.Spec.Retention.KeepLast; n > 0 {
		return int(n)
	}
	return 10
}

// KeepDaily returns retention.keepDaily with its default (7).
func KeepDaily(repo *wsnapv1.SnapshotRepository) int {
	if n := repo.Spec.Retention.KeepDaily; n != nil {
		return int(*n)
	}
	return 7
}

// TTL returns the credential lifetime with its default.
func TTL(repo *wsnapv1.SnapshotRepository) time.Duration {
	if d := repo.Spec.Credentials.TTL.Duration; d > 0 {
		return d
	}
	return time.Hour
}
