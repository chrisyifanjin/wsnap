package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/chrisyifanjin/wsnap/internal/workspace"
	"slices"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/webhook"
)

// JobConfig holds what every controller-created Job needs.
type JobConfig struct {
	// AgentImage is the default agent image.
	AgentImage string
	// BrokerURL is how agents reach the broker.
	BrokerURL string
	// Audience of the projected service account token.
	Audience string
	// CAConfigMap names a ConfigMap (key ca.crt) in each workload namespace
	// that holds the CA of the broker's TLS certificate. Empty for a plain
	// HTTP broker. trust-manager can keep it in sync across namespaces.
	CAConfigMap string
}

const (
	tokenMountPath = "/var/run/secrets/wsnap"
	dataMountPath  = "/workspace"
	stateMountPath = "/var/lib/wsnap"
	// tokenExpiry is the projected token lifetime. Kubernetes refreshes the
	// file before it expires; 600 seconds is the minimum it accepts.
	tokenExpiry = 600
	// jobDeadline stops a Job that cannot finish, for example a mover whose
	// node-local PV sits on a dead node. The rescue then shows as failed
	// instead of running forever.
	jobDeadline = 2 * 60 * 60
)

// shortHash keeps generated names within the 63-character limit.
func shortHash(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:10]
}

// newAgentJob builds a Job that runs the agent once. kind is mover or
// maintenance.
func (c JobConfig) newAgentJob(kind string, name string, ws *wsnapv1.Workspace, repo *wsnapv1.SnapshotRepository, args []string, extraVolumes []corev1.Volume, extraMounts []corev1.VolumeMount) *batchv1.Job {
	image := repo.Spec.Jobs.Image
	if image == "" {
		image = c.AgentImage
	}
	labels := map[string]string{
		wsnapv1.LabelManagedBy:    wsnapv1.ManagedByValue,
		wsnapv1.LabelJob:          kind,
		wsnapv1.LabelJobWorkspace: ws.Name,
		wsnapv1.LabelRepository:   ws.Spec.RepositoryName,
	}
	volumes := append([]corev1.Volume{
		{Name: "wsnap-token", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
			Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
				Audience: c.Audience, ExpirationSeconds: ptr.To[int64](tokenExpiry), Path: "token",
			}}},
		}}},
		{Name: "wsnap-state", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}, extraVolumes...)
	volumes = append(volumes, repo.Spec.Jobs.Volumes...)
	mounts := append([]corev1.VolumeMount{
		{Name: "wsnap-token", MountPath: tokenMountPath, ReadOnly: true},
		{Name: "wsnap-state", MountPath: stateMountPath},
	}, extraMounts...)
	mounts = append(mounts, repo.Spec.Jobs.VolumeMounts...)
	if c.CAConfigMap != "" {
		volumes = append(volumes, corev1.Volume{Name: "wsnap-ca", VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: c.CAConfigMap}}}})
		mounts = append(mounts, corev1.VolumeMount{Name: "wsnap-ca", MountPath: "/var/run/wsnap-ca", ReadOnly: true})
		args = append(args, "--ca-file=/var/run/wsnap-ca/ca.crt")
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: ws.Namespace, Name: name, Labels: labels},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptr.To[int32](3),
			ActiveDeadlineSeconds:   ptr.To(max(int64(jobDeadline), int64(3*workspace.OperationTimeout(repo)/time.Second))),
			TTLSecondsAfterFinished: ptr.To[int32](3600),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					// The agent authenticates with the projected token only.
					AutomountServiceAccountToken: ptr.To(false),
					SecurityContext:              repo.Spec.Jobs.SecurityContext,
					Volumes:                      volumes,
					Containers: []corev1.Container{{
						Name:            wsnapv1.DefaultSidecarName,
						Image:           image,
						Args:            append(args, "--broker-url="+c.BrokerURL, "--token-file="+tokenMountPath+"/token", "--state-dir="+stateMountPath),
						VolumeMounts:    mounts,
						SecurityContext: webhook.AgentSecurityContext(),
					}},
				},
			},
		},
	}
}

// moverJobName is stable per PV, so a restarted controller finds the Job it
// already created.
func moverJobName(pvName string) string { return "wsnap-mover-" + shortHash(pvName) }

// rescueClaimName is stable per PV for the same reason.
func rescueClaimName(pvName string) string { return "wsnap-rescue-" + shortHash(pvName) }

func maintenanceJobName(ws *wsnapv1.Workspace) string {
	return fmt.Sprintf("wsnap-maint-%s", shortHash(ws.Namespace, ws.Name, ws.Status.LastSnapshot.ID))
}

// maxRecordedJobs bounds Workspace.status.jobs; the oldest entries go first.
const maxRecordedJobs = 20

// createJob creates job and records its UID in the Workspace status, which
// is what the broker checks before it serves the Job.
func createJob(ctx context.Context, c client.Client, r client.Reader, job *batchv1.Job, wsName string) error {
	if err := c.Create(ctx, job); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		var existing batchv1.Job
		if err := r.Get(ctx, client.ObjectKeyFromObject(job), &existing); err != nil {
			return err
		}
		var ws wsnapv1.Workspace
		if err := r.Get(ctx, client.ObjectKey{Namespace: job.Namespace, Name: wsName}, &ws); err != nil {
			return err
		}
		if slices.ContainsFunc(ws.Status.Jobs, func(j wsnapv1.JobRef) bool {
			return workspace.JobRefMatches(j, &existing) && j.ClaimName == workspace.JobRef(job).ClaimName &&
				j.ClaimUID == job.Annotations[workspace.AnnotationClaimUID] &&
				j.VolumeName == job.Annotations[wsnapv1.AnnotationVolumeName] &&
				j.VolumeUID == job.Annotations[workspace.AnnotationPVUID]
		}) {
			*job = existing
			return nil
		}
		// A Job with this name that wsnap never registered: a namespace
		// user may have created it to run their own Pod spec with wsnap's
		// credentials, or wsnap crashed between create and register.
		// Either way it is not trusted: delete it and create a new one on
		// the next reconcile. Foreground, so its Pods are gone first.
		fg := metav1.DeletePropagationForeground
		if err := c.Delete(ctx, &existing, &client.DeleteOptions{PropagationPolicy: &fg,
			Preconditions: &metav1.Preconditions{UID: &existing.UID}}); client.IgnoreNotFound(err) != nil {
			return err
		}
		return fmt.Errorf("job %s/%s existed but was not registered by wsnap; deleted it, will create it again", job.Namespace, job.Name)
	}
	ref := workspace.JobRef(job)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var ws wsnapv1.Workspace
		if err := r.Get(ctx, client.ObjectKey{Namespace: job.Namespace, Name: wsName}, &ws); err != nil {
			return err
		}
		for _, j := range ws.Status.Jobs {
			if j.UID == ref.UID {
				return nil
			}
		}
		ws.Status.Jobs = append(ws.Status.Jobs, ref)
		if len(ws.Status.Jobs) > maxRecordedJobs {
			// Drop entries of Jobs that are gone first: dropping a running
			// Job's entry would make the broker refuse it.
			ws.Status.Jobs = slices.DeleteFunc(ws.Status.Jobs, func(j wsnapv1.JobRef) bool {
				if j.UID == ref.UID {
					return false
				}
				var cur batchv1.Job
				err := r.Get(ctx, client.ObjectKey{Namespace: job.Namespace, Name: j.Name}, &cur)
				return apierrors.IsNotFound(err) || (err == nil && string(cur.UID) != j.UID)
			})
		}
		if n := len(ws.Status.Jobs); n > maxRecordedJobs {
			ws.Status.Jobs = ws.Status.Jobs[n-maxRecordedJobs:]
		}
		return c.Status().Update(ctx, &ws)
	})
}
