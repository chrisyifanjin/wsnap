package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
	"github.com/chrisyifanjin/wsnap/internal/credentials"
)

// RepositoryReconciler validates SnapshotRepositories without touching
// storage, so configuration errors show up in `kubectl get snaprepo`
// instead of in the first failed snapshot.
type RepositoryReconciler struct {
	client.Client
	Reader client.Reader
}

// ValidateRepository returns the reason a repository cannot work, or nil.
func ValidateRepository(repo *wsnapv1.SnapshotRepository, secret *corev1.Secret) error {
	spec := repo.Spec
	b := spec.Backend
	provider := credentials.ProviderType(spec.Credentials.Provider)
	if provider == "" {
		provider = credentials.ProviderStatic
	}
	switch provider {
	case credentials.ProviderS3STS:
		if b.S3 == nil {
			return fmt.Errorf("provider s3-sts needs an s3 backend")
		}
	case credentials.ProviderGCSDownscoped:
		if b.GCS == nil {
			return fmt.Errorf("provider gcs-downscoped needs a gcs backend")
		}
		if spec.Engine.Type == "kopia" {
			return fmt.Errorf("kopia cannot use gcs-downscoped tokens; use restic, the static provider, or GCS through its S3 API")
		}
	case credentials.ProviderAzureSAS:
		if b.Azure == nil {
			return fmt.Errorf("provider azure-sas needs an azure backend")
		}
		if spec.Engine.Type == "restic" {
			// Verified against Azurite: restic init calls Get Container
			// Properties, which a container-scoped service SAS cannot do.
			return fmt.Errorf("restic cannot initialize a repository with a container SAS; use kopia with azure-sas, or restic with the static provider")
		}
	}
	if spec.Engine.Type == "exec" && len(spec.Engine.Command) == 0 {
		return fmt.Errorf("engine exec needs a command")
	}
	if secret == nil {
		return fmt.Errorf("secret %q not found", spec.Credentials.SecretName)
	}
	if len(secret.Data[credentials.KeyMasterKey]) < 16 {
		return fmt.Errorf("secret %q: %s must be at least 16 bytes", secret.Name, credentials.KeyMasterKey)
	}
	var need []string
	switch {
	case b.S3 != nil:
		need = []string{credentials.KeyAWSAccessKeyID, credentials.KeyAWSSecretAccessKey}
	case b.GCS != nil:
		need = []string{credentials.KeyGoogleCredentials}
	case b.Azure != nil:
		need = []string{credentials.KeyAzureAccountKey}
	}
	for _, k := range need {
		if len(secret.Data[k]) == 0 {
			return fmt.Errorf("secret %q has no key %s", secret.Name, k)
		}
	}
	return nil
}

func (r *RepositoryReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var repo wsnapv1.SnapshotRepository
	if err := r.Get(ctx, req.NamespacedName, &repo); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	var secret *corev1.Secret
	var s corev1.Secret
	err := r.Reader.Get(ctx, types.NamespacedName{Namespace: repo.Namespace, Name: repo.Spec.Credentials.SecretName}, &s)
	switch {
	case err == nil:
		secret = &s
	case !apierrors.IsNotFound(err):
		return ctrl.Result{}, err
	}
	ready := metav1.Condition{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Valid", Message: "configuration is valid", ObservedGeneration: repo.Generation}
	if verr := ValidateRepository(&repo, secret); verr != nil {
		ready.Status, ready.Reason, ready.Message = metav1.ConditionFalse, "Invalid", verr.Error()
	}
	scoped := metav1.Condition{Type: "CredentialsScoped", Status: metav1.ConditionTrue, Reason: "ScopedProvider",
		Message: "agents get per-workspace, per-role credentials", ObservedGeneration: repo.Generation}
	if p := repo.Spec.Credentials.Provider; p == "" || p == string(credentials.ProviderStatic) {
		scoped.Status, scoped.Reason, scoped.Message = metav1.ConditionFalse, "StaticProvider", "every agent holds the admin credential"
	}
	before := repo.Status.DeepCopy()
	meta.SetStatusCondition(&repo.Status.Conditions, ready)
	meta.SetStatusCondition(&repo.Status.Conditions, scoped)
	repo.Status.ObservedGeneration = repo.Generation
	// SetStatusCondition keeps lastTransitionTime when the status is
	// unchanged, so an unchanged repository compares equal.
	if equality.Semantic.DeepEqual(before, &repo.Status) {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, r.Status().Update(ctx, &repo)
}

// SetupWithManager registers the reconciler. Secrets are not watched (they
// are not cached), so a fixed Secret is picked up on the next repository
// change or controller restart; the broker reads Secrets live regardless.
func (r *RepositoryReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).Named("snapshotrepository").For(&wsnapv1.SnapshotRepository{}).Complete(r)
}
