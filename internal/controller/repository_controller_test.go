package controller

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	wsnapv1 "github.com/chrisyifanjin/wsnap/api/v1alpha1"
)

func TestValidateRepository(t *testing.T) {
	good := map[string][]byte{"WSNAP_MASTER_KEY": []byte("0123456789abcdef"), "AWS_ACCESS_KEY_ID": []byte("a"), "AWS_SECRET_ACCESS_KEY": []byte("b")}
	s3 := wsnapv1.BackendSpec{S3: &wsnapv1.S3Backend{Bucket: "b"}}
	gcs := wsnapv1.BackendSpec{GCS: &wsnapv1.GCSBackend{Bucket: "b"}}
	repo := func(engine, provider string, b wsnapv1.BackendSpec) *wsnapv1.SnapshotRepository {
		return &wsnapv1.SnapshotRepository{ObjectMeta: metav1.ObjectMeta{Name: "r"}, Spec: wsnapv1.SnapshotRepositorySpec{
			Engine: wsnapv1.EngineSpec{Type: engine}, Backend: b,
			Credentials: wsnapv1.CredentialsSpec{SecretName: "s", Provider: provider}}}
	}
	secret := func(data map[string][]byte) *corev1.Secret {
		return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s"}, Data: data}
	}
	cases := []struct {
		name   string
		repo   *wsnapv1.SnapshotRepository
		secret *corev1.Secret
		want   string // substring of the error; "" means valid
	}{
		{"valid", repo("kopia", "s3-sts", s3), secret(good), ""},
		{"missing secret", repo("kopia", "s3-sts", s3), nil, "not found"},
		// A credential must outlive one operation (reviewer #5 finding 4).
		{"ttl shorter than an operation", func() *wsnapv1.SnapshotRepository {
			r := repo("kopia", "s3-sts", s3)
			r.Spec.OperationTimeout = metav1.Duration{Duration: 2 * time.Hour}
			return r
		}(), secret(good), "operationTimeout"},
		{"short master key", repo("kopia", "s3-sts", s3), secret(map[string][]byte{"WSNAP_MASTER_KEY": []byte("short"), "AWS_ACCESS_KEY_ID": []byte("a"), "AWS_SECRET_ACCESS_KEY": []byte("b")}), "at least 16"},
		{"missing backend key", repo("restic", "static", s3), secret(map[string][]byte{"WSNAP_MASTER_KEY": []byte("0123456789abcdef")}), "AWS_ACCESS_KEY_ID"},
		{"sts on gcs", repo("restic", "s3-sts", gcs), secret(good), "needs an s3 backend"},
		{"kopia with downscoped gcs", repo("kopia", "gcs-downscoped", gcs), secret(good), "kopia cannot"},
		{"exec without command", repo("exec", "static", s3), secret(good), "needs a command"},
		{"restic with azure-sas", repo("restic", "azure-sas", wsnapv1.BackendSpec{Azure: &wsnapv1.AzureBackend{Account: "a", Container: "c"}}),
			secret(map[string][]byte{"WSNAP_MASTER_KEY": []byte("0123456789abcdef"), "AZURE_ACCOUNT_KEY": []byte("k")}), "restic cannot initialize"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateRepository(tc.repo, tc.secret)
			if tc.want == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}
