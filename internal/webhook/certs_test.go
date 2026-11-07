package webhook

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func certEnv(t *testing.T) client.Client {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(&admissionv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "wsnap"},
		Webhooks:   []admissionv1.MutatingWebhook{{Name: "pods.wsnap.dev"}},
	}).Build()
}

func setupFor(c client.Client, dir string) CertSetup {
	return CertSetup{Client: c, Namespace: "wsnap-system", SecretName: "wsnap-webhook-cert",
		ServiceName: "wsnap-webhook", WebhookName: "wsnap", CertDir: dir}
}

// TestReplicasShareOneCertificate: two replicas starting together end up
// serving the same certificate, and the webhook trusts its CA.
func TestReplicasShareOneCertificate(t *testing.T) {
	c := certEnv(t)
	d1, d2 := t.TempDir(), t.TempDir()
	if err := setupFor(c, d1).Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := setupFor(c, d2).Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	c1, _ := os.ReadFile(filepath.Join(d1, "tls.crt"))
	c2, _ := os.ReadFile(filepath.Join(d2, "tls.crt"))
	if !bytes.Equal(c1, c2) {
		t.Fatal("replicas serve different certificates; the webhook would trust only one")
	}
	var cfg admissionv1.MutatingWebhookConfiguration
	_ = c.Get(context.Background(), client.ObjectKey{Name: "wsnap"}, &cfg)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(cfg.Webhooks[0].ClientConfig.CABundle)
	block, _ := pem.Decode(c1)
	leaf, _ := x509.ParseCertificate(block.Bytes)
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool, DNSName: "wsnap-webhook.wsnap-system.svc"}); err != nil {
		t.Fatalf("the caBundle does not verify the serving certificate: %v", err)
	}
}

func TestExpiringCertificateIsRenewed(t *testing.T) {
	c := certEnv(t)
	s := setupFor(c, t.TempDir())
	if err := s.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	var secret corev1.Secret
	_ = c.Get(context.Background(), client.ObjectKey{Namespace: "wsnap-system", Name: "wsnap-webhook-cert"}, &secret)
	old := secret.Data["tls.crt"]
	// Replace it with a certificate that expires in a day.
	s2 := s
	data, _ := s2.generate()
	block, _ := pem.Decode(data["tls.crt"])
	cert, _ := x509.ParseCertificate(block.Bytes)
	if time.Until(cert.NotAfter) < renewBefore {
		t.Fatal("fresh certificate already counts as expiring")
	}
	secret.Data["tls.crt"] = []byte("not a certificate")
	_ = c.Update(context.Background(), &secret)
	if err := s.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = c.Get(context.Background(), client.ObjectKey{Namespace: "wsnap-system", Name: "wsnap-webhook-cert"}, &secret)
	if bytes.Equal(secret.Data["tls.crt"], old) || expiresSoon(secret.Data["tls.crt"]) {
		t.Error("an unusable certificate was not replaced")
	}
}

// TestRotationKeepsBothCAs: during a rotation, a replica that still serves
// the old certificate must stay trusted (reviewer finding R7).
func TestRotationKeepsBothCAs(t *testing.T) {
	c := certEnv(t)
	d1, d2 := t.TempDir(), t.TempDir()
	if err := setupFor(c, d1).Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := setupFor(c, d2).Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	oldCert, _ := os.ReadFile(filepath.Join(d2, "tls.crt"))
	// Make the current certificate count as expiring; replica 1 renews.
	var secret corev1.Secret
	_ = c.Get(context.Background(), client.ObjectKey{Namespace: "wsnap-system", Name: "wsnap-webhook-cert"}, &secret)
	secret.Data["tls.crt"] = []byte("expiring")
	_ = c.Update(context.Background(), &secret)
	if err := setupFor(c, d1).Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	var cfg admissionv1.MutatingWebhookConfiguration
	_ = c.Get(context.Background(), client.ObjectKey{Name: "wsnap"}, &cfg)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(cfg.Webhooks[0].ClientConfig.CABundle)
	verify := func(certPEM []byte) error {
		block, _ := pem.Decode(certPEM)
		leaf, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return err
		}
		_, err = leaf.Verify(x509.VerifyOptions{Roots: pool, DNSName: "wsnap-webhook.wsnap-system.svc"})
		return err
	}
	if err := verify(oldCert); err != nil {
		t.Fatalf("replica 2 still serves the old certificate and is no longer trusted: %v", err)
	}
	newCert, _ := os.ReadFile(filepath.Join(d1, "tls.crt"))
	if err := verify(newCert); err != nil {
		t.Fatalf("the renewed certificate is not trusted: %v", err)
	}
	// Replica 2's next periodic run serves the new certificate.
	if err := setupFor(c, d2).Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(d2, "tls.crt")); !bytes.Equal(got, newCert) {
		t.Error("replica 2 did not pick up the renewed certificate")
	}
}

type noWebhookCache struct{ client.Client }

func (c noWebhookCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*admissionv1.MutatingWebhookConfiguration); ok {
		return errors.New("webhook configuration is not in the manager cache")
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func TestPeriodicRenewalBypassesManagerCache(t *testing.T) {
	c := certEnv(t)
	s := setupFor(c, t.TempDir())
	if err := s.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	var secret corev1.Secret
	key := client.ObjectKey{Namespace: s.Namespace, Name: s.SecretName}
	if err := c.Get(context.Background(), key, &secret); err != nil {
		t.Fatal(err)
	}
	secret.Data["tls.crt"] = []byte("expiring")
	if err := c.Update(context.Background(), &secret); err != nil {
		t.Fatal(err)
	}

	// The periodic path uses a cached write client, but live reads must
	// bypass it. A cache miss must not stop certificate renewal.
	s.Client = noWebhookCache{c}
	s.Reader = c
	if err := s.Ensure(context.Background()); err != nil {
		t.Fatalf("renewal depended on the manager cache: %v", err)
	}
	if err := c.Get(context.Background(), key, &secret); err != nil {
		t.Fatal(err)
	}
	if expiresSoon(secret.Data["tls.crt"]) {
		t.Error("certificate was not renewed")
	}
}
