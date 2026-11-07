package webhook

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CertSetup keeps the webhook's serving certificate in one Secret, so
// every replica serves the same CA. The first replica to create the Secret
// wins; the others read it. Production clusters may use cert-manager
// instead and skip this.
type CertSetup struct {
	Client      client.Client
	Namespace   string
	SecretName  string
	ServiceName string
	WebhookName string
	CertDir     string
}

// certValidity and renewBefore: a certificate is renewed at startup when it
// has less than renewBefore left.
const (
	certValidity = 365 * 24 * time.Hour
	renewBefore  = 30 * 24 * time.Hour
)

// Ensure writes tls.crt and tls.key to CertDir and the CA into the webhook
// configuration's caBundle. Run it at startup and periodically on every
// replica: the replica that finds the certificate expiring renews it, and
// the others pick it up on their next run. The webhook server reloads the
// files when they change.
func (c CertSetup) Ensure(ctx context.Context) error {
	var secret corev1.Secret
	key := client.ObjectKey{Namespace: c.Namespace, Name: c.SecretName}
	err := c.Client.Get(ctx, key, &secret)
	if apierrors.IsNotFound(err) || (err == nil && expiresSoon(secret.Data["tls.crt"])) {
		data, gerr := c.generate()
		if gerr != nil {
			return gerr
		}
		if apierrors.IsNotFound(err) {
			secret = corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: c.Namespace, Name: c.SecretName}, Data: data}
			if err := c.Client.Create(ctx, &secret); err != nil {
				if !apierrors.IsAlreadyExists(err) {
					return err
				}
				// Another replica won; use its certificate.
				if err := c.Client.Get(ctx, key, &secret); err != nil {
					return err
				}
			}
		} else {
			// Keep the previous CA in the bundle: a replica that has not
			// reloaded yet still serves a certificate signed by it.
			data["ca-prev.crt"] = secret.Data["ca.crt"]
			secret.Data = data
			if err := c.Client.Update(ctx, &secret); err != nil {
				return err // a conflict means another replica renewed; the next run reads it
			}
		}
	} else if err != nil {
		return err
	}
	if err := os.MkdirAll(c.CertDir, 0o700); err != nil {
		return err
	}
	for _, f := range []string{"tls.crt", "tls.key"} {
		if err := os.WriteFile(filepath.Join(c.CertDir, f), secret.Data[f], 0o600); err != nil {
			return err
		}
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cfg admissionv1.MutatingWebhookConfiguration
		if err := c.Client.Get(ctx, client.ObjectKey{Name: c.WebhookName}, &cfg); err != nil {
			return err
		}
		bundle := append(append([]byte{}, secret.Data["ca.crt"]...), secret.Data["ca-prev.crt"]...)
		changed := false
		for i := range cfg.Webhooks {
			if !bytes.Equal(cfg.Webhooks[i].ClientConfig.CABundle, bundle) {
				cfg.Webhooks[i].ClientConfig.CABundle = bundle
				changed = true
			}
		}
		if !changed {
			return nil
		}
		return c.Client.Update(ctx, &cfg)
	})
}

func expiresSoon(certPEM []byte) bool {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return true
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	return err != nil || time.Until(cert.NotAfter) < renewBefore
}

func (c CertSetup) generate() (map[string][]byte, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()), Subject: pkix.Name{CommonName: "wsnap-webhook-ca"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(certValidity),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	caCert, _ := x509.ParseCertificate(caDER)
	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	host := fmt.Sprintf("%s.%s.svc", c.ServiceName, c.Namespace)
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano() + 1), Subject: pkix.Name{CommonName: host},
		DNSNames:  []string{host, host + ".cluster.local"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(certValidity),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	srvKeyDER, err := x509.MarshalECPrivateKey(srvKey)
	if err != nil {
		return nil, err
	}
	return map[string][]byte{
		"ca.crt":  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		"tls.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER}),
		"tls.key": pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: srvKeyDER}),
	}, nil
}
