package broker

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ManagedCerts keeps the broker's TLS certificate in one Secret, so every
// replica serves the same one. Agents receive the CA when their Pod or Job
// is created and never reload it, so the CA lives ten years and only the
// serving certificate is renewed, signed by the same CA.
type ManagedCerts struct {
	Client     client.Client
	Namespace  string
	SecretName string
	DNSNames   []string

	mu     sync.RWMutex
	cert   *tls.Certificate
	caCert []byte
}

const (
	caValidity    = 10 * 365 * 24 * time.Hour
	leafValidity  = 365 * 24 * time.Hour
	leafRenewal   = 30 * 24 * time.Hour
	ecKeyPEMType  = "EC PRIVATE KEY"
	certPEMType   = "CERTIFICATE"
	secretCA      = "ca.crt"
	secretCAKey   = "ca.key"
	secretTLSCert = "tls.crt"
	secretTLSKey  = "tls.key"
)

// Ensure creates the Secret, or renews its serving certificate when it
// expires within 30 days or lacks a DNS name, and loads it. Run it at
// startup and periodically on every replica: the replica that renews
// wins, and the others load the renewed certificate on their next run.
func (m *ManagedCerts) Ensure(ctx context.Context) error {
	key := client.ObjectKey{Namespace: m.Namespace, Name: m.SecretName}
	var secret corev1.Secret
	err := m.Client.Get(ctx, key, &secret)
	switch {
	case apierrors.IsNotFound(err):
		data, err := newCA()
		if err != nil {
			return err
		}
		if err := m.renewLeaf(data); err != nil {
			return err
		}
		secret = corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: m.Namespace, Name: m.SecretName}, Data: data}
		if err := m.Client.Create(ctx, &secret); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return err
			}
			// Another replica won; use its certificate.
			if err := m.Client.Get(ctx, key, &secret); err != nil {
				return err
			}
		}
	case err != nil:
		return err
	case m.needsRenewal(secret.Data[secretTLSCert]):
		if err := m.renewLeaf(secret.Data); err != nil {
			return err
		}
		// A conflict means another replica renewed; the next run loads it.
		if err := m.Client.Update(ctx, &secret); err != nil {
			return err
		}
	}
	cert, err := tls.X509KeyPair(secret.Data[secretTLSCert], secret.Data[secretTLSKey])
	if err != nil {
		return fmt.Errorf("secret %s/%s: %w", m.Namespace, m.SecretName, err)
	}
	m.mu.Lock()
	m.cert, m.caCert = &cert, secret.Data[secretCA]
	m.mu.Unlock()
	return nil
}

// GetCertificate implements tls.Config.GetCertificate.
func (m *ManagedCerts) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.cert == nil {
		return nil, errors.New("broker certificate not loaded yet")
	}
	return m.cert, nil
}

// CABundle returns the PEM CA that agents must trust.
func (m *ManagedCerts) CABundle() []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.caCert
}

func (m *ManagedCerts) needsRenewal(certPEM []byte) bool {
	cert, err := parseCert(certPEM)
	if err != nil || time.Until(cert.NotAfter) < leafRenewal {
		return true
	}
	for _, name := range m.DNSNames {
		if !slices.Contains(cert.DNSNames, name) {
			return true
		}
	}
	return false
}

func parseCert(certPEM []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil || block.Type != certPEMType {
		return nil, errors.New("no PEM certificate")
	}
	return x509.ParseCertificate(block.Bytes)
}

func newCA() (map[string][]byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()), Subject: pkix.Name{CommonName: "wsnap-broker-ca"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(caValidity),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	return map[string][]byte{
		secretCA:    pem.EncodeToMemory(&pem.Block{Type: certPEMType, Bytes: der}),
		secretCAKey: pem.EncodeToMemory(&pem.Block{Type: ecKeyPEMType, Bytes: keyDER}),
	}, nil
}

// renewLeaf writes a new serving certificate into data, signed by the CA
// already in data.
func (m *ManagedCerts) renewLeaf(data map[string][]byte) error {
	ca, err := parseCert(data[secretCA])
	if err != nil {
		return fmt.Errorf("broker CA: %w", err)
	}
	block, _ := pem.Decode(data[secretCAKey])
	if block == nil {
		return errors.New("broker CA key: no PEM block")
	}
	caKey, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("broker CA key: %w", err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	now := time.Now()
	notAfter := now.Add(leafValidity)
	if ca.NotAfter.Before(notAfter) {
		notAfter = ca.NotAfter
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()), Subject: pkix.Name{CommonName: m.DNSNames[0]},
		DNSNames: m.DNSNames, NotBefore: now.Add(-time.Hour), NotAfter: notAfter,
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		return err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	data[secretTLSCert] = pem.EncodeToMemory(&pem.Block{Type: certPEMType, Bytes: der})
	data[secretTLSKey] = pem.EncodeToMemory(&pem.Block{Type: ecKeyPEMType, Bytes: keyDER})
	return nil
}
