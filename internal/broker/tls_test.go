package broker

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func writePair(t *testing.T, dir, cn string, mod time.Time) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	kb, _ := x509.MarshalECPrivateKey(key)
	certFile, keyFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	_ = os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	_ = os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600)
	_ = os.Chtimes(certFile, mod, mod)
}

func TestCertReloaderPicksUpRotation(t *testing.T) {
	dir := t.TempDir()
	writePair(t, dir, "first", time.Now().Add(-time.Minute))
	r := &CertReloader{CertFile: filepath.Join(dir, "tls.crt"), KeyFile: filepath.Join(dir, "tls.key")}
	c1, err := r.GetCertificate(nil)
	if err != nil {
		t.Fatal(err)
	}
	writePair(t, dir, "second", time.Now())
	r.checked = time.Time{} // skip the 30s recheck interval
	c2, _ := r.GetCertificate(nil)
	leaf, _ := x509.ParseCertificate(c2.Certificate[0])
	if c1 == c2 || leaf.Subject.CommonName != "second" {
		t.Fatalf("rotated certificate not loaded: %s", leaf.Subject.CommonName)
	}
}

// TestTokenReviewMustConfirmTheAudience (suspicion S4).
func TestTokenReviewMustConfirmTheAudience(t *testing.T) {
	for _, tc := range []struct {
		audiences []string
		ok        bool
	}{{[]string{"wsnap"}, true}, {nil, false}, {[]string{"https://kubernetes.default.svc"}, false}} {
		cs := k8sfake.NewSimpleClientset()
		cs.PrependReactor("create", "tokenreviews", func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, &authnv1.TokenReview{Status: authnv1.TokenReviewStatus{Authenticated: true, Audiences: tc.audiences,
				User: authnv1.UserInfo{Username: "system:serviceaccount:ns:sa", Extra: map[string]authnv1.ExtraValue{
					extraPodName: {"p"}, extraPodUID: {"u"}}}}}, nil
		})
		_, err := TokenReviewAuthenticator{Client: cs, Audience: "wsnap"}.Authenticate(context.Background(), "t")
		if (err == nil) != tc.ok {
			t.Errorf("audiences %v: err = %v, want ok=%v", tc.audiences, err, tc.ok)
		}
	}
}
