package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func managedCerts(c client.Client) *ManagedCerts {
	return &ManagedCerts{Client: c, Namespace: "wsnap-system", SecretName: "wsnap-broker-cert",
		DNSNames: []string{"wsnap-broker.wsnap-system.svc", "wsnap-broker.wsnap-system.svc.cluster.local"}}
}

func certClient() client.Client {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	return fake.NewClientBuilder().WithScheme(scheme).Build()
}

// handshake checks that a client trusting only bundle accepts m's
// certificate for the broker's Service name.
func handshake(t *testing.T, m *ManagedCerts, bundle []byte) error {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	srv.TLS = &tls.Config{GetCertificate: m.GetCertificate}
	srv.StartTLS()
	defer srv.Close()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(bundle) {
		t.Fatal("empty CA bundle")
	}
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "wsnap-broker.wsnap-system.svc"}}}
	resp, err := c.Get(srv.URL)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

func TestReplicasShareTheBrokerCertificate(t *testing.T) {
	ctx := context.Background()
	c := certClient()
	a, b := managedCerts(c), managedCerts(c)
	if err := a.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if string(a.CABundle()) != string(b.CABundle()) {
		t.Fatal("replicas serve different CAs")
	}
	if err := handshake(t, b, a.CABundle()); err != nil {
		t.Fatalf("an agent with the CA from replica a cannot reach replica b: %v", err)
	}
}

// Agents keep the CA they got at Pod creation, so renewal must keep it.
func TestRenewalKeepsTheCA(t *testing.T) {
	ctx := context.Background()
	c := certClient()
	m := managedCerts(c)
	if err := m.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	oldCA := m.CABundle()
	var s corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: "wsnap-system", Name: "wsnap-broker-cert"}, &s); err != nil {
		t.Fatal(err)
	}
	oldLeaf := string(s.Data["tls.crt"])
	m.DNSNames = append(m.DNSNames, "wsnap-broker.new-name.svc") // forces a renewal
	if err := m.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	_ = c.Get(ctx, client.ObjectKey{Namespace: "wsnap-system", Name: "wsnap-broker-cert"}, &s)
	if string(s.Data["tls.crt"]) == oldLeaf {
		t.Fatal("certificate not renewed")
	}
	if string(m.CABundle()) != string(oldCA) {
		t.Fatal("renewal replaced the CA that running agents trust")
	}
	if err := handshake(t, m, oldCA); err != nil {
		t.Fatalf("an agent with the old CA cannot reach the renewed broker: %v", err)
	}
	leaf, _ := parseCert(s.Data["tls.crt"])
	if d := time.Until(leaf.NotAfter); d < 300*24*time.Hour {
		t.Errorf("renewed certificate lives only %s", d)
	}
}

func TestUntrustedCAIsRefused(t *testing.T) {
	ctx := context.Background()
	m, other := managedCerts(certClient()), managedCerts(certClient())
	if err := m.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if err := other.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if err := handshake(t, m, other.CABundle()); err == nil {
		t.Fatal("a client trusting another CA accepted the broker")
	}
}
