package agentpod

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestTrust(t *testing.T) {
	bundle := func() []byte { return []byte("ca") }
	var c corev1.Container
	Trust{ConfigMap: "wsnap-ca", Bundle: bundle}.Apply(&c)
	if len(c.Env) != 0 || len(c.VolumeMounts) != 1 || c.Args[0] != "--ca-file="+CAPath+"/ca.crt" {
		t.Fatalf("an operator's ConfigMap must win over the managed CA: %+v", c)
	}
	c = corev1.Container{}
	Trust{Bundle: bundle}.Apply(&c)
	if len(c.Env) != 1 || c.Env[0].Name != EnvCAPEM || c.Env[0].Value != "ca" || len(c.VolumeMounts) != 0 {
		t.Fatalf("managed CA not passed inline: %+v", c)
	}
	if !HasCA(&c) {
		t.Error("HasCA missed WSNAP_CA_PEM")
	}
	c = corev1.Container{}
	Trust{}.Apply(&c)
	if HasCA(&c) {
		t.Errorf("plain-HTTP broker got a CA: %+v", c)
	}
}

func TestGateHoldsNoCredential(t *testing.T) {
	g := Gate("agent:1", "data")
	if len(g.VolumeMounts) != 1 || !g.VolumeMounts[0].ReadOnly || g.VolumeMounts[0].Name != "data" {
		t.Fatalf("the gate must mount only the workspace, read-only: %+v", g.VolumeMounts)
	}
	if g.RestartPolicy != nil {
		t.Error("the gate must be a plain init container that runs once")
	}
}
