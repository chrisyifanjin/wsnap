package archtest

import (
	"os/exec"
	"strings"
	"testing"
)

const module = "github.com/chrisyifanjin/wsnap/"

// deps lists every package that pkg imports, directly or not.
func deps(t *testing.T, pkg string) []string {
	t.Helper()
	out, err := exec.Command("go", "list", "-deps", module+pkg).Output()
	if err != nil {
		t.Fatalf("go list -deps %s: %v", pkg, err)
	}
	return strings.Fields(string(out))
}

// imports lists the module's own packages that pkg imports directly.
func imports(t *testing.T, pkg string) []string {
	t.Helper()
	out, err := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, module+pkg).Output()
	if err != nil {
		t.Fatalf("go list %s: %v", pkg, err)
	}
	var own []string
	for _, p := range strings.Fields(string(out)) {
		if strings.HasPrefix(p, module) {
			own = append(own, strings.TrimPrefix(p, module))
		}
	}
	return own
}

func TestDependencyRules(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("needs the go command")
	}
	// The data plane runs in agent containers beside untrusted code and
	// must know nothing about the cluster: everything comes from the broker.
	for _, pkg := range []string{"internal/agent", "internal/engine", "internal/protocol", "internal/credentials", "cmd/wsnap-agent"} {
		for _, d := range deps(t, pkg) {
			for _, banned := range []string{"k8s.io/", "sigs.k8s.io/"} {
				if strings.HasPrefix(d, banned) {
					t.Errorf("%s depends on %s; the data plane must not depend on Kubernetes", pkg, d)
				}
			}
		}
	}
	// Importing the API or the agent Pod template must stay cheap.
	for _, pkg := range []string{"api/v1alpha1", "internal/agentpod"} {
		for _, d := range deps(t, pkg) {
			if strings.HasPrefix(d, "sigs.k8s.io/controller-runtime") || strings.HasPrefix(d, "k8s.io/client-go") {
				t.Errorf("%s depends on %s; it must need only API types", pkg, d)
			}
		}
	}
	// The three control-plane components share rules through workspace,
	// agentpod, credentials and protocol, never through each other. Only
	// cmd/ wires them together.
	components := []string{"internal/broker", "internal/controller", "internal/webhook"}
	for _, pkg := range components {
		for _, imp := range imports(t, pkg) {
			for _, other := range components {
				if imp == other {
					t.Errorf("%s imports %s; move the shared rule into workspace, agentpod or protocol", pkg, other)
				}
			}
		}
	}
}
