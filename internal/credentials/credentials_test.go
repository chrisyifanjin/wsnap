package credentials

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

func TestS3SessionPolicy(t *testing.T) {
	type stmt struct {
		Action    []string
		Resource  []string
		Condition map[string]map[string][]string
	}
	decode := func(t *testing.T, s string) map[string][]string {
		t.Helper()
		var doc struct{ Statement []stmt }
		if err := json.Unmarshal([]byte(s), &doc); err != nil {
			t.Fatal(err)
		}
		actions := map[string][]string{}
		for _, st := range doc.Statement {
			for _, a := range st.Action {
				actions[a] = append(actions[a], st.Resource...)
			}
		}
		return actions
	}
	const objects = "arn:aws:s3:::b/ns/ws/*"

	restore := decode(t, S3SessionPolicy(protocol.RoleRestore, "b", "/ns/ws/", []string{"locks/"}))
	if _, ok := restore["s3:PutObject"]; ok {
		t.Error("restore role can write")
	}
	if _, ok := restore["s3:DeleteObject"]; ok {
		t.Error("restore role can delete")
	}
	if got := restore["s3:GetObject"]; len(got) != 1 || got[0] != objects {
		t.Errorf("restore GetObject = %v", got)
	}

	snap := decode(t, S3SessionPolicy(protocol.RoleSnapshot, "b", "ns/ws", []string{"locks/"}))
	if got := snap["s3:PutObject"]; len(got) != 1 || got[0] != objects {
		t.Errorf("snapshot PutObject = %v", got)
	}
	if got := snap["s3:DeleteObject"]; len(got) != 1 || got[0] != "arn:aws:s3:::b/ns/ws/locks*" {
		t.Errorf("snapshot DeleteObject = %v, want only the locks prefix", got)
	}

	noDel := decode(t, S3SessionPolicy(protocol.RoleMover, "b", "ns/ws", nil))
	if _, ok := noDel["s3:DeleteObject"]; ok {
		t.Error("mover role can delete although the engine needs no writer deletes")
	}

	maint := decode(t, S3SessionPolicy(protocol.RoleMaintenance, "b", "ns/ws", []string{"locks/"}))
	if got := maint["s3:DeleteObject"]; len(got) != 1 || got[0] != objects {
		t.Errorf("maintenance DeleteObject = %v", got)
	}
}

func TestGCSAccessBoundary(t *testing.T) {
	roles := func(role protocol.Role) []string {
		var out []string
		for _, r := range GCSAccessBoundary(role, "b", "ns/ws", []string{"locks/"}) {
			out = append(out, strings.Join(r.AvailablePermissions, ",")+" if "+r.Condition.Expression)
		}
		return out
	}
	if got := roles(protocol.RoleRestore); len(got) != 1 || !strings.HasPrefix(got[0], "inRole:roles/storage.objectViewer if ") ||
		!strings.Contains(got[0], "objectListPrefix', '').startsWith('ns/ws/')") {
		t.Errorf("restore rules = %v", got)
	}
	got := roles(protocol.RoleSnapshot)
	want := []string{
		"inRole:roles/storage.objectCreator if resource.name.startsWith('projects/_/buckets/b/objects/ns/ws/')",
		"inRole:roles/storage.objectUser if resource.name.startsWith('projects/_/buckets/b/objects/ns/ws/locks/')",
	}
	if len(got) != 3 || got[1] != want[0] || got[2] != want[1] {
		t.Errorf("snapshot rules = %v\nwant viewer + %v", got, want)
	}
	if got := roles(protocol.RoleMaintenance); len(got) != 2 ||
		got[1] != "inRole:roles/storage.objectUser if resource.name.startsWith('projects/_/buckets/b/objects/ns/ws/')" {
		t.Errorf("maintenance rules = %v", got)
	}
}

func TestRepositoryPassword(t *testing.T) {
	key := []byte("0123456789abcdef0123")
	a1, _ := RepositoryPassword(key, "ns", "a")
	a2, _ := RepositoryPassword(key, "ns", "a")
	b, _ := RepositoryPassword(key, "ns", "b")
	// "ns" + "a/b" must not collide with "ns/a" + "b".
	c, _ := RepositoryPassword(key, "ns/a", "b")
	d, _ := RepositoryPassword(key, "ns", "a/b")
	if a1 != a2 {
		t.Error("password is not deterministic")
	}
	if a1 == b || c == d {
		t.Error("different workspaces share a password")
	}
	if _, err := RepositoryPassword([]byte("short"), "ns", "a"); err == nil {
		t.Error("short master key accepted")
	}
}

func TestSessionName(t *testing.T) {
	short := sessionName(Request{Role: protocol.RoleSnapshot, Namespace: "ns", Workspace: "ws"})
	if short != "wsnap-snapshot-ns.ws" {
		t.Errorf("short = %q", short)
	}
	long := sessionName(Request{Role: protocol.RoleMaintenance, Namespace: strings.Repeat("n", 63), Workspace: strings.Repeat("w", 63)})
	if len(long) > 64 {
		t.Errorf("session name has %d characters, STS allows 64", len(long))
	}
}

func TestAzureWorkspaceContainer(t *testing.T) {
	for _, base := range []string{"wsnap", "WSNAP-", strings.Repeat("x", 80)} {
		name := AzureWorkspaceContainer(base, "ns", "ws")
		if len(name) < 3 || len(name) > 63 || strings.Contains(name, "--") || strings.ToLower(name) != name {
			t.Errorf("AzureWorkspaceContainer(%q) = %q is not a valid container name", base, name)
		}
	}
	if AzureWorkspaceContainer("w", "ns", "a") == AzureWorkspaceContainer("w", "ns", "b") {
		t.Error("two workspaces share a container")
	}
}

// azuriteKey is the emulator's published development key.
const azuriteKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

// TestAzureSASAgainstAzurite needs the Azurite emulator:
//
//	docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0
//	WSNAP_TEST_AZURITE=http://127.0.0.1:10000/devstoreaccount1 go test ./internal/credentials
func TestAzureSASAgainstAzurite(t *testing.T) {
	serviceURL := os.Getenv("WSNAP_TEST_AZURITE")
	if serviceURL == "" {
		t.Skip("WSNAP_TEST_AZURITE not set")
	}
	ctx := context.Background()
	p := AzureSAS{ServiceURL: serviceURL}
	secret := map[string][]byte{KeyAzureAccountKey: []byte(azuriteKey)}
	backend := func(ws string) protocol.Backend {
		return protocol.Backend{Azure: &protocol.AzureBackend{Account: "devstoreaccount1", Container: AzureWorkspaceContainer("wsnap-test", "ns", ws)}}
	}
	client := func(t *testing.T, role protocol.Role, ws string, containerOf string) *container.Client {
		t.Helper()
		c, err := p.Issue(ctx, Request{Role: role, Namespace: "ns", Workspace: ws, Backend: backend(ws), Secret: secret})
		if err != nil {
			t.Fatalf("Issue: %v", err)
		}
		if !c.Scoped || c.AzureSAS == "" {
			t.Fatalf("credential not scoped: %+v", c)
		}
		u := strings.TrimRight(serviceURL, "/") + "/" + backend(containerOf).Azure.Container + "?" + c.AzureSAS
		cc, err := container.NewClientWithNoCredential(u, nil)
		if err != nil {
			t.Fatal(err)
		}
		return cc
	}
	upload := func(c *container.Client, name string) error {
		_, err := c.NewBlockBlobClient(name).UploadStream(ctx, bytes.NewReader([]byte("data")), nil)
		return err
	}

	writer := client(t, protocol.RoleSnapshot, "a", "a")
	if err := upload(writer, "p/blob1"); err != nil {
		t.Fatalf("snapshot role cannot write: %v", err)
	}
	reader := client(t, protocol.RoleRestore, "a", "a")
	resp, err := reader.NewBlobClient("p/blob1").DownloadStream(ctx, nil)
	if err != nil {
		t.Fatalf("restore role cannot read: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "data" {
		t.Fatalf("read %q", body)
	}
	pager := reader.NewListBlobsFlatPager(nil)
	if _, err := pager.NextPage(ctx); err != nil {
		t.Fatalf("restore role cannot list: %v", err)
	}
	if err := upload(reader, "p/blob2"); err == nil {
		t.Error("restore role can write")
	}
	if _, err := reader.NewBlobClient("p/blob1").Delete(ctx, nil); err == nil {
		t.Error("restore role can delete")
	}

	// Workspace b's credential against workspace a's container.
	client(t, protocol.RoleMaintenance, "b", "b") // creates b's container
	stolen := client(t, protocol.RoleMaintenance, "b", "a")
	if _, err := stolen.NewBlobClient("p/blob1").DownloadStream(ctx, nil); err == nil {
		t.Error("workspace b's SAS can read workspace a's container")
	}
}

func TestS3ListScopeExcludesSiblings(t *testing.T) {
	var doc struct {
		Statement []struct {
			Action    []string
			Condition map[string]map[string][]string
		}
	}
	_ = json.Unmarshal([]byte(S3SessionPolicy(protocol.RoleRestore, "b", "ns/ws", nil)), &doc)
	for _, st := range doc.Statement {
		if st.Action[0] != "s3:ListBucket" {
			continue
		}
		for _, p := range st.Condition["StringLike"]["s3:prefix"] {
			if p == "ns/ws" {
				t.Fatalf("list prefix %q also matches sibling workspaces like ns/ws2/", p)
			}
		}
	}
}
