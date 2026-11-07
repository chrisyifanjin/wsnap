package engine_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/chrisyifanjin/wsnap/internal/credentials"
	"github.com/chrisyifanjin/wsnap/internal/engine"
	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

// The S3 cases run only when a test store is configured, e.g. MinIO:
//
//	WSNAP_TEST_S3_ENDPOINT=127.0.0.1:9000 WSNAP_TEST_S3_BUCKET=wsnap \
//	WSNAP_TEST_S3_ACCESS_KEY=broker WSNAP_TEST_S3_SECRET_KEY=... go test ./internal/engine
//
// The access key must be a non-root MinIO user, because MinIO's AssumeRole
// rejects the root user.

func engines(t *testing.T) map[string]protocol.Engine {
	_, file, _, _ := runtime.Caller(0)
	tarEngine := filepath.Join(filepath.Dir(file), "..", "..", "examples", "exec-engine", "tar-engine.sh")
	all := map[string]protocol.Engine{
		"kopia":  {Type: protocol.EngineKopia},
		"restic": {Type: protocol.EngineRestic},
		"exec":   {Type: protocol.EngineExec, Command: []string{tarEngine}},
	}
	for name, spec := range all {
		bin := map[protocol.EngineType]string{protocol.EngineKopia: "kopia", protocol.EngineRestic: "restic"}[spec.Type]
		if bin == "" {
			continue
		}
		if _, err := exec.LookPath(bin); err != nil {
			t.Logf("skipping %s: %v", name, err)
			delete(all, name)
		}
	}
	return all
}

type store struct {
	backend func(workspace string) protocol.Backend
	issue   func(t *testing.T, role protocol.Role, b protocol.Backend, eng protocol.Engine) protocol.Credentials
}

func stores(t *testing.T) map[string]store {
	fsRoot := t.TempDir()
	out := map[string]store{
		"filesystem": {
			backend: func(ws string) protocol.Backend {
				return protocol.Backend{Filesystem: &protocol.FilesystemBackend{Path: filepath.Join(fsRoot, ws)}}
			},
			issue: func(*testing.T, protocol.Role, protocol.Backend, protocol.Engine) protocol.Credentials {
				return protocol.Credentials{}
			},
		},
	}
	// A second S3 implementation with the static provider, e.g. SeaweedFS:
	// WSNAP_TEST_S3_STATIC_ENDPOINT=127.0.0.1:8333 WSNAP_TEST_S3_STATIC_BUCKET=wsnap-sw
	if ep := os.Getenv("WSNAP_TEST_S3_STATIC_ENDPOINT"); ep != "" {
		bucket := os.Getenv("WSNAP_TEST_S3_STATIC_BUCKET")
		run := time.Now().UTC().Format("20060102T150405")
		out["s3-static"] = store{
			backend: func(ws string) protocol.Backend {
				return protocol.Backend{S3: &protocol.S3Backend{Endpoint: ep, Bucket: bucket, Prefix: "engine-test/" + run + "/" + ws, Insecure: true}}
			},
			issue: func(t *testing.T, role protocol.Role, b protocol.Backend, _ protocol.Engine) protocol.Credentials {
				c, err := credentials.Static{}.Issue(context.Background(), credentials.Request{Role: role, Backend: b, Secret: map[string][]byte{
					credentials.KeyAWSAccessKeyID: []byte("any"), credentials.KeyAWSSecretAccessKey: []byte("any")}})
				if err != nil {
					t.Fatal(err)
				}
				return c
			},
		}
	}
	// GCS through fake-gcs-server. STORAGE_EMULATOR_HOST must be set too.
	// restic gets an access token (the gcs-downscoped path), kopia a
	// service account file (the static path; kopia cannot use tokens).
	if bucket := os.Getenv("WSNAP_TEST_GCS_BUCKET"); bucket != "" && os.Getenv("STORAGE_EMULATOR_HOST") != "" {
		saJSON := fakeServiceAccount(t)
		run := time.Now().UTC().Format("20060102T150405")
		out["gcs-emulator"] = store{
			backend: func(ws string) protocol.Backend {
				return protocol.Backend{GCS: &protocol.GCSBackend{Bucket: bucket, Prefix: "engine-test/" + run + "/" + ws}}
			},
			issue: func(t *testing.T, role protocol.Role, b protocol.Backend, eng protocol.Engine) protocol.Credentials {
				if eng.Type == protocol.EngineRestic {
					return protocol.Credentials{GoogleAccessToken: "emulator-ignores-it", Scoped: true}
				}
				return protocol.Credentials{GoogleCredentialsJSON: saJSON}
			},
		}
	}
	// Azure Blob through Azurite over TLS with production-style hostnames,
	// with per-workspace containers and SAS tokens from the azure-sas
	// provider. Run inside Linux (Go on macOS ignores SSL_CERT_FILE):
	//   WSNAP_TEST_AZURE_SUFFIX=localtest.me:10443 SSL_CERT_FILE=... ./engine.test
	if suffix := os.Getenv("WSNAP_TEST_AZURE_SUFFIX"); suffix != "" {
		const account = "devstoreaccount1"
		p := credentials.AzureSAS{ServiceURL: "https://" + account + ".blob." + suffix}
		secret := map[string][]byte{credentials.KeyAzureAccountKey: []byte(os.Getenv("WSNAP_TEST_AZURE_KEY"))}
		run := strings.ToLower(time.Now().UTC().Format("20060102t150405"))
		// With the static provider the admin pre-creates the container, as
		// with an S3 bucket; nothing in wsnap creates it.
		staticContainer := "static-" + run
		if err := createAzureContainer(p.ServiceURL, account, string(secret[credentials.KeyAzureAccountKey]), staticContainer); err != nil {
			t.Fatalf("create container: %v", err)
		}
		out["azure-static"] = store{
			backend: func(ws string) protocol.Backend {
				// Azurite rejects the empty tier header that restic's
				// "account default" sends, so name the tier.
				return protocol.Backend{Azure: &protocol.AzureBackend{Account: account, EndpointSuffix: suffix,
					Container: staticContainer, Prefix: ws, AccessTier: "Hot"}}
			},
			issue: func(t *testing.T, role protocol.Role, b protocol.Backend, _ protocol.Engine) protocol.Credentials {
				return protocol.Credentials{AzureAccountKey: string(secret[credentials.KeyAzureAccountKey])}
			},
		}
		out["azure-sas"] = store{
			backend: func(ws string) protocol.Backend {
				return protocol.Backend{Azure: &protocol.AzureBackend{Account: account, EndpointSuffix: suffix,
					Container: credentials.AzureWorkspaceContainer("e"+run, "test", ws)}}
			},
			issue: func(t *testing.T, role protocol.Role, b protocol.Backend, _ protocol.Engine) protocol.Credentials {
				t.Helper()
				c, err := p.Issue(context.Background(), credentials.Request{Role: role, Namespace: "test", Workspace: "ws", Backend: b, Secret: secret})
				if err != nil {
					t.Fatalf("issue %s SAS: %v", role, err)
				}
				return c
			},
		}
	}
	endpoint := os.Getenv("WSNAP_TEST_S3_ENDPOINT")
	if endpoint == "" {
		return out
	}
	bucket := os.Getenv("WSNAP_TEST_S3_BUCKET")
	secret := map[string][]byte{
		credentials.KeyAWSAccessKeyID:     []byte(os.Getenv("WSNAP_TEST_S3_ACCESS_KEY")),
		credentials.KeyAWSSecretAccessKey: []byte(os.Getenv("WSNAP_TEST_S3_SECRET_KEY")),
	}
	run := time.Now().UTC().Format("20060102T150405")
	out["s3-sts"] = store{
		backend: func(ws string) protocol.Backend {
			return protocol.Backend{S3: &protocol.S3Backend{Endpoint: endpoint, Bucket: bucket, Prefix: "engine-test/" + run + "/" + ws, Insecure: true}}
		},
		issue: func(t *testing.T, role protocol.Role, b protocol.Backend, eng protocol.Engine) protocol.Credentials {
			t.Helper()
			c, err := credentials.S3STS{Endpoint: "http://" + endpoint}.Issue(context.Background(), credentials.Request{
				Role: role, Namespace: "test", Workspace: "ws", Backend: b, Secret: secret,
				WriterDeletePrefixes: engine.WriterDeletePrefixes(eng),
			})
			if err != nil {
				t.Fatalf("issue %s credential: %v", role, err)
			}
			return c
		},
	}
	return out
}

func createAzureContainer(serviceURL, account, key, name string) error {
	cred, err := azblob.NewSharedKeyCredential(account, key)
	if err != nil {
		return err
	}
	c, err := container.NewClientWithSharedKeyCredential(serviceURL+"/"+name, cred, nil)
	if err != nil {
		return err
	}
	_, err = c.Create(context.Background(), nil)
	return err
}

// fakeServiceAccount builds a syntactically valid key file; the emulator
// never checks it.
func fakeServiceAccount(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	b, _ := json.Marshal(map[string]string{
		"type": "service_account", "project_id": "test", "private_key_id": "x", "private_key": string(pemKey),
		"client_email": "t@test.iam.gserviceaccount.com", "client_id": "1", "token_uri": "http://" + os.Getenv("STORAGE_EMULATOR_HOST") + "/token",
	})
	return string(b)
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// TestRoundTrip runs the full workspace life cycle for every engine and
// backend: a writer snapshots twice, a read-only restorer restores the
// latest snapshot into an empty directory, and maintenance applies
// retention.
func TestRoundTrip(t *testing.T) {
	for ename, espec := range engines(t) {
		for sname, st := range stores(t) {
			if espec.Type == protocol.EngineExec && sname != "filesystem" {
				continue // the example tar engine supports filesystem only
			}
			if espec.Type == protocol.EngineRestic && sname == "azure-sas" {
				// restic's init calls Get Container Properties, which a
				// container-scoped service SAS cannot authorize. The
				// repository validator rejects this combination.
				continue
			}
			t.Run(ename+"/"+sname, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
				defer cancel()
				eng, err := engine.New(espec)
				if err != nil {
					t.Fatal(err)
				}
				ws := "ws-" + ename
				backend := st.backend(ws)
				target := func(role protocol.Role) engine.Target {
					return engine.Target{
						Backend: backend, Credentials: st.issue(t, role, backend, espec),
						Password: "test-password", Workspace: ws,
						StateDir: t.TempDir(), ReadOnly: !role.CanWrite(), Maintenance: role == protocol.RoleMaintenance,
					}
				}

				// An empty repository: the restore role must not create it,
				// and the error must say "not found" so the sidecar can tell
				// a new workspace from a broken store.
				if err := eng.Init(ctx, target(protocol.RoleRestore)); !errors.Is(err, engine.ErrRepositoryNotFound) {
					t.Fatalf("read-only Init on a missing repository = %v, want ErrRepositoryNotFound", err)
				}

				src := t.TempDir()
				writeFile(t, filepath.Join(src, "a.txt"), "one")
				writeFile(t, filepath.Join(src, "dir", "b.txt"), "two")
				writeFile(t, filepath.Join(src, engine.StateDirName, "marker"), "agent state")
				writeFile(t, filepath.Join(src, "lost+found", "x"), "fs junk")
				// Only the top-level entries are excluded; nested folders
				// that share the names are workspace data.
				writeFile(t, filepath.Join(src, "dir", "lost+found", "keep.txt"), "nested")
				writeFile(t, filepath.Join(src, "dir", ".wsnap", "keep.txt"), "nested")

				w := target(protocol.RoleSnapshot)
				if err := eng.Init(ctx, w); err != nil {
					t.Fatalf("Init: %v", err)
				}
				first, err := eng.Snapshot(ctx, w, src)
				if err != nil {
					t.Fatalf("first Snapshot: %v", err)
				}
				time.Sleep(1100 * time.Millisecond) // distinct timestamps for the tar engine
				writeFile(t, filepath.Join(src, "a.txt"), "one, edited")
				writeFile(t, filepath.Join(src, "c.txt"), "three")
				w = target(protocol.RoleSnapshot) // fresh credential, as the sidecar does
				if err := eng.Init(ctx, w); err != nil {
					t.Fatalf("second Init: %v", err)
				}
				second, err := eng.Snapshot(ctx, w, src)
				if err != nil {
					t.Fatalf("second Snapshot: %v", err)
				}
				if first.ID == second.ID {
					t.Fatalf("two snapshots share ID %s", first.ID)
				}

				r := target(protocol.RoleRestore)
				if err := eng.Init(ctx, r); err != nil {
					t.Fatalf("read-only Init: %v", err)
				}
				latest, err := eng.Latest(ctx, r)
				if err != nil || latest == nil {
					t.Fatalf("Latest = %v, %v", latest, err)
				}
				if latest.ID != second.ID {
					t.Fatalf("Latest = %s, want %s", latest.ID, second.ID)
				}
				dst := t.TempDir()
				if err := eng.Restore(ctx, r, latest.ID, dst); err != nil {
					t.Fatalf("Restore: %v", err)
				}
				if got := readFile(t, filepath.Join(dst, "a.txt")); got != "one, edited" {
					t.Errorf("a.txt = %q", got)
				}
				if got := readFile(t, filepath.Join(dst, "dir", "b.txt")); got != "two" {
					t.Errorf("dir/b.txt = %q", got)
				}
				if got := readFile(t, filepath.Join(dst, "c.txt")); got != "three" {
					t.Errorf("c.txt = %q", got)
				}
				if espec.Type != protocol.EngineExec {
					for _, nested := range []string{"dir/lost+found/keep.txt", "dir/.wsnap/keep.txt"} {
						if got := readFile(t, filepath.Join(dst, nested)); got != "nested" {
							t.Errorf("%s = %q; a nested folder with an excluded name was dropped", nested, got)
						}
					}
				}
				for _, excluded := range []string{engine.StateDirName, "lost+found"} {
					if _, err := os.Stat(filepath.Join(dst, excluded)); err == nil {
						t.Errorf("%s was restored; it must be excluded from snapshots", excluded)
					}
				}

				if sname == "s3-sts" || sname == "azure-sas" {
					// The restore credential must not be able to write.
					if _, err := eng.Snapshot(ctx, r, src); err == nil {
						t.Errorf("Snapshot with the restore credential succeeded")
					}
				}
				if sname == "s3-sts" {
					// The snapshot credential must not be able to delete.
					if err := eng.Maintain(ctx, target(protocol.RoleSnapshot), 1, nil); err == nil {
						t.Errorf("Maintain with the snapshot credential succeeded")
					}
				}

				m := target(protocol.RoleMaintenance)
				if err := eng.Init(ctx, m); err != nil {
					t.Fatalf("maintenance Init: %v", err)
				}
				time.Sleep(1100 * time.Millisecond)
				writeFile(t, filepath.Join(src, "c.txt"), "four")
				if err := eng.Init(ctx, w); err != nil {
					t.Fatalf("third Init: %v", err)
				}
				third, err := eng.Snapshot(ctx, w, src)
				if err != nil {
					t.Fatalf("third Snapshot: %v", err)
				}
				// keepLast 1 keeps the third; the pinned first survives too
				// (reviewer #3 finding 8); the second goes.
				if err := eng.Maintain(ctx, m, 1, []string{first.ID}); err != nil {
					t.Fatalf("Maintain: %v", err)
				}
				latest, err = eng.Latest(ctx, m)
				if err != nil || latest == nil || latest.ID != third.ID {
					t.Fatalf("after Maintain, Latest = %v, %v; want %s", latest, err, third.ID)
				}
				if err := eng.Restore(ctx, m, first.ID, t.TempDir()); err != nil {
					t.Errorf("the pinned snapshot was deleted: %v", err)
				}
				if err := eng.Restore(ctx, m, second.ID, t.TempDir()); err == nil {
					t.Errorf("the unpinned old snapshot survived maintenance")
				}
			})
		}
	}
}

// TestS3Isolation checks that a credential for one workspace cannot open
// another workspace's repository.
func TestS3Isolation(t *testing.T) {
	st, ok := stores(t)["s3-sts"]
	if !ok {
		t.Skip("WSNAP_TEST_S3_ENDPOINT not set")
	}
	if _, err := exec.LookPath("restic"); err != nil {
		t.Skip(err)
	}
	ctx := context.Background()
	spec := protocol.Engine{Type: protocol.EngineRestic}
	eng, _ := engine.New(spec)
	a, b := st.backend("iso-a"), st.backend("iso-b")
	wa := engine.Target{Backend: a, Credentials: st.issue(t, protocol.RoleSnapshot, a, spec), Password: "p", Workspace: "iso-a", StateDir: t.TempDir()}
	if err := eng.Init(ctx, wa); err != nil {
		t.Fatal(err)
	}
	// Workspace b's credential pointed at workspace a's repository.
	stolen := engine.Target{Backend: a, Credentials: st.issue(t, protocol.RoleMaintenance, b, spec), Password: "p", Workspace: "iso-b", StateDir: t.TempDir()}
	err := eng.Init(ctx, stolen)
	if err == nil {
		t.Fatal("workspace b's credential opened workspace a's repository")
	}
	if !strings.Contains(err.Error(), "Access Denied") {
		t.Fatalf("want Access Denied, got %v", err)
	}
}

// TestUnreadableFileDoesNotFailTheSnapshot: one file the agent cannot read
// must not lose the whole snapshot, above all a final one or a rescue
// (reviewer suspicion S7). restic exits 3; kopia records the error.
func TestUnreadableFileDoesNotFailTheSnapshot(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads every file")
	}
	for _, spec := range []protocol.Engine{{Type: protocol.EngineKopia}, {Type: protocol.EngineRestic}} {
		t.Run(string(spec.Type), func(t *testing.T) {
			if _, err := exec.LookPath(string(spec.Type)); err != nil {
				t.Skip(err)
			}
			eng, _ := engine.New(spec)
			ctx := context.Background()
			tg := engine.Target{Backend: protocol.Backend{Filesystem: &protocol.FilesystemBackend{Path: filepath.Join(t.TempDir(), "repo")}},
				Password: "p", Workspace: "ws", StateDir: t.TempDir()}
			if err := eng.Init(ctx, tg); err != nil {
				t.Fatal(err)
			}
			src := t.TempDir()
			writeFile(t, filepath.Join(src, "ok.txt"), "readable")
			writeFile(t, filepath.Join(src, "secret.txt"), "unreadable")
			if err := os.Chmod(filepath.Join(src, "secret.txt"), 0); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(filepath.Join(src, "secret.txt"), 0o644) })
			snap, err := eng.Snapshot(ctx, tg, src)
			if err != nil {
				t.Fatalf("one unreadable file failed the snapshot: %v", err)
			}
			dst := t.TempDir()
			if err := eng.Restore(ctx, tg, snap.ID, dst); err != nil {
				t.Fatal(err)
			}
			if got := readFile(t, filepath.Join(dst, "ok.txt")); got != "readable" {
				t.Errorf("ok.txt = %q", got)
			}
		})
	}
}

// TestKopiaWriterDoesNotOwnMaintenance: "repository create" makes the
// creator the maintenance owner, and Kopia then runs automatic maintenance
// as the writer, whose credential cannot delete (reviewer #3 suspicion).
func TestKopiaWriterDoesNotOwnMaintenance(t *testing.T) {
	if _, err := exec.LookPath("kopia"); err != nil {
		t.Skip(err)
	}
	ctx := context.Background()
	tgt := engine.Target{Backend: protocol.Backend{Filesystem: &protocol.FilesystemBackend{Path: filepath.Join(t.TempDir(), "repo")}},
		Password: "pw-0123456789", Workspace: "ws1", StateDir: t.TempDir()}
	k := &engine.Kopia{Binary: "kopia"}
	if err := k.Init(ctx, tgt); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("kopia", "maintenance", "info", "--json")
	cmd.Env = append(os.Environ(), "KOPIA_CONFIG_PATH="+filepath.Join(tgt.StateDir, "kopia", "repository.config"), "KOPIA_PASSWORD="+tgt.Password,
		"KOPIA_CACHE_DIRECTORY="+t.TempDir(), "KOPIA_LOG_DIR="+t.TempDir(), "KOPIA_CHECK_FOR_UPDATES=false")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("maintenance info: %v", err)
	}
	if !strings.Contains(string(out), `"owner":"wsnap-maintenance@ws1"`) {
		t.Fatalf("maintenance owner is not the maintenance identity:\n%s", out)
	}
}
