package engine

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

// Kopia's built-in policy must not prune a snapshot before wsnap decides
// whether a restore or fork still needs it.
func TestKopiaKeepsSnapshotsUntilWsnapMaintenance(t *testing.T) {
	if _, err := exec.LookPath("kopia"); err != nil {
		t.Skip(err)
	}
	ctx := context.Background()
	k := &Kopia{Binary: "kopia"}
	tg := Target{Backend: protocol.Backend{Filesystem: &protocol.FilesystemBackend{Path: filepath.Join(t.TempDir(), "repo")}},
		Password: "pw", Workspace: "ws", StateDir: t.TempDir()}
	if err := k.Init(ctx, tg); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := k.Init(ctx, tg); err != nil {
			t.Fatal(err)
		}
	}
	policy, err := k.kopia(ctx, tg, "", "policy", "show", "--global", "--json")
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(policy), `"/.wsnap"`); n != 1 {
		t.Fatalf("expected one .wsnap ignore rule after repeated Init, got %d: %s", n, policy)
	}
	dir := t.TempDir()
	var ids []string
	for i := 0; i < 14; i++ {
		os.WriteFile(filepath.Join(dir, "f"), []byte(fmt.Sprint(i)), 0o644)
		s, err := k.Snapshot(ctx, tg, dir)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, s.ID)
	}
	list, err := k.list(ctx, tg)
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, s := range list {
		have[s.ID] = true
	}
	if len(list) != len(ids) {
		t.Fatalf("kopia deleted %d snapshots before wsnap maintenance; first present=%v", len(ids)-len(list), have[ids[0]])
	}
	if err := k.Maintain(ctx, tg, Retention{KeepLast: 1, Keep: []string{ids[0]}}); err != nil {
		t.Fatal(err)
	}
	list, err = k.list(ctx, tg)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].ID != ids[0] || list[1].ID != ids[len(ids)-1] {
		t.Errorf("after maintenance got %v, want first and last snapshot", list)
	}
}

// A crash between repository creation and policy setup must be repaired by
// the next writer, before any workspace data is captured.
func TestKopiaRepairsInterruptedInitialization(t *testing.T) {
	if _, err := exec.LookPath("kopia"); err != nil {
		t.Skip(err)
	}
	ctx := context.Background()
	k := &Kopia{Binary: "kopia"}
	tg := Target{Backend: protocol.Backend{Filesystem: &protocol.FilesystemBackend{Path: filepath.Join(t.TempDir(), "repo")}},
		Password: "pw", Workspace: "ws", StateDir: t.TempDir()}
	os.MkdirAll(tg.Backend.Filesystem.Path, 0o700)
	storage, _ := k.storageArgs(tg)
	// Simulate: create succeeded, the process died before policy set.
	if _, err := k.kopia(ctx, tg, "", append(append([]string{"repository", "create"}, storage...),
		"--override-username=wsnap", "--override-hostname=ws", "--no-persist-credentials")...); err != nil {
		t.Fatal(err)
	}
	if err := k.Init(ctx, tg); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, ".wsnap"), 0o755)
	os.WriteFile(filepath.Join(dir, ".wsnap", "initialized"), []byte("x"), 0o644)
	os.WriteFile(filepath.Join(dir, "work"), []byte("x"), 0o644)
	s, err := k.Snapshot(ctx, tg, dir)
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	if err := k.Restore(ctx, tg, s.ID, out); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(out, ".wsnap", "initialized")); err == nil {
		t.Errorf(".wsnap is in the snapshot: the ignore policy was never applied")
	}
	pol, err := k.kopia(ctx, tg, "", "maintenance", "info", "--json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(pol), `"owner":"wsnap-maintenance@ws"`) {
		t.Errorf("maintenance owner was not repaired: %s", pol)
	}
}

// TestKopiaKeepsSnapshotsFromBeforePins (reviewer #5 finding 3): snapshots
// taken before wsnap pinned them must survive Kopia's own retention after
// an upgrade.
func TestKopiaKeepsSnapshotsFromBeforePins(t *testing.T) {
	if _, err := exec.LookPath("kopia"); err != nil {
		t.Skip(err)
	}
	ctx := context.Background()
	k := &Kopia{Binary: "kopia"}
	tg := Target{Backend: protocol.Backend{Filesystem: &protocol.FilesystemBackend{Path: filepath.Join(t.TempDir(), "repo")}},
		Password: "pw", Workspace: "ws", StateDir: t.TempDir()}
	if err := k.Init(ctx, tg); err != nil {
		t.Fatal(err)
	}
	// An older wsnap: Kopia's default retention, snapshots without pins.
	if _, err := k.kopia(ctx, tg, "", "policy", "set", "--global", "--keep-latest=10", "--keep-hourly=48", "--keep-daily=7"); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for i := range 8 {
		if err := os.WriteFile(filepath.Join(dir, "f"), []byte(fmt.Sprint("old", i)), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := k.kopia(ctx, tg, dir, "snapshot", "create", dir); err != nil {
			t.Fatal(err)
		}
	}
	// The upgraded agent.
	for i := range 10 {
		if err := k.Init(ctx, tg); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "f"), []byte(fmt.Sprint("new", i)), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := k.Snapshot(ctx, tg, dir); err != nil {
			t.Fatal(err)
		}
	}
	list, err := k.list(ctx, tg)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 18 {
		t.Fatalf("%d snapshots left of 18; Kopia's retention deleted pre-upgrade snapshots", len(list))
	}
}
