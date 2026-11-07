package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

// fakeBroker hands out exec-engine credentials for a filesystem repository
// and records reports.
type fakeBroker struct {
	t       *testing.T
	repo    string
	command []string
	fail503 atomic.Int32 // fail this many credential requests with 503
	fail409 atomic.Int32 // refuse this many credential requests with 409
	// denySnapshot refuses writer (snapshot) credentials while set.
	denySnapshot atomic.Bool
	credReqs     atomic.Int32
	mu           sync.Mutex
	reports      []protocol.Report
	// restore overrides for pinned and fork restores
	restoreID, restoreSource string
	latestID                 string // LatestSnapshotID for restores
	restoreRepo              string
	waitQueue                chan string
	failReports              atomic.Bool // answer reports with 503
}

func (b *fakeBroker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v1/credentials":
		b.credReqs.Add(1)
		if b.fail503.Load() > 0 {
			b.fail503.Add(-1)
			http.Error(w, "restarting", http.StatusServiceUnavailable)
			return
		}
		if b.fail409.Load() > 0 {
			b.fail409.Add(-1)
			http.Error(w, "workspace ws is written by pod other; waiting until it stops", http.StatusConflict)
			return
		}
		var req protocol.CredentialsRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Role == protocol.RoleSnapshot && b.denySnapshot.Load() {
			http.Error(w, "another writer still owns the workspace", http.StatusConflict)
			return
		}
		resp := protocol.CredentialsResponse{
			Namespace: "ns", Workspace: "ws", Role: req.Role,
			Engine:             protocol.Engine{Type: protocol.EngineExec, Command: b.command},
			Backend:            protocol.Backend{Filesystem: &protocol.FilesystemBackend{Path: b.repo}},
			RepositoryPassword: "pw",
			Credentials:        protocol.Credentials{Scoped: true, ExpiresAt: time.Now().Add(time.Hour)},
			Interval:           protocol.Duration{Duration: time.Hour},
		}
		if req.Role == protocol.RoleRestore {
			resp.SnapshotID, resp.SourceWorkspace, resp.LatestSnapshotID = b.restoreID, b.restoreSource, b.latestID
			if b.restoreRepo != "" {
				resp.Backend.Filesystem.Path = b.restoreRepo
				resp.RestoreFromGeneration = 7
			}
		}
		_ = json.NewEncoder(w).Encode(resp)
	case "/v1/wait":
		var names []string
		if b.waitQueue != nil {
			select {
			case n := <-b.waitQueue:
				names = []string{n}
			case <-time.After(200 * time.Millisecond):
			}
		}
		_ = json.NewEncoder(w).Encode(protocol.WaitResponse{Requests: names})
	case "/v1/report":
		if b.failReports.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		var rep protocol.Report
		_ = json.NewDecoder(r.Body).Decode(&rep)
		b.mu.Lock()
		b.reports = append(b.reports, rep)
		b.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}
}

func (b *fakeBroker) snapshotReports() []protocol.Report {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []protocol.Report
	for _, r := range b.reports {
		if r.Operation == protocol.OpSnapshot {
			out = append(out, r)
		}
	}
	return out
}

func tarEngine(t *testing.T) string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "examples", "exec-engine", "tar-engine.sh")
}

func setup(t *testing.T, command ...string) (*Config, *fakeBroker) {
	t.Helper()
	if len(command) == 0 {
		command = []string{tarEngine(t)}
	}
	b := &fakeBroker{t: t, repo: t.TempDir(), command: command}
	srv := httptest.NewServer(b)
	t.Cleanup(srv.Close)
	token := filepath.Join(t.TempDir(), "token")
	_ = os.WriteFile(token, []byte("tok"), 0o600)
	return &Config{
		BrokerURL: srv.URL, TokenFile: token, Dir: t.TempDir(), StateDir: t.TempDir(),
		FinalTimeout: 20 * time.Second, HTTP: srv.Client(),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, b
}

func write(t *testing.T, path, content string) {
	t.Helper()
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// seed stores one snapshot of files in the repository, as an earlier Pod
// would have.
func seed(t *testing.T, c *Config, files map[string]string) string {
	t.Helper()
	src := &Config{BrokerURL: c.BrokerURL, TokenFile: c.TokenFile, Dir: t.TempDir(), StateDir: t.TempDir(), HTTP: c.HTTP, Log: c.Log}
	for name, content := range files {
		write(t, filepath.Join(src.Dir, name), content)
	}
	st, _, err := src.takeSnapshot(context.Background(), protocol.RoleSnapshot, snapOpts{}, state{})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond) // the tar engine names snapshots by second
	return st.id
}

func TestUnchangedVolumeSkipsSnapshotsAndConfirmsFinal(t *testing.T) {
	c, b := setup(t)
	id := seed(t, c, map[string]string{"a.txt": "one"})
	st, err := c.initialize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.id != id || !st.valid {
		t.Fatalf("after restore state = %+v, want id %s", st, id)
	}
	before := len(b.snapshotReports())
	st2, _, err := c.takeSnapshot(context.Background(), protocol.RoleSnapshot, snapOpts{}, st)
	if err != nil || st2.id != id || len(b.snapshotReports()) != before {
		t.Fatalf("periodic snapshot of an unchanged volume was not skipped: %+v %v", st2, err)
	}
	if _, _, err := c.takeSnapshot(context.Background(), protocol.RoleSnapshot, snapOpts{final: true}, st); err != nil {
		t.Fatal(err)
	}
	reps := b.snapshotReports()
	last := reps[len(reps)-1]
	if !last.Final || !last.Reused || last.SnapshotID != id {
		t.Fatalf("final report = %+v, want a reused final for %s", last, id)
	}

	// Any change, even one that keeps the size, must be snapshotted.
	write(t, filepath.Join(c.Dir, "a.txt"), "two")
	st3, _, err := c.takeSnapshot(context.Background(), protocol.RoleSnapshot, snapOpts{}, st)
	if err != nil || st3.id == id {
		t.Fatalf("changed volume not snapshotted: %+v %v", st3, err)
	}
}

func TestInterruptedRestoreIsRedone(t *testing.T) {
	c, _ := setup(t)
	seed(t, c, map[string]string{"a.txt": "full", "b.txt": "full"})
	// A previous attempt moved only a.txt into place, with stale content.
	write(t, filepath.Join(c.Dir, "a.txt"), "partial")
	write(t, markerPath(c.Dir, "restoring"), "x")
	if _, err := c.initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"a.txt", "b.txt"} {
		if got, _ := os.ReadFile(filepath.Join(c.Dir, f)); string(got) != "full" {
			t.Errorf("%s = %q, want the full restore", f, got)
		}
	}
	if _, err := os.Stat(markerPath(c.Dir, "restoring")); !os.IsNotExist(err) {
		t.Error("restoring marker left behind")
	}
	if Ready(c.Dir) != nil {
		t.Error("volume not marked initialized")
	}
}

func TestSeededVolumeIsKept(t *testing.T) {
	c, _ := setup(t)
	seed(t, c, map[string]string{"a.txt": "from snapshot"})
	write(t, filepath.Join(c.Dir, "a.txt"), "user data")
	if _, err := c.initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(c.Dir, "a.txt")); string(got) != "user data" {
		t.Errorf("seeded data overwritten: %q", got)
	}
}

func TestCredentialsAreCachedAndRetried(t *testing.T) {
	c, b := setup(t)
	b.fail503.Store(2)
	if _, err := c.credentials(context.Background(), protocol.RoleSnapshot); err != nil {
		t.Fatalf("credentials after two 503s: %v", err)
	}
	if n := b.credReqs.Load(); n != 3 {
		t.Errorf("%d requests, want 3 (two retries)", n)
	}
	if _, err := c.credentials(context.Background(), protocol.RoleSnapshot); err != nil {
		t.Fatal(err)
	}
	if n := b.credReqs.Load(); n != 3 {
		t.Errorf("valid credential not reused: %d requests", n)
	}
	if _, err := c.credentials(context.Background(), protocol.RoleRestore); err != nil {
		t.Fatal(err)
	}
	if _, err := c.credentials(context.Background(), protocol.RoleRestore); err != nil {
		t.Fatal(err)
	}
	if n := b.credReqs.Load(); n != 5 {
		t.Errorf("restore credentials must not be cached: %d requests", n)
	}
}

// TestTerminationWaitsForInflightSnapshot uses an engine whose snapshot
// takes two seconds. SIGTERM arrives in the middle; the snapshot must
// finish, and the final snapshot must follow.
func TestTerminationWaitsForInflightSnapshot(t *testing.T) {
	dir := t.TempDir()
	slow := filepath.Join(dir, "slow.sh")
	write(t, slow, "#!/bin/sh\n[ \"$1\" = snapshot ] && sleep 2\nexec "+tarEngine(t)+" \"$@\"\n")
	_ = os.Chmod(slow, 0o755)
	c, b := setup(t, slow)
	c.IntervalOverride = 200 * time.Millisecond
	write(t, filepath.Join(c.Dir, "f.txt"), "v1")

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- c.RunSidecar(ctx) }()
	time.Sleep(time.Second) // the periodic snapshot is now sleeping
	write(t, filepath.Join(c.Dir, "f.txt"), "v2")
	cancel()
	if err := <-errc; err != nil {
		t.Fatalf("RunSidecar: %v", err)
	}
	reps := b.snapshotReports()
	if len(reps) < 2 {
		t.Fatalf("want the in-flight and the final snapshot, got %+v", reps)
	}
	if reps[0].Error != "" || reps[0].Final {
		t.Errorf("in-flight snapshot was killed or mislabeled: %+v", reps[0])
	}
	last := reps[len(reps)-1]
	if !last.Final || last.Reused || last.Error != "" {
		t.Errorf("final = %+v; the write during the in-flight snapshot needs a new final snapshot", last)
	}
}

func (b *fakeBroker) restoreReports() []protocol.Report {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []protocol.Report
	for _, r := range b.reports {
		if r.Operation == protocol.OpRestore {
			out = append(out, r)
		}
	}
	return out
}

func readResult(t *testing.T, c *Config, name string) map[string]string {
	t.Helper()
	var out map[string]string
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(filepath.Join(markerPath(c.Dir, "results"), name+".json")); err == nil {
			_ = json.Unmarshal(b, &out)
			return out
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no result for request %s", name)
	return nil
}

// TestRequestTriggers drives a running sidecar through request files and
// broker requests, then terminates it with a request still queued.
func TestRequestTriggers(t *testing.T) {
	c, b := setup(t)
	c.IntervalOverride = time.Hour // only triggers cause snapshots
	c.TriggerPoll = 50 * time.Millisecond
	b.waitQueue = make(chan string, 4)
	id := seed(t, c, map[string]string{"a.txt": "one"})
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- c.RunSidecar(ctx) }()
	for StartupReady(c.StateDir) != nil {
		time.Sleep(50 * time.Millisecond)
	}
	reqDir := markerPath(c.Dir, "requests")
	if info, err := os.Stat(reqDir); err != nil || info.Mode()&os.ModeSticky == 0 || info.Mode().Perm() != 0o777 {
		t.Fatalf("requests dir mode = %v, %v; want sticky 0777 so any workload user can write", info.Mode(), err)
	}

	// Unchanged volume: answered with the restored snapshot, no upload.
	write(t, filepath.Join(reqDir, "before-refactor"), "")
	if got := readResult(t, c, "before-refactor"); got["snapshotID"] != id {
		t.Fatalf("unchanged request answered with %v, want %s", got, id)
	}
	// Changed volume: a new snapshot.
	write(t, filepath.Join(c.Dir, "a.txt"), "two")
	time.Sleep(1100 * time.Millisecond) // tar engine IDs have 1s resolution
	write(t, filepath.Join(reqDir, "after-edit"), "")
	got := readResult(t, c, "after-edit")
	if got["snapshotID"] == "" || got["snapshotID"] == id {
		t.Fatalf("changed request answered with %v", got)
	}
	// Unsafe names are ignored and left for nobody.
	write(t, filepath.Join(reqDir, ".hidden"), "")

	// A WorkspaceSnapshot request from the broker is reported by name.
	b.waitQueue <- "nightly-1"
	deadline := time.Now().Add(15 * time.Second)
	for !hasRequestReport(b, "nightly-1") {
		if time.Now().After(deadline) {
			t.Fatal("broker request not reported")
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	if err := <-errc; err != nil {
		t.Fatalf("RunSidecar: %v", err)
	}
	reps := b.snapshotReports()
	if last := reps[len(reps)-1]; !last.Final {
		t.Errorf("last report is not final: %+v", last)
	}
	if _, err := os.Stat(filepath.Join(markerPath(c.Dir, "results"), ".hidden.json")); err == nil {
		t.Error("hidden request file was served")
	}
}

func hasRequestReport(b *fakeBroker, name string) bool {
	for _, r := range b.snapshotReports() {
		if r.Request == name && r.Error == "" && r.SnapshotID != "" {
			return true
		}
	}
	return false
}

func TestQuiesceHandshake(t *testing.T) {
	c, _ := setup(t)
	c.QuiesceTimeout = 5 * time.Second
	if err := c.prepareTriggerDirs(); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(c.Dir, "db"), "v1")
	qdir := markerPath(c.Dir, "quiesce")
	acked := make(chan time.Time, 1)
	go func() { // a cooperating agent
		for {
			if b, err := os.ReadFile(filepath.Join(qdir, "request")); err == nil {
				write(t, filepath.Join(c.Dir, "db"), "flushed") // flush before ack
				_ = os.WriteFile(filepath.Join(qdir, "ack"), b, 0o644)
				acked <- time.Now()
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	st, _, err := c.takeSnapshot(context.Background(), protocol.RoleSnapshot, snapOpts{}, state{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-acked:
	default:
		t.Fatal("snapshot did not wait for the agent's ack")
	}
	if _, err := os.Stat(filepath.Join(qdir, "thaw")); err != nil {
		t.Error("no thaw signal after the snapshot")
	}
	// The flushed write is in the snapshot: restore it elsewhere.
	r := &Config{BrokerURL: c.BrokerURL, TokenFile: c.TokenFile, Dir: t.TempDir(), StateDir: t.TempDir(), HTTP: c.HTTP, Log: c.Log}
	if _, err := r.initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(r.Dir, "db")); string(got) != "flushed" {
		t.Errorf("restored %q; the write made before the ack must be in the snapshot (id %s)", got, st.id)
	}

	// An agent that never answers costs the timeout, then the snapshot runs.
	c.QuiesceTimeout = 300 * time.Millisecond
	_ = os.Remove(filepath.Join(qdir, "ack"))
	write(t, filepath.Join(c.Dir, "db"), "v3")
	start := time.Now()
	if _, _, err := c.takeSnapshot(context.Background(), protocol.RoleSnapshot, snapOpts{}, st); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 300*time.Millisecond {
		t.Error("did not wait for the quiesce timeout")
	}
}

func TestPinnedAndForkRestore(t *testing.T) {
	c, b := setup(t)
	first := seed(t, c, map[string]string{"v.txt": "first"})
	seed(t, c, map[string]string{"v.txt": "second"})

	// Pinned: restores the older snapshot, not the latest.
	b.restoreID = first
	st, err := c.initialize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(c.Dir, "v.txt")); string(got) != "first" {
		t.Fatalf("pinned restore gave %q", got)
	}
	if st.valid {
		t.Errorf("pinned restore state = %+v; an older snapshot must not stand in for the newest", st)
	}

	// Fork: another workspace's repository. Its snapshot is not in ours, so
	// the state must not let a final snapshot reuse it.
	fork, fb := setup(t)
	fb.restoreRepo, fb.restoreSource = b.repo, "parent"
	st, err = fork.initialize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(fork.Dir, "v.txt")); string(got) != "second" {
		t.Fatalf("fork restored %q, want the parent's latest", got)
	}
	if st.valid {
		t.Error("fork state is reusable; the first snapshot must go to the fork's own repository")
	}
	reps := fb.restoreReports()
	if len(reps) != 1 || reps[0].SourceWorkspace != "parent" || reps[0].RestoreFromGeneration != 7 {
		t.Errorf("fork restore report = %+v, want source and generation echoed", reps)
	}

	// A pinned ID that does not exist fails and is reported.
	bad, bb := setup(t)
	bb.restoreID = "20000101T000000-1"
	if _, err := bad.initialize(context.Background()); err == nil {
		t.Fatal("restore of a missing snapshot succeeded")
	}
	if Ready(bad.Dir) == nil {
		t.Error("volume marked initialized after a failed restore")
	}
}

func listTree(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		rel, _ := filepath.Rel(dir, path)
		if d.IsDir() && rel == ".wsnap" {
			return filepath.SkipDir
		}
		if !d.IsDir() {
			b, _ := os.ReadFile(path)
			out[rel] = string(b)
		}
		return nil
	})
	return out
}

func TestRestoreInPlace(t *testing.T) {
	c, b := setup(t)
	old := seed(t, c, map[string]string{"a.txt": "old", "dir/b.txt": "old-b"})
	if _, err := c.initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The agent changes things after the snapshot.
	write(t, filepath.Join(c.Dir, "a.txt"), "new")
	write(t, filepath.Join(c.Dir, "added.txt"), "should vanish")
	_ = os.RemoveAll(filepath.Join(c.Dir, "dir"))
	time.Sleep(1100 * time.Millisecond)

	b.restoreID = old
	st, id, err := c.restoreInPlace(context.Background(), "rollback-1", state{})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"a.txt": "old", "dir/b.txt": "old-b"}
	if got := listTree(t, c.Dir); len(got) != len(want) || got["a.txt"] != "old" || got["dir/b.txt"] != "old-b" {
		t.Fatalf("after restore tree = %v, want exactly %v", got, want)
	}
	if Ready(c.Dir) != nil {
		t.Error("the restore removed the agent's own state")
	}
	if id != old {
		t.Errorf("restored %s, want %s", id, old)
	}
	rr := b.restoreReports()
	if last := rr[len(rr)-1]; last.Request != "rollback-1" || last.SnapshotID != old || last.Error != "" {
		t.Errorf("restore report = %+v", last)
	}
	// History must reflect the rollback: a new snapshot of the restored state.
	snaps := b.snapshotReports()
	if last := snaps[len(snaps)-1]; last.SnapshotID == "" || last.SnapshotID == old || last.Reused || st.id != last.SnapshotID {
		t.Errorf("post-restore snapshot = %+v, state %+v; want a fresh snapshot recorded as current", last, st)
	}
	for _, leftover := range []string{"staging", "trash", "swap"} {
		if _, err := os.Stat(markerPath(c.Dir, leftover)); err == nil {
			t.Errorf(".wsnap/%s left behind", leftover)
		}
	}
}

func TestRestoreFailureLeavesLiveDataAlone(t *testing.T) {
	c, b := setup(t)
	if err := c.prepareTriggerDirs(); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(c.Dir, "keep.txt"), "live")
	b.restoreID = "20000101T000000-1" // does not exist
	if _, _, err := c.restoreInPlace(context.Background(), "bad", state{}); err == nil {
		t.Fatal("restore of a missing snapshot succeeded")
	}
	if got := listTree(t, c.Dir); got["keep.txt"] != "live" || len(got) != 1 {
		t.Fatalf("live data changed by a failed restore: %v", got)
	}
	rr := b.restoreReports()
	if last := rr[len(rr)-1]; last.Request != "bad" || last.Error == "" {
		t.Errorf("failure not reported against the request: %+v", last)
	}
}

// TestSwapRecovery simulates a crash at every point of the swap and checks
// that recovery always ends with exactly the staged contents live.
func TestSwapRecovery(t *testing.T) {
	staged := map[string]string{"a": "new-a", "b": "new-b", "c": "new-c"}
	live := map[string]string{"a": "old-a", "x": "old-x", "y": "old-y"}
	for phase := 1; phase <= 2; phase++ {
		for moved := 0; moved <= 3; moved++ {
			t.Run(fmt.Sprintf("phase%d-moved%d", phase, moved), func(t *testing.T) {
				c := &Config{Dir: t.TempDir(), Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
				staging, trash := markerPath(c.Dir, "staging"), markerPath(c.Dir, "trash")
				_ = os.MkdirAll(staging, 0o755)
				_ = os.MkdirAll(trash, 0o755)
				write(t, MarkerPath(c.Dir), "x")
				for k, v := range staged {
					write(t, filepath.Join(staging, k), v)
				}
				for k, v := range live {
					write(t, filepath.Join(c.Dir, k), v)
				}
				names := []string{"a", "x", "y"}
				if phase == 1 {
					for _, n := range names[:moved] {
						_ = os.Rename(filepath.Join(c.Dir, n), filepath.Join(trash, n))
					}
				} else {
					for _, n := range names {
						_ = os.Rename(filepath.Join(c.Dir, n), filepath.Join(trash, n))
					}
					for _, n := range []string{"a", "b", "c"}[:moved] {
						_ = os.Rename(filepath.Join(staging, n), filepath.Join(c.Dir, n))
					}
				}
				write(t, markerPath(c.Dir, "swap"), fmt.Sprintf("%d", phase))
				if err := c.recoverSwap(); err != nil {
					t.Fatal(err)
				}
				got := listTree(t, c.Dir)
				if len(got) != len(staged) {
					t.Fatalf("tree = %v, want exactly %v", got, staged)
				}
				for k, v := range staged {
					if got[k] != v {
						t.Fatalf("%s = %q, want %q (tree %v)", k, got[k], v, got)
					}
				}
				if Ready(c.Dir) != nil {
					t.Error("agent state lost")
				}
			})
		}
	}
}

func TestRestorerJob(t *testing.T) {
	c, b := setup(t)
	id := seed(t, c, map[string]string{"data.txt": "snapshot"})
	write(t, filepath.Join(c.Dir, "data.txt"), "stale")
	write(t, filepath.Join(c.Dir, "junk.txt"), "stale")
	c.Request = "restore-idle"
	b.restoreID = id
	if err := c.RunRestorer(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := listTree(t, c.Dir); len(got) != 1 || got["data.txt"] != "snapshot" {
		t.Fatalf("tree = %v", got)
	}
	if Ready(c.Dir) != nil {
		t.Error("restored idle volume not marked initialized; the next Pod would restore over it")
	}
	snaps := b.snapshotReports()
	if last := snaps[len(snaps)-1]; !last.Final || last.Role != protocol.RoleMover {
		t.Errorf("restorer did not snapshot the result as a final mover snapshot: %+v", last)
	}
}

// TestSidecarRestartFinishesSwap: the sidecar container crashes mid-swap
// and the kubelet restarts it. Initialization must finish the swap before
// anything else, or the workload would run on a half-swapped volume.
func TestSidecarRestartFinishesSwap(t *testing.T) {
	c, _ := setup(t)
	write(t, MarkerPath(c.Dir), "x") // the volume was initialized before
	staging, trash := markerPath(c.Dir, "staging"), markerPath(c.Dir, "trash")
	write(t, filepath.Join(staging, "restored.txt"), "restored")
	write(t, filepath.Join(c.Dir, "live.txt"), "pre-restore")
	_ = os.MkdirAll(trash, 0o755)
	write(t, markerPath(c.Dir, "swap"), "1")
	if _, err := c.initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := listTree(t, c.Dir); len(got) != 1 || got["restored.txt"] != "restored" {
		t.Fatalf("after restart tree = %v, want only the restored contents", got)
	}
}

func TestOperationTimeout(t *testing.T) {
	dir := t.TempDir()
	hang := filepath.Join(dir, "hang.sh")
	write(t, hang, "#!/bin/sh\n[ \"$1\" = snapshot ] && exec sleep 30\nexec "+tarEngine(t)+" \"$@\"\n")
	_ = os.Chmod(hang, 0o755)
	c, b := setup(t, hang)
	c.OperationTimeout = 500 * time.Millisecond
	write(t, filepath.Join(c.Dir, "f"), "x")
	start := time.Now()
	if _, _, err := c.takeSnapshot(context.Background(), protocol.RoleSnapshot, snapOpts{}, state{}); err == nil {
		t.Fatal("a hung engine did not time out")
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("timeout took %s", took)
	}
	reps := b.snapshotReports()
	if len(reps) == 0 || reps[len(reps)-1].Error == "" {
		t.Error("the timeout was not reported")
	}
}

// TestPinnedRollbackSurvivesAnIdlePod: roll back to an older snapshot,
// change nothing, stop. The next cold start must get the rolled-back state,
// not the newer snapshot (reviewer finding R3).
func TestPinnedRollbackSurvivesAnIdlePod(t *testing.T) {
	c, b := setup(t)
	first := seed(t, c, map[string]string{"v.txt": "first"})
	seed(t, c, map[string]string{"v.txt": "second"})
	b.restoreID = first
	st, err := c.initialize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if _, _, err := c.takeSnapshot(context.Background(), protocol.RoleSnapshot, snapOpts{final: true}, st); err != nil {
		t.Fatal(err)
	}
	b.restoreID = ""
	next, _ := setup(t)
	next.BrokerURL, next.HTTP = c.BrokerURL, c.HTTP
	if _, err := next.initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(next.Dir, "v.txt")); string(got) != "first" {
		t.Fatalf("after an idle rollback the next restore gave %q; the rollback was undone", got)
	}
}

// TestMoverSavesNothingFromAnUninitializedVolume: a Pod killed during its
// restore leaves an empty or partial volume; it must not become the newest
// snapshot (reviewer finding R1).
func TestMoverSavesNothingFromAnUninitializedVolume(t *testing.T) {
	for name, prepare := range map[string]func(c *Config){
		"empty volume": func(c *Config) {},
		// A first restore cut off: "restoring" without "initialized".
		"restore interrupted": func(c *Config) {
			write(t, markerPath(c.Dir, "restoring"), "id")
			write(t, filepath.Join(c.Dir, "half.txt"), "partial")
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, b := setup(t)
			prepare(c)
			if err := c.RunMover(context.Background()); err != nil {
				t.Fatal(err)
			}
			// One NoData report (so the broker marks the volume final), and
			// no snapshot.
			if reps := b.snapshotReports(); len(reps) != 1 || !reps[0].NoData || !reps[0].Final || reps[0].SnapshotID != "" {
				t.Fatalf("want one NoData report and no snapshot, got %+v", reps)
			}
			c.Request = "req-1"
			if err := c.RunMover(context.Background()); err != nil {
				t.Fatal(err)
			}
			reps := b.snapshotReports()
			if len(reps) != 2 || reps[1].Request != "req-1" || reps[1].Error == "" || reps[1].SnapshotID != "" || !reps[1].NoData {
				t.Fatalf("a request on such a volume must fail with the reason, got %+v", reps)
			}
		})
	}
}

// TestSwapPhase2Collision: the workload re-created a directory after phase
// 1 moved the original away; phase 2 must still finish (suspicion S3).
func TestSwapPhase2Collision(t *testing.T) {
	c := &Config{Dir: t.TempDir(), Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	staging, trash := markerPath(c.Dir, "staging"), markerPath(c.Dir, "trash")
	write(t, MarkerPath(c.Dir), "x")
	write(t, filepath.Join(staging, "src", "main.go"), "restored")
	_ = os.MkdirAll(trash, 0o755)
	write(t, filepath.Join(c.Dir, "src", "new.go"), "written after phase 1")
	write(t, markerPath(c.Dir, "swap"), "2")
	if err := c.recoverSwap(); err != nil {
		t.Fatalf("swap stopped halfway: %v", err)
	}
	got := listTree(t, c.Dir)
	if len(got) != 1 || got["src/main.go"] != "restored" {
		t.Fatalf("tree = %v, want only the restored tree", got)
	}
}

// TestCredentialOutlivesTheOperation (finding R10): with a 1h credential
// and a 1h operation timeout, the credential must not be reused at all.
func TestCredentialOutlivesTheOperation(t *testing.T) {
	c, b := setup(t)
	c.OperationTimeout = time.Hour // the fake broker issues 1h credentials
	for i := 0; i < 2; i++ {
		if _, err := c.credentials(context.Background(), protocol.RoleSnapshot); err != nil {
			t.Fatal(err)
		}
	}
	if n := b.credReqs.Load(); n != 2 {
		t.Fatalf("a credential that could expire mid-operation was reused (%d requests)", n)
	}
}

// The workload can write anywhere in the volume. These tests plant the
// symlinks a malicious workload would, and check that the agent never acts
// outside the volume (reviewer finding R6).
func outsideDir(t *testing.T) (string, string) {
	dir := t.TempDir()
	victim := filepath.Join(dir, "kopia")
	write(t, victim, "binary the agent needs")
	return dir, victim
}

func TestSymlinkedRequestsDirCannotDeleteOutsideFiles(t *testing.T) {
	c, _ := setup(t)
	c.TriggerPoll = 20 * time.Millisecond
	if err := c.prepareTriggerDirs(); err != nil {
		t.Fatal(err)
	}
	outside, victim := outsideDir(t)
	_ = os.RemoveAll(markerPath(c.Dir, "requests"))
	if err := os.Symlink(outside, markerPath(c.Dir, "requests")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	c.watchRequestFiles(ctx, make(chan trigger, 16))
	if _, err := os.Stat(victim); err != nil {
		t.Fatalf("the request watcher deleted a file outside the volume through a symlink: %v", err)
	}
}

func TestReplacedStateDirIsRefused(t *testing.T) {
	c, _ := setup(t)
	outside, _ := outsideDir(t)
	if err := os.Symlink(outside, filepath.Join(c.Dir, ".wsnap")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.initialize(context.Background()); err == nil {
		t.Fatal("the agent used a .wsnap that is a symlink out of the volume")
	}
	if entries, _ := os.ReadDir(outside); len(entries) != 1 {
		t.Fatalf("the agent wrote outside the volume: %v", entries)
	}
}

func TestSymlinkedResultsDirCannotWriteOutside(t *testing.T) {
	c, _ := setup(t)
	if err := c.prepareTriggerDirs(); err != nil {
		t.Fatal(err)
	}
	outside, _ := outsideDir(t)
	_ = os.RemoveAll(markerPath(c.Dir, "results"))
	_ = os.Symlink(outside, markerPath(c.Dir, "results"))
	c.writeResult(runResult{trigger: trigger{name: "x"}, id: "s1"})
	if _, err := os.Stat(filepath.Join(outside, "x.json")); err == nil {
		t.Fatal("a result was written outside the volume through a symlink")
	}
}

func TestSymlinkedStagingIsNotFollowed(t *testing.T) {
	c, b := setup(t)
	id := seed(t, c, map[string]string{"a.txt": "data"})
	b.restoreID = id
	if err := c.prepareTriggerDirs(); err != nil {
		t.Fatal(err)
	}
	outside, victim := outsideDir(t)
	_ = os.Symlink(outside, markerPath(c.Dir, "staging"))
	write(t, filepath.Join(c.Dir, "live.txt"), "live")
	_, _, _ = c.restoreInPlace(context.Background(), "r", state{})
	if got, _ := os.ReadFile(victim); string(got) != "binary the agent needs" {
		t.Fatalf("the restore wrote through a planted staging symlink: %q", got)
	}
	if _, err := os.Stat(filepath.Join(outside, "a.txt")); err == nil {
		t.Fatal("restored files landed outside the volume")
	}
}

// TestMoverSavesDataWhoseMarkerTheWorkloadDeleted: "git clean -fdx" in a
// repo at the volume root removes .wsnap; the work must still be saved
// (reviewer #2 finding 3).
func TestMoverSavesDataWhoseMarkerTheWorkloadDeleted(t *testing.T) {
	c, b := setup(t)
	write(t, filepath.Join(c.Dir, "src", "main.go"), "package main")
	if err := c.RunMover(context.Background()); err != nil {
		t.Fatal(err)
	}
	reps := b.snapshotReports()
	if len(reps) != 1 || reps[0].SnapshotID == "" || !reps[0].Final {
		t.Fatalf("data without a marker was not saved: %+v", reps)
	}
}

func TestWarmVolumeWaitsForWriterClaimBeforeStartup(t *testing.T) {
	c, b := setup(t)
	c.IntervalOverride = time.Hour
	b.denySnapshot.Store(true)
	write(t, MarkerPath(c.Dir), "previous Pod initialized this volume\n")
	write(t, filepath.Join(c.Dir, "work.txt"), "keep this work")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- c.RunSidecar(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for b.credReqs.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if b.credReqs.Load() == 0 {
		t.Fatal("sidecar did not ask the broker for writer credentials")
	}
	if err := StartupReady(c.StateDir); err == nil {
		t.Fatal("startup probe passed while the broker denied writer ownership")
	}
	if err := Ready(c.Dir); err != nil {
		t.Fatalf("warm volume marker disappeared: %v", err)
	}

	b.denySnapshot.Store(false)
	for StartupReady(c.StateDir) != nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if err := StartupReady(c.StateDir); err != nil {
		t.Fatalf("startup did not become ready after the broker accepted the writer: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(c.Dir, "work.txt")); err != nil || string(got) != "keep this work" {
		t.Fatalf("warm volume data changed: %q, %v", got, err)
	}
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("RunSidecar: %v", err)
	}
}

// TestMoverFailsClosed: when the mover cannot tell what the volume holds,
// it must fail the Job (the PV is then kept as RescueFailed), never report
// "nothing to save" and let the controller reclaim unsaved data (reviewer
// #3 finding 1).
func TestMoverFailsClosed(t *testing.T) {
	c, b := setup(t)
	write(t, filepath.Join(c.Dir, "work.txt"), "unsaved")
	// .wsnap replaced by a file: openVolume refuses it.
	if err := os.RemoveAll(filepath.Join(c.Dir, ".wsnap")); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(c.Dir, ".wsnap"), "not a directory")
	if err := c.RunMover(context.Background()); err == nil {
		t.Fatal("RunMover succeeded on a volume it could not check; the PV would be reclaimed unsaved")
	}
	if reps := b.snapshotReports(); len(reps) != 0 {
		t.Fatalf("reports: %+v", reps)
	}
}

// TestMoverSavesAfterCrashBetweenMarkers: "initialized" is written before
// "restoring" is removed. A crash between the two must not make every
// later rescue skip the volume (reviewer #3 finding 1), and the next
// sidecar removes the stale marker.
func TestMoverSavesAfterCrashBetweenMarkers(t *testing.T) {
	c, b := setup(t)
	write(t, MarkerPath(c.Dir), "x")
	write(t, markerPath(c.Dir, "restoring"), "id")
	write(t, filepath.Join(c.Dir, "work.txt"), "written after the restore")
	if err := c.RunMover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if reps := b.snapshotReports(); len(reps) != 1 || reps[0].Error != "" || !reps[0].Final {
		t.Fatalf("want one final snapshot, got %+v", reps)
	}
	if _, err := c.initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(markerPath(c.Dir, "restoring")); !os.IsNotExist(err) {
		t.Fatalf("stale restoring marker survived a warm start: %v", err)
	}
}

// TestMoverFinishesAnInterruptedSwap: a volume left mid-swap is completed
// and saved, not skipped.
func TestMoverFinishesAnInterruptedSwap(t *testing.T) {
	c, b := setup(t)
	write(t, MarkerPath(c.Dir), "x")
	write(t, filepath.Join(c.Dir, "old.txt"), "old")
	write(t, filepath.Join(markerPath(c.Dir, "staging"), "new.txt"), "restored")
	write(t, markerPath(c.Dir, "swap"), "1")
	if err := os.MkdirAll(markerPath(c.Dir, "trash"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := c.RunMover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(c.Dir, "new.txt")); string(got) != "restored" {
		t.Fatalf("swap not finished: new.txt = %q", got)
	}
	if reps := b.snapshotReports(); len(reps) != 1 || reps[0].Error != "" {
		t.Fatalf("want one snapshot of the restored tree, got %+v", reps)
	}
}

// TestRollbackSavesLiveWorkFirst (reviewer #3 finding 7): a rollback must
// not destroy work done since the last snapshot, and the request is
// reported only after the restored state is in a snapshot.
func TestRollbackSavesLiveWorkFirst(t *testing.T) {
	c, b := setup(t)
	old := seed(t, c, map[string]string{"a.txt": "old"})
	st, err := c.initialize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	write(t, filepath.Join(c.Dir, "a.txt"), "unsaved work")
	b.restoreID = old
	if _, _, err := c.restoreInPlace(context.Background(), "rollback-1", st); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	reps := slices.Clone(b.reports)
	b.mu.Unlock()
	// Expected order: safety snapshot, post-restore snapshot, restore report.
	var order []string
	for _, r := range reps {
		if r.Operation == protocol.OpRestore && r.Request == "" {
			continue // the initial restore in seed/initialize
		}
		order = append(order, string(r.Operation))
	}
	if !slices.Equal(order[len(order)-3:], []string{"snapshot", "snapshot", "restore"}) {
		t.Fatalf("report order %v, want a safety snapshot, then the post-restore snapshot, then the restore", order)
	}
	safety := reps[len(reps)-3]
	if safety.SnapshotID == "" || safety.SnapshotID == old {
		t.Fatalf("no new safety snapshot: %+v", safety)
	}
	// The unsaved work is in the safety snapshot: restore it elsewhere.
	b.restoreID = safety.SnapshotID
	next, _ := setup(t)
	next.BrokerURL, next.HTTP = c.BrokerURL, c.HTTP
	if _, err := next.initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(next.Dir, "a.txt")); string(got) != "unsaved work" {
		t.Fatalf("safety snapshot holds %q; the rollback destroyed live work", got)
	}
}

// TestCredentialsAreReusedWithDefaults (reviewer #3 finding 9): with a 1h
// credential and the default 30m operation limit, the cache must work.
func TestCredentialsAreReusedWithDefaults(t *testing.T) {
	c, b := setup(t)
	c.OperationTimeout = 30 * time.Minute
	for range 3 {
		if _, err := c.credentials(context.Background(), protocol.RoleSnapshot); err != nil {
			t.Fatal(err)
		}
	}
	if n := b.credReqs.Load(); n != 1 {
		t.Fatalf("%d credential requests for 3 uses; the cache is dead", n)
	}
}

// TestRootAgentTakesOverForeignStateDir (reviewer #3 finding 1): a .wsnap
// created by an agent under another uid must not block every later agent.
// Runs only as root (the Linux agent image).
func TestRootAgentTakesOverForeignStateDir(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root")
	}
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, ".wsnap"), 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(filepath.Join(dir, ".wsnap"), 1000, 1000); err != nil {
		t.Fatal(err)
	}
	v, err := openVolume(dir)
	if err != nil {
		t.Fatalf("root agent refused a .wsnap owned by uid 1000: %v", err)
	}
	v.Close()
	info, _ := os.Lstat(filepath.Join(dir, ".wsnap"))
	if uid, _ := ownerUID(info); uid != 0 || info.Mode().Perm() != 0o755 {
		t.Fatalf("after take-over: uid %d mode %v; the workload could still rename entries", uid, info.Mode().Perm())
	}
	// A symlink is still refused.
	dir2 := t.TempDir()
	if err := os.Symlink("/etc", filepath.Join(dir2, ".wsnap")); err != nil {
		t.Fatal(err)
	}
	if v, err := openVolume(dir2); err == nil {
		v.Close()
		t.Fatal("a .wsnap symlink was accepted")
	}
}

// TestNotWorkspaceDataReportsErrorsAsErrors: a check that fails is an
// error, never a reason to save nothing (reviewer #3 finding 1; before the
// fix, the error text was returned as the reason).
func TestNotWorkspaceDataReportsErrorsAsErrors(t *testing.T) {
	c, _ := setup(t)
	if err := os.RemoveAll(filepath.Join(c.Dir, ".wsnap")); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(c.Dir, ".wsnap"), "not a directory")
	reason, err := c.notWorkspaceData()
	if err == nil {
		t.Fatalf("no error; reason %q would let the controller reclaim unsaved data", reason)
	}
}

// TestJobFailsWhenItsReportIsLost (reviewer #4 finding 1): a mover whose
// snapshot report never reaches the broker must fail, or the controller
// reclaims the volume while lastSnapshot still names older data.
func TestJobFailsWhenItsReportIsLost(t *testing.T) {
	c, b := setup(t)
	write(t, filepath.Join(c.Dir, "work.txt"), "unsaved")
	b.failReports.Store(true)
	defer func(d time.Duration) { strictReportFor = d }(strictReportFor)
	strictReportFor = 2 * time.Second
	if err := c.RunMover(context.Background()); err == nil {
		t.Fatal("RunMover succeeded although no report reached the broker")
	}
}

// TestMissingRepositoryWithRecordedSnapshotFails (reviewer #4 finding 7):
// the broker recorded a snapshot, so a missing repository must stop the
// start, not begin an empty workspace.
func TestMissingRepositoryWithRecordedSnapshotFails(t *testing.T) {
	c, b := setup(t)
	b.latestID = "recorded-snap"
	if _, err := c.initialize(context.Background()); err == nil {
		t.Fatal("initialize started an empty workspace although the broker recorded a snapshot")
	}
	if Ready(c.Dir) == nil {
		t.Fatal("the volume was marked initialized")
	}
}

// TestRollbackKeepsWritesMadeDuringTheDownload (reviewer #4 finding 4):
// the agent writes while the restore downloads; those writes must be in
// the safety snapshot.
func TestRollbackKeepsWritesMadeDuringTheDownload(t *testing.T) {
	wrapper := filepath.Join(t.TempDir(), "engine.sh")
	// On "restore", write into the live volume (two levels above
	// .wsnap/staging) before the real restore.
	script := "#!/bin/sh\nif [ \"$1\" = restore ]; then echo during > \"$(dirname \"$(dirname \"$WSNAP_DIR\")\")/during.txt\"; fi\nexec " + tarEngine(t) + " \"$@\"\n"
	if err := os.WriteFile(wrapper, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	c, b := setup(t, wrapper)
	old := seed(t, c, map[string]string{"a.txt": "old"})
	st, err := c.initialize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	b.restoreID = old
	if _, _, err := c.restoreInPlace(context.Background(), "rollback-1", st); err != nil {
		t.Fatal(err)
	}
	snaps := b.snapshotReports()
	safety := snaps[len(snaps)-2]
	// Check with the plain engine, which writes nothing by itself.
	b.restoreID, b.command = safety.SnapshotID, []string{tarEngine(t)}
	next, _ := setup(t)
	next.BrokerURL, next.HTTP = c.BrokerURL, c.HTTP
	if _, err := next.initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(next.Dir, "during.txt")); string(got) != "during\n" {
		t.Fatalf("a write made during the download is in no snapshot (safety snapshot %s holds %q)", safety.SnapshotID, got)
	}
}

// TestRequestFilesAreSpaced (reviewer #4 finding 3): the workload cannot
// trigger snapshots faster than FileRequestSpacing.
func TestRequestFilesAreSpaced(t *testing.T) {
	c, _ := setup(t)
	c.IntervalOverride = time.Hour
	c.TriggerPoll = 20 * time.Millisecond
	c.FileRequestSpacing = 2 * time.Second
	seed(t, c, map[string]string{"a.txt": "one"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- c.RunSidecar(ctx) }()
	for StartupReady(c.StateDir) != nil {
		time.Sleep(20 * time.Millisecond)
	}
	reqDir := markerPath(c.Dir, "requests")
	start := time.Now()
	write(t, filepath.Join(reqDir, "r1"), "")
	readResult(t, c, "r1")
	write(t, filepath.Join(reqDir, "r2"), "")
	readResult(t, c, "r2")
	if elapsed := time.Since(start); elapsed < 2*time.Second {
		t.Fatalf("two request files answered within %s; want at least the 2s spacing", elapsed)
	}
	cancel()
	<-errc
}
