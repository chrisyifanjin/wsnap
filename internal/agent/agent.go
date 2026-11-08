// Package agent is the wsnap agent. One binary runs as an in-pod sidecar
// (restore, periodic snapshots, final snapshot on SIGTERM) or as a Job
// (mover, maintenance).
package agent

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chrisyifanjin/wsnap/internal/engine"
	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

// Config is shared by all agent modes.
type Config struct {
	BrokerURL string
	// TokenFile is the projected service account token. It is re-read for
	// every request because the kubelet rotates it.
	TokenFile string
	// Dir is the workspace data directory.
	Dir string
	// StateDir is private scratch space for engine config and caches.
	StateDir string
	// WorkerPodUID is the workload Pod gated by this separate worker.
	WorkerPodUID      string
	workerMayFinalize atomic.Bool
	// FinalTimeout bounds everything after SIGTERM: finishing an in-flight
	// snapshot plus the final one. Keep it below the Pod's
	// terminationGracePeriodSeconds.
	FinalTimeout time.Duration
	// IntervalOverride replaces the repository's interval (for tests).
	IntervalOverride time.Duration
	// QuiesceTimeout enables the freeze handshake (see quiesce). Zero
	// disables it.
	QuiesceTimeout time.Duration
	// Request is the WorkspaceSnapshot a mover Job serves.
	Request string
	// TriggerPoll is how often the sidecar looks for request files.
	TriggerPoll time.Duration
	// FileRequestSpacing is the shortest time between two snapshots that
	// request files (from the workload) trigger.
	FileRequestSpacing time.Duration
	// StrictReports makes a failed report an error (Jobs; see report).
	StrictReports bool
	// OperationTimeout bounds one snapshot or restore. Engines retry
	// backend errors for a long time (restic up to 15 minutes), which would
	// hold the request queue during a storage outage.
	OperationTimeout time.Duration
	HTTP             *http.Client
	Log              *slog.Logger

	mu            sync.Mutex
	cache         map[protocol.Role]cachedCredentials
	repoOpTimeout time.Duration
}

type cachedCredentials struct {
	resp       *protocol.CredentialsResponse
	validUntil time.Time
}

const (
	// credentialMargin is how long before expiry a cached credential is
	// replaced, so no operation starts with a credential about to expire.
	credentialMargin = 5 * time.Minute
	// unexpiringReuse bounds reuse of credentials without an expiry (static
	// provider), so repository changes reach running sidecars.
	unexpiringReuse = 10 * time.Minute
	// redeliveryQuiet is how long a finished request is not run again when
	// the broker delivers it again.
	redeliveryQuiet = time.Minute
	// conflictPoll is the retry interval while the broker answers 409.
	conflictPoll = 3 * time.Second
)

// Files inside the volume's .wsnap directory.
func markerPath(dir, name string) string { return filepath.Join(dir, engine.StateDirName, name) }

// MarkerPath is the file that says the volume was initialized (restored, or
// found with data). It lives in the volume, so a warm resume skips the
// restore.
func MarkerPath(dir string) string { return markerPath(dir, "initialized") }

// Ready reports whether the workspace volume has been initialized. This
// marker can survive across Pods and must not be used by the startup probe.
func Ready(dir string) error {
	_, err := os.Stat(MarkerPath(dir))
	return err
}

// startupReadyPath is on the sidecar's private volume, so a marker left on
// a reused workspace volume cannot start the workload before this Pod is
// accepted as its writer.
func startupReadyPath(stateDir string) string { return filepath.Join(stateDir, "startup-ready") }

// StartupReady is checked by the sidecar's startup probe.
func StartupReady(stateDir string) error {
	_, err := os.Stat(startupReadyPath(stateDir))
	return err
}

func workerReadyPath(dir, podUID string) (string, error) {
	if podUID == "" || podUID == "." || filepath.Base(podUID) != podUID {
		return "", fmt.Errorf("invalid workload Pod UID %q", podUID)
	}
	return filepath.Join(dir, engine.StateDirName, "worker-ready", podUID), nil
}

// WaitWorker blocks a workload's first init container until its separate
// worker has restored the PVC and claimed this Pod as writer.
func WaitWorker(ctx context.Context, dir, podUID string) error {
	path, err := workerReadyPath(dir, podUID)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

type brokerError struct {
	status int
	body   string
}

func (e *brokerError) Error() string { return fmt.Sprintf("broker returned %d: %s", e.status, e.body) }

func retryable(err error) bool {
	var be *brokerError
	if errors.As(err, &be) {
		return be.status >= 500 || be.status == http.StatusTooManyRequests
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// post retries network errors, 5xx and 429 with exponential backoff, so a
// broker restart does not fail a final snapshot.
func (c *Config) post(ctx context.Context, path string, in, out any) error {
	var err error
	backoff := 500 * time.Millisecond
	for attempt := 0; attempt < 5; attempt++ {
		if err = c.postOnce(ctx, path, in, out); err == nil || !retryable(err) {
			return err
		}
		c.Log.Warn("broker request failed; retrying", "path", path, "error", err, "in", backoff)
		select {
		case <-ctx.Done():
			return err
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return err
}

func (c *Config) postOnce(ctx context.Context, path string, in, out any) error {
	token, err := os.ReadFile(c.TokenFile)
	if err != nil {
		return fmt.Errorf("read token: %w", err)
	}
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BrokerURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 300 {
		return &brokerError{status: resp.StatusCode, body: strings.TrimSpace(string(data))}
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

// credentials returns a credential for role, reusing a cached one while it
// is valid. Restore and maintenance credentials are one-shot and never
// cached.
func (c *Config) credentials(ctx context.Context, role protocol.Role) (*protocol.CredentialsResponse, error) {
	cacheable := role == protocol.RoleSnapshot || role == protocol.RoleMover
	if cacheable {
		c.mu.Lock()
		cc, ok := c.cache[role]
		c.mu.Unlock()
		if ok && time.Now().Before(cc.validUntil) {
			return cc.resp, nil
		}
	}
	var resp protocol.CredentialsResponse
	if err := c.post(ctx, "/v1/credentials", protocol.CredentialsRequest{Role: role}, &resp); err != nil {
		return nil, err
	}
	c.adopt(&resp)
	if cacheable {
		until := time.Now().Add(unexpiringReuse)
		if exp := resp.Credentials.ExpiresAt; !exp.IsZero() {
			// An operation started with this credential may run for
			// OperationTimeout; it must not expire halfway. With the
			// defaults (1h credential, 30m operations) one credential
			// serves 25 minutes of snapshots.
			until = exp.Add(-max(credentialMargin, c.operationTimeout()+credentialMargin))
			if time.Until(exp) < c.operationTimeout() {
				c.Log.Warn("credentials expire before an operation's time limit; raise credentials.ttl or lower --operation-timeout",
					"expiresIn", time.Until(exp).Round(time.Second), "operationTimeout", c.operationTimeout())
			}
		}
		c.mu.Lock()
		if c.cache == nil {
			c.cache = map[protocol.Role]cachedCredentials{}
		}
		c.cache[role] = cachedCredentials{resp: &resp, validUntil: until}
		c.mu.Unlock()
	}
	return &resp, nil
}

// credentialsFor fetches a one-shot credential bound to a request.
func (c *Config) credentialsFor(ctx context.Context, req protocol.CredentialsRequest) (*protocol.CredentialsResponse, error) {
	var resp protocol.CredentialsResponse
	if err := c.post(ctx, "/v1/credentials", req, &resp); err != nil {
		return nil, err
	}
	c.adopt(&resp)
	return &resp, nil
}

// invalidate drops a cached credential after a failure, in case the
// failure came from a revoked or misconfigured credential.
func (c *Config) invalidate(role protocol.Role) {
	c.mu.Lock()
	delete(c.cache, role)
	c.mu.Unlock()
}

// strictReportFor is how long a Job keeps retrying a report. A variable
// for tests.
var strictReportFor = 5 * time.Minute

// report sends r to the broker. For a sidecar a lost report delays
// bookkeeping only: without a recorded final snapshot, the controller
// rescues the volume later. A Job (StrictReports) exists to produce the
// record, so it retries for longer and returns the error, and the Job
// fails instead of completing with nothing recorded.
func (c *Config) report(ctx context.Context, r protocol.Report) error {
	err := c.post(ctx, "/v1/report", r, nil)
	if c.StrictReports {
		deadline := time.Now().Add(strictReportFor)
		for err != nil && retryable(err) && time.Now().Before(deadline) && ctx.Err() == nil {
			c.Log.Warn("report failed; retrying", "operation", r.Operation, "error", err)
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
			}
			err = c.post(ctx, "/v1/report", r, nil)
		}
	}
	if err != nil {
		c.Log.Error("report failed", "operation", r.Operation, "error", err)
		if c.StrictReports {
			return fmt.Errorf("report %s: %w", r.Operation, err)
		}
	}
	return nil
}

// open fetches a credential for role and opens the repository with it.
func (c *Config) open(ctx context.Context, role protocol.Role) (*protocol.CredentialsResponse, engine.Engine, engine.Target, error) {
	creds, err := c.credentials(ctx, role)
	if err != nil {
		return nil, nil, engine.Target{}, err
	}
	eng, err := engine.New(creds.Engine)
	if err != nil {
		return creds, nil, engine.Target{}, err
	}
	t := engine.Target{
		Backend: creds.Backend, Credentials: creds.Credentials, Password: creds.RepositoryPassword,
		Workspace: creds.Workspace, StateDir: c.StateDir, ReadOnly: !role.CanWrite(),
		Maintenance: role == protocol.RoleMaintenance,
	}
	if !creds.Credentials.Scoped {
		c.Log.Warn("broker issued an unscoped credential; it can reach every workspace in the repository", "role", role)
	}
	return creds, eng, t, eng.Init(ctx, t)
}

func (c *Config) withOperationTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	d := c.operationTimeout()
	if d <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, d)
}

// operationTimeout is the repository's spec.operationTimeout once the
// broker has sent it, else the --operation-timeout flag.
func (c *Config) operationTimeout() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.repoOpTimeout > 0 {
		return c.repoOpTimeout
	}
	return c.OperationTimeout
}

func (c *Config) adopt(resp *protocol.CredentialsResponse) {
	if d := resp.OperationTimeout.Duration; d > 0 {
		c.mu.Lock()
		c.repoOpTimeout = d
		c.mu.Unlock()
	}
}

// state is what the sidecar knows about the newest snapshot of the volume.
type state struct {
	id    string
	fp    fingerprint
	valid bool // fp describes the volume as captured in snapshot id
}

// snapOpts carries per-snapshot options.
type snapOpts struct {
	final bool
	// request names the WorkspaceSnapshot being served. A request is
	// reported even when nothing changed.
	request string
	// noQuiesce skips the handshake when the caller already froze the agent.
	noQuiesce bool
	// complete makes a partial snapshot an error: the caller is about to
	// delete the live data (the safety snapshot before a restore).
	complete bool
}

// takeSnapshot snapshots the volume unless nothing changed since st. An
// unchanged volume is answered with st.id: for a final snapshot this makes
// shutdown of an idle workspace nearly instant. It returns the new state
// and the snapshot ID that now represents the volume.
func (c *Config) takeSnapshot(ctx context.Context, role protocol.Role, o snapOpts, st state) (state, string, error) {
	start := time.Now().UTC()
	// The fingerprint is taken before the snapshot starts. A write during
	// the snapshot changes the next fingerprint, so it is never missed.
	fp, fpErr := computeFingerprint(c.Dir)
	if fpErr != nil {
		c.Log.Warn("cannot fingerprint the volume; snapshotting anyway", "error", fpErr)
	}
	if fpErr == nil && st.valid && fp == st.fp {
		if !o.final && o.request == "" {
			c.Log.Debug("no change since the last snapshot; skipping", "id", st.id)
			return st, st.id, nil
		}
		if err := c.report(ctx, protocol.Report{Role: role, Operation: protocol.OpSnapshot, SnapshotID: st.id,
			Final: o.final, Reused: true, Request: o.request, StartTime: start, EndTime: time.Now().UTC()}); err != nil {
			return st, "", err
		}
		c.Log.Info("no change since the last snapshot; confirmed it", "id", st.id, "final", o.final, "request", o.request)
		return st, st.id, nil
	}
	if c.QuiesceTimeout > 0 && !o.noQuiesce {
		thaw := c.quiesce(ctx)
		defer thaw()
		// Writes before the agent acknowledged the freeze belong in this
		// snapshot, so fingerprint the frozen state.
		fp, fpErr = computeFingerprint(c.Dir)
	}
	rep := protocol.Report{Role: role, Operation: protocol.OpSnapshot, Final: o.final, Request: o.request, StartTime: start}
	ctx, cancel := c.withOperationTimeout(ctx)
	defer cancel()
	_, eng, t, err := c.open(ctx, role)
	var snap engine.Snapshot
	if err == nil {
		snap, err = eng.Snapshot(ctx, t, c.Dir)
	}
	rep.EndTime = time.Now().UTC()
	if err != nil {
		c.invalidate(role)
		rep.Error = err.Error()
		c.report(context.WithoutCancel(ctx), rep)
		return st, "", err
	}
	rep.SnapshotID = snap.ID
	if snap.Partial && rep.Final {
		// Some files are in no snapshot: record what was saved, but do not
		// mark the volume final, so wsnap never deletes it.
		c.Log.Warn("snapshot is partial: some files could not be read; not marking it final", "id", snap.ID)
		rep.Final = false
	}
	if err := c.report(context.WithoutCancel(ctx), rep); err != nil {
		return st, "", err
	}
	if snap.Partial && o.complete {
		return st, snap.ID, fmt.Errorf("snapshot %s is partial: some files could not be read, so the live data is kept", snap.ID)
	}
	if snap.Partial && o.final && role == protocol.RoleMover {
		return st, snap.ID, fmt.Errorf("snapshot %s is partial: some files could not be read; keeping the volume", snap.ID)
	}
	c.Log.Info("snapshot taken", "id", snap.ID, "final", o.final, "request", o.request, "seconds", rep.EndTime.Sub(start).Seconds())
	return state{id: snap.ID, fp: fp, valid: fpErr == nil}, snap.ID, nil
}

// quiesce runs the opt-in freeze handshake through files, so it works with
// any agent: the sidecar writes quiesce/request with a token, the agent
// flushes and writes the same token to quiesce/ack, and after the snapshot
// the sidecar writes quiesce/thaw. An agent that does not answer within
// QuiesceTimeout gets a crash-consistent snapshot instead.
func (c *Config) quiesce(ctx context.Context) (thaw func()) {
	v, err := openVolume(c.Dir)
	if err != nil {
		c.Log.Warn("cannot request quiesce", "error", err)
		return func() {}
	}
	dir := markerPath(c.Dir, "quiesce")
	token := fmt.Sprintf("%d", time.Now().UnixNano())
	_ = v.Remove(filepath.Join(dir, "ack"))
	_ = v.Remove(filepath.Join(dir, "thaw"))
	if err := v.WriteFile(filepath.Join(dir, "request"), []byte(token+"\n"), 0o644); err != nil {
		c.Log.Warn("cannot request quiesce", "error", err)
		v.Close()
		return func() {}
	}
	deadline := time.Now().Add(c.QuiesceTimeout)
	for {
		if b, err := v.ReadFile(filepath.Join(dir, "ack")); err == nil && strings.TrimSpace(string(b)) == token {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			c.Log.Warn("agent did not acknowledge quiesce; taking a crash-consistent snapshot", "timeout", c.QuiesceTimeout)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return func() {
		_ = v.Remove(filepath.Join(dir, "request"))
		_ = v.WriteFile(filepath.Join(dir, "thaw"), []byte(token+"\n"), 0o644)
		v.Close()
	}
}

// initialize makes the volume ready for the workload. The order of marker
// writes makes an interrupted restore detectable: "restoring" exists from
// the moment restored files start to appear until "initialized" exists.
func (c *Config) initialize(ctx context.Context) (state, error) {
	if c.swapPending() {
		// A swap was cut off. A restorer Job may still be running it, so
		// get admitted as the writer first: the broker refuses (409) while
		// a Job works on this volume.
		if _, err := c.credentials(ctx, protocol.RoleSnapshot); err != nil {
			return state{}, err
		}
	}
	if err := c.recoverSwap(); err != nil {
		return state{}, err
	}
	v, err := openVolume(c.Dir)
	if err != nil {
		return state{}, err
	}
	defer v.Close()
	restoring := markerPath(c.Dir, "restoring")
	if Ready(c.Dir) == nil {
		c.Log.Info("volume already initialized; skipping restore")
		// "initialized" is written before "restoring" is removed; a crash
		// between the two leaves a stale "restoring" that would make every
		// later rescue think a restore was cut off.
		if err := v.Remove(restoring); err != nil && !os.IsNotExist(err) {
			return state{}, err
		}
		return state{}, c.prepareTriggerDirs()
	}
	if _, err := v.Lstat(restoring); err == nil {
		// Get admitted before deleting anything: another Pod on this volume
		// (RWX, or RWO on the same node) may be the writer mid-restore.
		if _, err := c.credentialsFor(ctx, protocol.CredentialsRequest{Role: protocol.RoleRestore}); err != nil {
			return state{}, err
		}
		c.Log.Warn("a previous restore was interrupted; clearing the partial data")
		if err := clearWorkspace(v, c.Dir); err != nil {
			return state{}, err
		}
	}
	empty, err := isEmpty(v, c.Dir)
	if err != nil {
		return state{}, err
	}
	var res restoreResult
	if empty {
		if res, err = c.restore(ctx); err != nil {
			return state{}, err
		}
	} else {
		c.Log.Info("volume has data but no marker; keeping it")
	}
	if err := c.prepareTriggerDirs(); err != nil {
		return state{}, err
	}
	syncDisks() // the restored files reach the disk before "initialized"
	if err := v.WriteFile(MarkerPath(c.Dir), []byte(time.Now().UTC().Format(time.RFC3339)+"\n"), 0o644); err != nil {
		return state{}, err
	}
	syncDisks()
	if err := v.Remove(restoring); err != nil && !os.IsNotExist(err) {
		return state{}, err
	}
	// Only a restore of this workspace's newest snapshot may stand in for
	// a snapshot. A fork's snapshot is in another repository, and a pinned
	// (older) snapshot is not the newest: confirming it as final would
	// leave the newer snapshot as "latest" and undo the rollback.
	if res.id == "" || res.source != "" || res.pinned {
		return state{}, nil
	}
	fp, err := computeFingerprint(c.Dir)
	return state{id: res.id, fp: fp, valid: err == nil}, nil
}

// prepareTriggerDirs creates the directories the workload writes to. They
// are world-writable with the sticky bit, like /tmp, because the workload
// may run as any user.
func (c *Config) prepareTriggerDirs() error {
	v, err := openVolume(c.Dir)
	if err != nil {
		return err
	}
	defer v.Close()
	if err := v.MkdirAll(markerPath(c.Dir, ""), 0o755); err != nil {
		return err
	}
	for _, d := range []string{"requests", "results", "quiesce"} {
		path := markerPath(c.Dir, d)
		if err := v.MkdirAll(path, 0o755); err != nil {
			return err
		}
		mode := os.ModeSticky | 0o777
		if d == "results" {
			mode = 0o755
		}
		if err := v.Chmod(path, mode); err != nil {
			return err
		}
	}
	return nil
}

type restoreResult struct {
	id     string
	source string // set for a fork
	pinned bool   // the broker chose a specific, possibly older, snapshot
}

// restore writes a snapshot into a staging directory inside the volume,
// then renames the entries into place. Renames within one filesystem are
// cheap and never copy data. The broker decides which repository and
// snapshot: the latest of this workspace, a pinned one, or another
// workspace's (fork).
func (c *Config) restore(ctx context.Context) (restoreResult, error) {
	start := time.Now().UTC()
	rep := protocol.Report{Role: protocol.RoleRestore, Operation: protocol.OpRestore, StartTime: start}
	creds, eng, t, err := c.open(ctx, protocol.RoleRestore)
	var id string
	if creds != nil {
		rep.SourceWorkspace, rep.RestoreFromGeneration = creds.SourceWorkspace, creds.RestoreFromGeneration
	}
	switch {
	case errors.Is(err, engine.ErrRepositoryNotFound) && creds.SnapshotID == "" && creds.SourceWorkspace == "" && creds.LatestSnapshotID == "":
		err = nil // a new workspace: no repository, and the broker recorded no snapshot
	case errors.Is(err, engine.ErrRepositoryNotFound):
		// The broker recorded a snapshot (or a fork names one), so a missing
		// repository is a misconfiguration or lost storage, never a new
		// workspace. Starting empty would hide it and, after the first
		// snapshot, bury the real history.
		err = fmt.Errorf("the repository is missing, but snapshot %s is expected: %w",
			cmp.Or(creds.SnapshotID, creds.LatestSnapshotID, "of "+creds.SourceWorkspace), err)
	case err == nil && creds.SnapshotID != "":
		id = creds.SnapshotID
		err = c.stageAndMove(ctx, eng, t, id)
	case err == nil && creds.LatestSnapshotID != "":
		id = creds.LatestSnapshotID
		err = c.stageAndMove(ctx, eng, t, id)
	case err == nil:
		var latest *engine.Snapshot
		latest, err = eng.Latest(ctx, t)
		if err == nil && latest != nil {
			id = latest.ID
			err = c.stageAndMove(ctx, eng, t, id)
		}
	}
	rep.EndTime = time.Now().UTC()
	if err != nil {
		var be *brokerError
		if errors.As(err, &be) && be.status < 500 {
			// The broker refused (e.g. another writer or a pending rescue);
			// a report would be refused too.
			return restoreResult{}, err
		}
		rep.Error = err.Error()
		c.report(ctx, rep)
		return restoreResult{}, err
	}
	rep.SnapshotID = id
	c.report(ctx, rep)
	if id == "" {
		c.Log.Info("no snapshot yet; starting with an empty workspace")
	} else {
		c.Log.Info("restored", "id", id, "from", rep.SourceWorkspace, "seconds", rep.EndTime.Sub(start).Seconds())
	}
	return restoreResult{id: id, source: rep.SourceWorkspace, pinned: creds.SnapshotID != ""}, nil
}

func (c *Config) stageAndMove(ctx context.Context, eng engine.Engine, t engine.Target, id string) error {
	v, err := openVolume(c.Dir)
	if err != nil {
		return err
	}
	defer v.Close()
	staging := markerPath(c.Dir, "staging")
	if err := v.RemoveAll(staging); err != nil {
		return err
	}
	if err := v.MkdirAll(staging, 0o755); err != nil {
		return err
	}
	// Checked right before the engine, which resolves the path itself.
	if err := v.plainDir(staging); err != nil {
		return err
	}
	if err := eng.Restore(ctx, t, id, staging); err != nil {
		return err
	}
	if err := v.WriteFile(markerPath(c.Dir, "restoring"), []byte(id+"\n"), 0o644); err != nil {
		return err
	}
	entries, err := v.ReadDir(staging)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := v.Rename(filepath.Join(staging, e.Name()), filepath.Join(c.Dir, e.Name())); err != nil {
			return fmt.Errorf("move restored %s into place: %w", e.Name(), err)
		}
	}
	return v.RemoveAll(staging)
}

// clearWorkspace removes everything except the entries that never belong
// to the workspace.
func clearWorkspace(v *volume, dir string) error {
	entries, err := v.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Name() == "lost+found" {
			continue
		}
		if e.Name() == engine.StateDirName {
			if err := v.RemoveAll(markerPath(dir, "staging")); err != nil {
				return err
			}
			continue
		}
		if err := v.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// isEmpty ignores the entries that never belong to the workspace.
func isEmpty(v *volume, dir string) (bool, error) {
	entries, err := v.ReadDir(dir)
	if err != nil {
		return false, err
	}
	for _, e := range entries {
		if e.Name() != engine.StateDirName && e.Name() != "lost+found" {
			return false, nil
		}
	}
	return true, nil
}

// workerPoll is how often a worker asks whether its workload Pod stopped.
var workerPoll = 3 * time.Second

// RunWorker runs the sidecar's snapshot loop in a separate Pod. The broker
// tells it when its workload Pod has stopped; only then may it mark a
// snapshot final. A drain evicts both Pods at once, so on SIGTERM the
// worker first waits up to half its final timeout for the workload to
// stop, then takes the last snapshot. If the workload still runs, that
// snapshot is not final, and the volume stays protected for the next Pod
// or a rescue.
func (c *Config) RunWorker(ctx context.Context) error {
	if _, err := workerReadyPath(c.Dir, c.WorkerPodUID); err != nil {
		return err
	}
	wctx, stop := context.WithCancel(context.WithoutCancel(ctx))
	defer stop()
	go c.watchWorkload(ctx, wctx, stop)
	return c.RunSidecar(wctx)
}

// watchWorkload calls stop when the workload Pod has stopped, or when
// term is cancelled and the workload does not stop in time.
func (c *Config) watchWorkload(term, run context.Context, stop context.CancelFunc) {
	ticker := time.NewTicker(workerPoll)
	defer ticker.Stop()
	terminated := term.Done()
	var giveUp <-chan time.Time
	for {
		var state struct {
			Stop bool `json:"stop"`
		}
		pctx, cancel := context.WithTimeout(run, 10*time.Second)
		err := c.post(pctx, "/v1/worker-state", struct{}{}, &state)
		cancel()
		switch {
		case err == nil && state.Stop:
			c.workerMayFinalize.Store(true)
			stop()
			return
		case err != nil && run.Err() == nil:
			c.Log.Warn("cannot read workload state", "error", err)
		}
		select {
		case <-run.Done():
			return
		case <-terminated:
			terminated = nil
			giveUp = time.After(c.FinalTimeout / 2)
			c.Log.Info("termination requested; waiting for the workload Pod to stop", "limit", c.FinalTimeout/2)
		case <-giveUp:
			c.Log.Warn("the workload Pod still runs; the last snapshot is not final")
			stop()
			return
		case <-ticker.C:
		}
	}
}

// RunSidecar initializes the volume, then serves three kinds of snapshot
// one at a time: periodic ones, requests (WorkspaceSnapshot objects through
// the broker, and files in .wsnap/requests), and on SIGTERM a final one
// after any in-flight snapshot finishes.
func (c *Config) RunSidecar(ctx context.Context) error {
	readyPath := startupReadyPath(c.StateDir)
	// A restarted sidecar container keeps its state volume; it must be
	// admitted again before the probe passes.
	if err := os.Remove(readyPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("clear startup readiness: %w", err)
	}
	st, err := c.initializeWithRetry(ctx)
	if err != nil {
		return nil // terminated before initialization; nothing to snapshot
	}
	if err := os.MkdirAll(c.StateDir, 0o700); err != nil {
		return fmt.Errorf("create private state directory: %w", err)
	}
	if err := os.WriteFile(readyPath, []byte("ready\n"), 0o600); err != nil {
		return fmt.Errorf("mark startup ready: %w", err)
	}
	if c.WorkerPodUID != "" {
		path, err := workerReadyPath(c.Dir, c.WorkerPodUID)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return fmt.Errorf("create worker readiness directory: %w", err)
		}
		if err := os.WriteFile(path, []byte("ready\n"), 0o600); err != nil {
			return fmt.Errorf("mark worker ready: %w", err)
		}
	}
	interval := c.IntervalOverride
	if interval == 0 {
		// Cached, so the first periodic snapshot reuses this credential.
		creds, err := c.credentials(ctx, protocol.RoleSnapshot)
		if err != nil {
			c.Log.Error("cannot read the snapshot interval; using 15m", "error", err)
			interval = 15 * time.Minute
		} else {
			interval = creds.Interval.Duration
		}
	}
	c.Log.Info("sidecar ready", "interval", interval)

	triggers := make(chan trigger, 16)
	go c.watchRequestFiles(ctx, triggers)
	go c.waitForRequests(ctx, triggers)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// Snapshots ignore SIGTERM, so termination does not kill the engine
	// mid-write; one is cancelled only at the final deadline.
	pbase := context.WithoutCancel(ctx)
	var (
		p       *running
		queue   []trigger
		tickDue bool
		// The broker delivers a claimed request again on every poll until
		// it is complete. inflight and recent keep one request from running
		// twice: while it is queued or running, and for a short time after,
		// while the broker's cache may still show it Running.
		inflight = map[trigger]bool{}
		recent   = map[trigger]time.Time{}
	)
	var (
		lastFileRun time.Time
		fileTimer   <-chan time.Time
	)
	startNext := func() {
		if p != nil {
			return
		}
		switch {
		case len(queue) > 0:
			// Request files come from the workload. Space their snapshots,
			// so it cannot flood the repository (or push history out).
			if t := queue[0]; !t.object && t.name != "" {
				if wait := c.FileRequestSpacing - time.Since(lastFileRun); wait > 0 {
					if fileTimer == nil {
						fileTimer = time.After(wait)
					}
					return
				}
				lastFileRun = time.Now()
			}
			p = c.start(pbase, st, queue[0])
			queue = queue[1:]
		case tickDue:
			tickDue = false
			p = c.start(pbase, st, trigger{})
		}
	}
	for {
		var done <-chan runResult
		if p != nil {
			done = p.done
		}
		select {
		case <-ticker.C:
			tickDue = true
			startNext()
		case <-fileTimer:
			fileTimer = nil
			startNext()
		case t := <-triggers:
			if t.object {
				if inflight[t] || time.Since(recent[t]) < redeliveryQuiet {
					continue
				}
				inflight[t] = true
			}
			queue = append(queue, t)
			startNext()
		case r := <-done:
			p.cancel()
			p = nil
			if r.trigger.object {
				delete(inflight, r.trigger)
				for k, at := range recent {
					if time.Since(at) >= redeliveryQuiet {
						delete(recent, k)
					}
				}
				recent[r.trigger] = time.Now()
			}
			if r.err != nil {
				c.Log.Error("snapshot failed", "error", r.err, "request", r.trigger.name)
			} else {
				st = r.st
			}
			c.writeResult(r)
			startNext()
		case <-ctx.Done():
			deadline := time.Now().Add(c.FinalTimeout)
			c.Log.Info("termination requested", "deadline", deadline, "inflight", p != nil, "queued", len(queue))
			if p != nil {
				select {
				case r := <-p.done:
					if r.err == nil {
						st = r.st
					}
					c.writeResult(r)
				case <-time.After(time.Until(deadline)):
					p.cancel()
					<-p.done
				}
				p.cancel()
			}
			fctx, cancel := context.WithDeadline(pbase, deadline)
			defer cancel()
			final := c.WorkerPodUID == "" || c.workerMayFinalize.Load()
			_, id, err := c.takeSnapshot(fctx, protocol.RoleSnapshot, snapOpts{final: final}, st)
			// Queued snapshot requests are answered by the final snapshot;
			// queued restores fail, as the Pod is going away.
			for _, t := range queue {
				if t.restore {
					c.report(fctx, protocol.Report{Role: protocol.RoleRestore, Operation: protocol.OpRestore, Request: t.name,
						Error: "pod terminated before the restore started", StartTime: time.Now().UTC(), EndTime: time.Now().UTC()})
					continue
				}
				c.writeResult(runResult{trigger: t, id: id, err: err})
				if t.object {
					c.report(fctx, protocol.Report{Role: protocol.RoleSnapshot, Operation: protocol.OpSnapshot,
						SnapshotID: id, Reused: true, Request: t.name, Error: errString(err), StartTime: time.Now().UTC(), EndTime: time.Now().UTC()})
				}
			}
			return err
		}
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// trigger is one snapshot request. object is true for a WorkspaceSnapshot
// (reported to the broker), false for a request file (answered in
// .wsnap/results). The zero trigger is a periodic snapshot.
type trigger struct {
	name    string
	object  bool
	restore bool // a WorkspaceRestore; object is true too
}

type runResult struct {
	trigger trigger
	st      state
	id      string
	err     error
}

type running struct {
	done   chan runResult
	cancel context.CancelFunc
}

func (c *Config) start(base context.Context, cur state, t trigger) *running {
	ctx, cancel := context.WithCancel(base)
	p := &running{done: make(chan runResult, 1), cancel: cancel}
	go func() {
		if t.restore {
			s, id, err := c.restoreInPlace(ctx, t.name, cur)
			p.done <- runResult{trigger: t, st: s, id: id, err: err}
			return
		}
		// A request file on an unchanged volume is answered with the
		// current snapshot ID, which takeSnapshot returns without uploading.
		o := snapOpts{}
		if t.object {
			o.request = t.name
		}
		s, id, err := c.takeSnapshot(ctx, protocol.RoleSnapshot, o, cur)
		p.done <- runResult{trigger: t, st: s, id: id, err: err}
	}()
	return p
}

// resultNameOK accepts request file names that are safe as result file
// names: no path separators, no "." or "..".
var resultNameOK = func(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, "/\\")
}

// writeResult answers a request file with .wsnap/results/<name>.json.
func (c *Config) writeResult(r runResult) {
	if r.trigger.name == "" || r.trigger.object || !resultNameOK(r.trigger.name) {
		return
	}
	out := map[string]string{"snapshotID": r.id, "time": time.Now().UTC().Format(time.RFC3339)}
	if r.err != nil {
		out = map[string]string{"error": r.err.Error(), "time": out["time"]}
	}
	b, _ := json.Marshal(out)
	v, err := openVolume(c.Dir)
	if err != nil {
		c.Log.Error("cannot write request result", "error", err)
		return
	}
	defer v.Close()
	path := filepath.Join(markerPath(c.Dir, "results"), r.trigger.name+".json")
	tmp := path + ".tmp"
	if err := v.WriteFile(tmp, append(b, '\n'), 0o644); err == nil {
		_ = v.Rename(tmp, path) // readers never see a partial file
	}
}

// watchRequestFiles turns files in .wsnap/requests into triggers. Any
// process that can write to the volume can ask for a snapshot: the agent
// before a risky step, or a platform through a router and sandboxd exec.
func (c *Config) watchRequestFiles(ctx context.Context, out chan<- trigger) {
	poll := c.TriggerPoll
	if poll == 0 {
		poll = time.Second
	}
	dir := markerPath(c.Dir, "requests")
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(poll):
		}
		v, err := openVolume(c.Dir)
		if err != nil {
			c.Log.Error("not reading request files", "error", err)
			continue
		}
		entries, err := v.ReadDir(dir)
		if err != nil {
			v.Close()
			continue
		}
		var names []string
		for _, e := range entries {
			name := e.Name()
			if strings.HasPrefix(name, ".") || !resultNameOK(name) {
				continue
			}
			// Remove first: a request that arrives again later is a new one.
			if err := v.Remove(filepath.Join(dir, name)); err != nil {
				continue
			}
			names = append(names, name)
		}
		v.Close()
		for _, name := range names {
			select {
			case out <- trigger{name: name}:
			case <-ctx.Done():
				return
			}
		}
	}
}

// waitForRequests long-polls the broker for WorkspaceSnapshot requests.
func (c *Config) waitForRequests(ctx context.Context, out chan<- trigger) {
	for ctx.Err() == nil {
		var resp protocol.WaitResponse
		if err := c.postOnce(ctx, "/v1/wait", struct{}{}, &resp); err != nil {
			if ctx.Err() == nil {
				c.Log.Debug("wait for requests failed", "error", err)
				select {
				case <-ctx.Done():
				case <-time.After(5 * time.Second):
				}
			}
			continue
		}
		for _, name := range resp.Requests {
			select {
			case out <- trigger{name: name, object: true}:
			case <-ctx.Done():
				return
			}
		}
		for _, name := range resp.Restores {
			select {
			case out <- trigger{name: name, object: true, restore: true}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (c *Config) initializeWithRetry(ctx context.Context) (state, error) {
	backoff := time.Second
	for {
		st, err := c.initialize(ctx)
		if err == nil {
			// A warm volume skips restore, but it must still claim the writer
			// role before the startup probe may release workload containers.
			// The cache is empty in a new process, so this always reaches
			// the broker, which claims the writer role.
			_, err = c.credentials(ctx, protocol.RoleSnapshot)
		}
		if err == nil {
			return st, nil
		}
		wait := backoff
		var be *brokerError
		if errors.As(err, &be) && be.status == http.StatusConflict {
			// 409 means "wait for another writer or a rescue", which ends
			// at a moment we cannot see. Poll steadily so the restore
			// starts within seconds of it, instead of backing off to a
			// minute.
			wait = conflictPoll
			c.Log.Info("waiting before restore", "reason", be.body, "in", wait)
		} else {
			c.Log.Error("initialization failed; retrying", "error", err, "in", wait)
			backoff = min(backoff*2, time.Minute)
		}
		select {
		case <-ctx.Done():
			return state{}, ctx.Err()
		case <-time.After(wait):
		}
	}
}

// RunMover takes one final snapshot of a volume whose Pod is gone.
//
// A volume whose first restore was cut off holds no workspace data of its
// own: the workload only starts after initialization. Snapshotting it
// would store a partial tree as the newest snapshot. So the mover saves
// nothing (the rescue still completes and the PV is reclaimed), and a
// WorkspaceSnapshot request fails with the reason. Any other doubt, such as
// an error while checking, fails the Job: the controller then keeps the PV
// (RescueFailed) instead of reclaiming data that was never saved.
func (c *Config) RunMover(ctx context.Context) error {
	c.StrictReports = true
	// An in-place restore cut off mid-swap: finish it, so the volume holds
	// whole trees. The pre-restore data was snapshotted before the swap.
	if err := c.recoverSwap(); err != nil {
		return fmt.Errorf("finish an interrupted swap: %w", err)
	}
	reason, err := c.notWorkspaceData()
	if err != nil {
		return fmt.Errorf("check the volume: %w", err)
	}
	if reason != "" {
		c.Log.Info("nothing to save", "reason", reason)
		now := time.Now().UTC()
		rep := protocol.Report{Role: protocol.RoleMover, Operation: protocol.OpSnapshot, Request: c.Request,
			NoData: true, Final: true, StartTime: now, EndTime: now}
		if c.Request != "" {
			rep.Error = "volume not snapshotted: " + reason
		}
		// The controller frees the volume only after the broker marks it.
		return c.report(ctx, rep)
	}
	_, _, err = c.takeSnapshot(ctx, protocol.RoleMover, snapOpts{final: true, request: c.Request}, state{})
	return err
}

// notWorkspaceData explains why the volume holds no workspace data, or
// returns "". It returns an error when it cannot tell.
func (c *Config) notWorkspaceData() (string, error) {
	v, err := openVolume(c.Dir)
	if err != nil {
		return "", err
	}
	defer v.Close()
	exists := func(name string) (bool, error) {
		_, err := v.Lstat(markerPath(c.Dir, name))
		if os.IsNotExist(err) {
			return false, nil
		}
		return err == nil, err
	}
	restoring, err := exists("restoring")
	if err != nil {
		return "", err
	}
	initialized, err := exists("initialized")
	if err != nil {
		return "", err
	}
	// "restoring" with "initialized" is a crash between the two marker
	// writes: the restore finished and the workload may have run.
	if restoring && !initialized {
		return "the first restore was interrupted, so the volume holds a partial tree", nil
	}
	// Not the "initialized" marker: the workload can delete .wsnap (for
	// example with "git clean -fdx"), and its data would then be lost.
	empty, err := isEmpty(v, c.Dir)
	if err != nil {
		return "", err
	}
	if empty {
		return "the volume is empty", nil
	}
	return "", nil
}

// RunMaintenance applies retention.
func (c *Config) RunMaintenance(ctx context.Context) error {
	start := time.Now().UTC()
	rep := protocol.Report{Role: protocol.RoleMaintenance, Operation: protocol.OpMaintenance, StartTime: start}
	creds, eng, t, err := c.open(ctx, protocol.RoleMaintenance)
	if err == nil {
		err = eng.Maintain(ctx, t, engine.Retention{KeepLast: creds.KeepLast, KeepDaily: creds.KeepDaily, Keep: creds.KeepIDs})
	}
	rep.EndTime = time.Now().UTC()
	if err != nil {
		rep.Error = err.Error()
	}
	c.report(ctx, rep)
	return err
}

// restoreInPlace replaces the live volume contents with a snapshot chosen
// by WorkspaceRestore request. First it snapshots the live data, so a
// rollback never destroys unsaved work. After the swap it snapshots the
// result, so the newest snapshot matches the volume again; without that, a
// later restore onto a fresh volume would bring back the pre-restore
// state. The request is reported only after that, so Succeeded means the
// restored state is durable.
func (c *Config) restoreInPlace(ctx context.Context, request string, cur state) (state, string, error) {
	if c.QuiesceTimeout > 0 {
		thaw := c.quiesce(ctx)
		defer thaw()
	}
	// A restore that already ran (its report was lost, so the broker
	// delivers it again) must not run twice: that would roll back work done
	// since. Send the saved report again instead.
	if done, ok := c.loadDone(request); ok {
		c.Log.Info("restore already done; reporting it again", "request", request, "id", done.SnapshotID)
		c.report(context.WithoutCancel(ctx), done)
		return state{}, done.SnapshotID, nil
	}
	start := time.Now().UTC()
	rep := protocol.Report{Role: protocol.RoleRestore, Operation: protocol.OpRestore, Request: request, StartTime: start}
	finish := func(st state, id string, err error) (state, string, error) {
		rep.EndTime, rep.SnapshotID = time.Now().UTC(), id
		if err != nil {
			rep.Error = err.Error()
		} else {
			c.saveDone(request, rep)
		}
		c.report(context.WithoutCancel(ctx), rep)
		return st, id, err
	}
	creds, err := c.credentialsFor(ctx, protocol.CredentialsRequest{Role: protocol.RoleRestore, Request: request})
	if err != nil {
		return finish(cur, "", err)
	}
	rep.SourceWorkspace = creds.SourceWorkspace
	id, err := c.restoreAndSwap(ctx, creds, func() error {
		var err error
		if cur, _, err = c.takeSnapshot(ctx, protocol.RoleSnapshot, snapOpts{noQuiesce: true, complete: true}, cur); err != nil {
			return fmt.Errorf("could not save the live data before the restore: %w", err)
		}
		return nil
	})
	if err != nil {
		return finish(cur, id, err)
	}
	c.Log.Info("restored in place", "id", id, "from", creds.SourceWorkspace, "request", request)
	// The restored snapshot is from another repository (fork) or older than
	// latest (rollback), so always upload: an invalid state forces it.
	st, _, err := c.takeSnapshot(ctx, protocol.RoleSnapshot, snapOpts{noQuiesce: true}, state{})
	if err != nil {
		return finish(state{}, id, fmt.Errorf("restored %s but could not snapshot the result: %w", id, err))
	}
	return finish(st, id, nil)
}

// doneDir holds one file per restore request this Pod completed, on the
// sidecar's private volume, which survives container restarts.
func (c *Config) doneFile(request string) string {
	return filepath.Join(c.StateDir, "done", request+".json")
}

func (c *Config) saveDone(request string, rep protocol.Report) {
	b, _ := json.Marshal(rep)
	if err := os.MkdirAll(filepath.Dir(c.doneFile(request)), 0o700); err == nil {
		err = os.WriteFile(c.doneFile(request), b, 0o600)
	}
}

func (c *Config) loadDone(request string) (protocol.Report, bool) {
	var rep protocol.Report
	b, err := os.ReadFile(c.doneFile(request))
	return rep, err == nil && json.Unmarshal(b, &rep) == nil
}

// restoreAndSwap downloads the snapshot into .wsnap/staging, calls
// beforeSwap (the safety snapshot of the live data), then swaps. The
// safety snapshot runs after the download, so writes made while the
// snapshot downloads are in it too.
func (c *Config) restoreAndSwap(ctx context.Context, creds *protocol.CredentialsResponse, beforeSwap func() error) (string, error) {
	ctx, cancel := c.withOperationTimeout(ctx)
	defer cancel()
	eng, err := engine.New(creds.Engine)
	if err != nil {
		return "", err
	}
	t := engine.Target{Backend: creds.Backend, Credentials: creds.Credentials, Password: creds.RepositoryPassword,
		Workspace: creds.Workspace, StateDir: c.StateDir, ReadOnly: true}
	if err := eng.Init(ctx, t); err != nil {
		return "", err
	}
	id := cmp.Or(creds.SnapshotID, creds.LatestSnapshotID)
	if id == "" {
		latest, err := eng.Latest(ctx, t)
		if err != nil {
			return "", err
		}
		if latest == nil {
			return "", fmt.Errorf("the repository has no snapshot to restore")
		}
		id = latest.ID
	}
	v, err := openVolume(c.Dir)
	if err != nil {
		return "", err
	}
	defer v.Close()
	staging := markerPath(c.Dir, "staging")
	if err := v.RemoveAll(staging); err != nil {
		return "", err
	}
	if err := v.MkdirAll(staging, 0o755); err != nil {
		return "", err
	}
	if err := v.plainDir(staging); err != nil {
		return "", err
	}
	if err := eng.Restore(ctx, t, id, staging); err != nil {
		return "", err
	}
	if err := beforeSwap(); err != nil {
		return "", err
	}
	return id, c.swap()
}

// swap exchanges the live contents for .wsnap/staging in two phases
// recorded in .wsnap/swap, so recoverSwap can finish an interrupted swap:
// phase 1 moves live entries to .wsnap/trash, phase 2 moves staged
// entries into place. No entry is ever lost or duplicated, whatever the
// crash point.
func (c *Config) swap() error {
	v, err := openVolume(c.Dir)
	if err != nil {
		return err
	}
	defer v.Close()
	marker := markerPath(c.Dir, "swap")
	trash := markerPath(c.Dir, "trash")
	if err := v.RemoveAll(trash); err != nil {
		return err
	}
	if err := v.MkdirAll(trash, 0o755); err != nil {
		return err
	}
	syncDisks() // the staged tree is on disk before the swap starts
	if err := v.WriteFile(marker, []byte("1\n"), 0o644); err != nil {
		return err
	}
	return c.recoverSwap()
}

// swapPending reports whether .wsnap/swap exists. An error counts as
// pending, so the caller takes the safe path.
func (c *Config) swapPending() bool {
	v, err := openVolume(c.Dir)
	if err != nil {
		return true
	}
	defer v.Close()
	_, err = v.Lstat(markerPath(c.Dir, "swap"))
	return !os.IsNotExist(err)
}

// recoverSwap finishes a swap from whichever phase it reached.
func (c *Config) recoverSwap() error {
	v, err := openVolume(c.Dir)
	if err != nil {
		return err
	}
	defer v.Close()
	marker := markerPath(c.Dir, "swap")
	b, err := v.ReadFile(marker)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	staging, trash := markerPath(c.Dir, "staging"), markerPath(c.Dir, "trash")
	if strings.TrimSpace(string(b)) == "1" {
		if err := moveEntries(v, c.Dir, trash, true); err != nil {
			return fmt.Errorf("swap phase 1: %w", err)
		}
		if err := v.WriteFile(marker, []byte("2\n"), 0o644); err != nil {
			return err
		}
	}
	// The workload may have re-created a name since phase 1; move such an
	// entry aside first, or the rename fails (non-empty directory) and the
	// swap stops halfway.
	if err := clearCollisions(v, staging, c.Dir, trash); err != nil {
		return fmt.Errorf("swap phase 2: %w", err)
	}
	if err := moveEntries(v, staging, c.Dir, false); err != nil {
		return fmt.Errorf("swap phase 2: %w", err)
	}
	syncDisks() // the renames are on disk before the marker goes
	if err := v.Remove(marker); err != nil {
		return err
	}
	_ = v.RemoveAll(staging)
	return v.RemoveAll(trash)
}

// moveEntries renames every entry of src into dst. fromLive skips the
// entries that never belong to the workspace.
func moveEntries(v *volume, src, dst string, fromLive bool) error {
	entries, err := v.ReadDir(src)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		if fromLive && (e.Name() == engine.StateDirName || e.Name() == "lost+found") {
			continue
		}
		if err := v.Rename(filepath.Join(src, e.Name()), filepath.Join(dst, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// clearCollisions moves entries of dst that also exist in src into trash
// under a unique name.
func clearCollisions(v *volume, src, dst, trash string) error {
	entries, err := v.ReadDir(src)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for i, e := range entries {
		target := filepath.Join(dst, e.Name())
		if _, err := v.Lstat(target); err == nil {
			aside := filepath.Join(trash, fmt.Sprintf("collision-%d-%s", i, e.Name()))
			if err := v.Rename(target, aside); err != nil {
				return err
			}
		}
	}
	return nil
}

// RunRestorer restores an idle volume for a WorkspaceRestore from a Job.
// Like restoreInPlace, it saves the volume's data first, and reports the
// request only after the restored state is in a snapshot.
func (c *Config) RunRestorer(ctx context.Context) error {
	c.StrictReports = true
	if err := c.recoverSwap(); err != nil {
		return err
	}
	start := time.Now().UTC()
	rep := protocol.Report{Role: protocol.RoleRestore, Operation: protocol.OpRestore, Request: c.Request, StartTime: start}
	finish := func(id string, err error) error {
		rep.EndTime, rep.SnapshotID = time.Now().UTC(), id
		if err != nil {
			rep.Error = err.Error()
		}
		return cmp.Or(err, c.report(context.WithoutCancel(ctx), rep))
	}
	creds, err := c.credentialsFor(ctx, protocol.CredentialsRequest{Role: protocol.RoleRestore, Request: c.Request})
	if err != nil {
		return finish("", err)
	}
	rep.SourceWorkspace = creds.SourceWorkspace
	id, err := c.restoreAndSwap(ctx, creds, func() error {
		// The last Pod may have stopped without a final snapshot.
		reason, err := c.notWorkspaceData()
		if err != nil {
			return fmt.Errorf("check the volume: %w", err)
		}
		if reason == "" {
			if _, _, err := c.takeSnapshot(ctx, protocol.RoleMover, snapOpts{complete: true}, state{}); err != nil {
				return fmt.Errorf("could not save the volume before the restore: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return finish(id, err)
	}
	// Keep the volume's identity: the next Pod must not restore over it.
	v, err := openVolume(c.Dir)
	if err != nil {
		return finish(id, err)
	}
	err = v.WriteFile(MarkerPath(c.Dir), []byte(time.Now().UTC().Format(time.RFC3339)+"\n"), 0o644)
	v.Close()
	if err != nil {
		return finish(id, err)
	}
	c.Log.Info("restored idle volume", "id", id, "request", c.Request)
	if _, _, err := c.takeSnapshot(ctx, protocol.RoleMover, snapOpts{final: true}, state{}); err != nil {
		return finish(id, fmt.Errorf("restored %s but could not snapshot the result: %w", id, err))
	}
	return finish(id, nil)
}
