// Package agent is the wsnap agent. One binary runs as an in-pod sidecar
// (restore, periodic snapshots, final snapshot on SIGTERM) or as a Job
// (mover, maintenance).
package agent

import (
	"bytes"
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
	// OperationTimeout bounds one snapshot or restore. Engines retry
	// backend errors for a long time (restic up to 15 minutes), which would
	// hold the request queue during a storage outage.
	OperationTimeout time.Duration
	HTTP             *http.Client
	Log              *slog.Logger

	mu    sync.Mutex
	cache map[protocol.Role]cachedCredentials
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

// StartupReady is checked by the sidecar's startup probe. Its marker is on
// the sidecar's private volume, so a marker left on a reused workspace volume
// cannot start the workload before this Pod is accepted as its writer.
func StartupReady(stateDir string) error {
	_, err := os.Stat(filepath.Join(stateDir, "startup-ready"))
	return err
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
	if cacheable {
		until := time.Now().Add(unexpiringReuse)
		if exp := resp.Credentials.ExpiresAt; !exp.IsZero() {
			// An operation started with this credential may run for
			// OperationTimeout; it must not expire halfway.
			until = exp.Add(-max(credentialMargin, c.OperationTimeout+credentialMargin))
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
	return &resp, nil
}

// invalidate drops a cached credential after a failure, in case the
// failure came from a revoked or misconfigured credential.
func (c *Config) invalidate(role protocol.Role) {
	c.mu.Lock()
	delete(c.cache, role)
	c.mu.Unlock()
}

func (c *Config) report(ctx context.Context, r protocol.Report) {
	// A lost report delays bookkeeping only. For a final snapshot, the
	// controller then rescues the volume, which is safe but slower.
	if err := c.post(ctx, "/v1/report", r, nil); err != nil {
		c.Log.Error("report failed", "operation", r.Operation, "error", err)
	}
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
	}
	if !creds.Credentials.Scoped {
		c.Log.Warn("broker issued an unscoped credential; it can reach every workspace in the repository", "role", role)
	}
	return creds, eng, t, eng.Init(ctx, t)
}

func (c *Config) withOperationTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.OperationTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, c.OperationTimeout)
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
		c.report(ctx, protocol.Report{Role: role, Operation: protocol.OpSnapshot, SnapshotID: st.id,
			Final: o.final, Reused: true, Request: o.request, StartTime: start, EndTime: time.Now().UTC()})
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
	c.report(context.WithoutCancel(ctx), rep)
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
	if err := c.recoverSwap(); err != nil {
		return state{}, err
	}
	v, err := openVolume(c.Dir)
	if err != nil {
		return state{}, err
	}
	defer v.Close()
	if Ready(c.Dir) == nil {
		c.Log.Info("volume already initialized; skipping restore")
		return state{}, c.prepareTriggerDirs()
	}
	restoring := markerPath(c.Dir, "restoring")
	if _, err := v.Lstat(restoring); err == nil {
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
	if err := v.WriteFile(MarkerPath(c.Dir), []byte(time.Now().UTC().Format(time.RFC3339)+"\n"), 0o644); err != nil {
		return state{}, err
	}
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
	case errors.Is(err, engine.ErrRepositoryNotFound) && creds.SnapshotID == "" && creds.SourceWorkspace == "":
		err = nil // a new workspace
	case err == nil && creds.SnapshotID != "":
		id = creds.SnapshotID
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

// RunSidecar initializes the volume, then serves three kinds of snapshot
// one at a time: periodic ones, requests (WorkspaceSnapshot objects through
// the broker, and files in .wsnap/requests), and on SIGTERM a final one
// after any in-flight snapshot finishes.
func (c *Config) RunSidecar(ctx context.Context) error {
	readyPath := filepath.Join(c.StateDir, "startup-ready")
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
	)
	startNext := func() {
		if p != nil {
			return
		}
		switch {
		case len(queue) > 0:
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
		case t := <-triggers:
			queue = append(queue, t)
			startNext()
		case r := <-done:
			p.cancel()
			p = nil
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
			_, id, err := c.takeSnapshot(fctx, protocol.RoleSnapshot, snapOpts{final: true}, st)
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
// A volume that was never initialized, or is in the middle of a restore,
// holds no workspace data of its own: the workload only starts after
// initialization. Snapshotting it would store an empty or partial tree as
// the newest snapshot, and the next restore would bring that back. So the
// mover saves nothing (the rescue still completes and the PV is
// reclaimed), and a WorkspaceSnapshot request fails with the reason.
func (c *Config) RunMover(ctx context.Context) error {
	if reason := c.notWorkspaceData(); reason != "" {
		c.Log.Info("nothing to save", "reason", reason)
		if c.Request != "" {
			now := time.Now().UTC()
			c.report(ctx, protocol.Report{Role: protocol.RoleMover, Operation: protocol.OpSnapshot, Request: c.Request,
				Error: "volume not snapshotted: " + reason, StartTime: now, EndTime: now})
		}
		return nil
	}
	_, _, err := c.takeSnapshot(ctx, protocol.RoleMover, snapOpts{final: true, request: c.Request}, state{})
	return err
}

// notWorkspaceData explains why the volume holds no workspace data, or
// returns "".
func (c *Config) notWorkspaceData() string {
	v, err := openVolume(c.Dir)
	if err != nil {
		return err.Error()
	}
	defer v.Close()
	if _, err := v.Stat(markerPath(c.Dir, "restoring")); err == nil {
		return "a restore was interrupted, so the volume holds a partial tree"
	}
	if _, err := v.Stat(markerPath(c.Dir, "swap")); err == nil {
		return "an in-place restore was interrupted mid-swap"
	}
	if _, err := v.Lstat(MarkerPath(c.Dir)); err != nil {
		return "the volume was never initialized, so no workload ever ran on it"
	}
	return ""
}

// RunMaintenance applies retention.
func (c *Config) RunMaintenance(ctx context.Context) error {
	start := time.Now().UTC()
	rep := protocol.Report{Role: protocol.RoleMaintenance, Operation: protocol.OpMaintenance, StartTime: start}
	creds, eng, t, err := c.open(ctx, protocol.RoleMaintenance)
	if err == nil {
		err = eng.Maintain(ctx, t, creds.KeepLast)
	}
	rep.EndTime = time.Now().UTC()
	if err != nil {
		rep.Error = err.Error()
	}
	c.report(ctx, rep)
	return err
}

// restoreInPlace replaces the live volume contents with a snapshot chosen
// by WorkspaceRestore request, then snapshots the result so the
// repository's latest snapshot matches the volume again. Without that, a
// later restore onto a fresh volume would bring back the pre-restore state.
func (c *Config) restoreInPlace(ctx context.Context, request string, cur state) (state, string, error) {
	if c.QuiesceTimeout > 0 {
		thaw := c.quiesce(ctx)
		defer thaw()
	}
	id, source, err := c.restoreRequest(ctx, request)
	if err != nil {
		return cur, "", err
	}
	c.Log.Info("restored in place", "id", id, "from", source, "request", request)
	// The restored snapshot is from another repository (fork) or older than
	// latest (rollback), so always upload: an invalid state forces it.
	st, _, err := c.takeSnapshot(ctx, protocol.RoleSnapshot, snapOpts{noQuiesce: true}, state{})
	if err != nil {
		return state{}, id, fmt.Errorf("restored %s but could not snapshot the result: %w", id, err)
	}
	return st, id, nil
}

// restoreRequest restores the snapshot a WorkspaceRestore names into the
// volume with a crash-safe swap, and reports the outcome.
func (c *Config) restoreRequest(ctx context.Context, request string) (string, string, error) {
	start := time.Now().UTC()
	rep := protocol.Report{Role: protocol.RoleRestore, Operation: protocol.OpRestore, Request: request, StartTime: start}
	creds, err := c.credentialsFor(ctx, protocol.CredentialsRequest{Role: protocol.RoleRestore, Request: request})
	var id string
	if err == nil {
		rep.SourceWorkspace = creds.SourceWorkspace
		id, err = c.restoreAndSwap(ctx, creds)
	}
	rep.EndTime = time.Now().UTC()
	rep.SnapshotID = id
	if err != nil {
		rep.Error = err.Error()
	}
	c.report(context.WithoutCancel(ctx), rep)
	return id, rep.SourceWorkspace, err
}

func (c *Config) restoreAndSwap(ctx context.Context, creds *protocol.CredentialsResponse) (string, error) {
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
	id := creds.SnapshotID
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
	if err := v.WriteFile(marker, []byte("1\n"), 0o644); err != nil {
		return err
	}
	return c.recoverSwap()
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

// RunRestorer restores an idle volume for a WorkspaceRestore, then
// snapshots the result, from a Job.
func (c *Config) RunRestorer(ctx context.Context) error {
	if err := c.recoverSwap(); err != nil {
		return err
	}
	id, _, err := c.restoreRequest(ctx, c.Request)
	if err != nil {
		return err
	}
	// Keep the volume's identity: the next Pod must not restore over it.
	v, err := openVolume(c.Dir)
	if err != nil {
		return err
	}
	err = v.WriteFile(MarkerPath(c.Dir), []byte(time.Now().UTC().Format(time.RFC3339)+"\n"), 0o644)
	v.Close()
	if err != nil {
		return err
	}
	c.Log.Info("restored idle volume", "id", id, "request", c.Request)
	_, _, err = c.takeSnapshot(ctx, protocol.RoleMover, snapOpts{final: true}, state{})
	return err
}
