package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

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
		if err := c.heartbeatWorkerReady(ctx); err != nil {
			return err
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
