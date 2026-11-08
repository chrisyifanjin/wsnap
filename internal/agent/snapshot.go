package agent

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/chrisyifanjin/wsnap/internal/engine"
	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

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

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
