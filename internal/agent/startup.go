package agent

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/chrisyifanjin/wsnap/internal/engine"
	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

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
