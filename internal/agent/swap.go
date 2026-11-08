package agent

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/chrisyifanjin/wsnap/internal/engine"
	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

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
