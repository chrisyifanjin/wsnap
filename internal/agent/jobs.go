package agent

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/chrisyifanjin/wsnap/internal/engine"
	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

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
