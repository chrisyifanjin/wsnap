package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/chrisyifanjin/wsnap/internal/engine"
	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

// The worker refreshes the gate's marker every workerHeartbeat; the gate
// accepts a marker younger than workerFresh. A marker left from before a
// node reboot is stale, so the restarted workload waits for its worker.
var (
	workerHeartbeat = 5 * time.Second
	workerFresh     = 20 * time.Second
)

func workerReadyPath(dir, podUID string) (string, error) {
	if podUID == "" || podUID == "." || filepath.Base(podUID) != podUID {
		return "", fmt.Errorf("invalid workload Pod UID %q", podUID)
	}
	return filepath.Join(dir, engine.StateDirName, "worker-ready", podUID), nil
}

// WaitWorker blocks a workload's first init container until its separate
// worker has restored the PVC, claimed this Pod as writer, and is alive.
func WaitWorker(ctx context.Context, dir, podUID string) error {
	path, err := workerReadyPath(dir, podUID)
	if err != nil {
		return err
	}
	rel, _ := filepath.Rel(dir, path)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		// Through os.Root: a planted symlink cannot point the gate at a
		// fresh file elsewhere. The volume may not exist yet as a root.
		if root, err := os.OpenRoot(dir); err == nil {
			info, err := root.Stat(rel)
			root.Close()
			if err == nil && time.Since(info.ModTime()) < workerFresh {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// heartbeatWorkerReady writes the gate's marker now and then every
// workerHeartbeat until ctx ends. Through os.Root, so a symlink the
// workload planted cannot lead the root agent out of the volume.
func (c *Config) heartbeatWorkerReady(ctx context.Context) error {
	path, err := workerReadyPath(c.Dir, c.WorkerPodUID)
	if err != nil {
		return err
	}
	v, err := openVolume(c.Dir)
	if err != nil {
		return err
	}
	write := func() error {
		if err := v.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return fmt.Errorf("create worker readiness directory: %w", err)
		}
		return v.WriteFile(path, []byte(time.Now().UTC().Format(time.RFC3339)+"\n"), 0o644)
	}
	if err := write(); err != nil {
		v.Close()
		return fmt.Errorf("mark worker ready: %w", err)
	}
	go func() {
		defer v.Close()
		t := time.NewTicker(workerHeartbeat)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := write(); err != nil {
					c.Log.Warn("cannot refresh the worker readiness marker", "error", err)
				}
			}
		}
	}()
	return nil
}

// workerPoll is how often a worker asks whether its workload Pod stopped.
var workerPoll = 3 * time.Second

// RunWorker runs the sidecar's snapshot loop in a separate Pod. The broker
// tells it when its workload Pod has stopped; only then may it mark a
// snapshot final. If the workload Pod is gone without being seen stopped
// (a forced deletion), its containers may still run for their grace
// period, so the worker waits that long first. A drain evicts both Pods at
// once, so on SIGTERM the worker waits up to the workload's grace for it
// to stop; if it still runs, the last snapshot is not final, and the
// volume stays protected for the next Pod or a rescue.
func (c *Config) RunWorker(ctx context.Context) error {
	if _, err := workerReadyPath(c.Dir, c.WorkerPodUID); err != nil {
		return err
	}
	wctx, stop := context.WithCancel(context.WithoutCancel(ctx))
	defer stop()
	go c.watchWorkload(ctx, wctx, stop)
	return c.RunSidecar(wctx)
}

// watchWorkload calls stop when the workload has stopped (allowing a final
// snapshot), or when term is cancelled and the workload does not stop in
// time (not final).
func (c *Config) watchWorkload(term, run context.Context, stop context.CancelFunc) {
	ticker := time.NewTicker(workerPoll)
	defer ticker.Stop()
	finalize := func() {
		c.workerMayFinalize.Store(true)
		stop()
	}
	grace := c.WorkloadGrace
	if grace <= 0 {
		grace = 30 * time.Second // the Kubernetes default
	}
	terminated := term.Done()
	var giveUp, settled <-chan time.Time
	for {
		var state protocol.WorkerState
		pctx, cancel := context.WithTimeout(run, 10*time.Second)
		err := c.post(pctx, "/v1/worker-state", struct{}{}, &state)
		cancel()
		switch {
		case err == nil && state.Stop && !state.Gone:
			finalize()
			return
		case err == nil && state.Gone && settled == nil:
			c.Log.Info("the workload Pod is gone; waiting its grace period for its containers to stop", "grace", grace)
			settled = time.After(grace)
		case err != nil && run.Err() == nil:
			c.Log.Warn("cannot read workload state", "error", err)
		}
		select {
		case <-run.Done():
			return
		case <-settled:
			finalize()
			return
		case <-terminated:
			terminated = nil
			giveUp = time.After(grace)
			c.Log.Info("termination requested; waiting for the workload Pod to stop", "limit", grace)
		case <-giveUp:
			c.Log.Warn("the workload Pod still runs; the last snapshot is not final")
			stop()
			return
		case <-ticker.C:
		}
	}
}
