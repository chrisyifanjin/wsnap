package agent

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A root workload plants .wsnap/worker-ready as a symlink. The root worker
// must not follow it out of the volume when it writes the gate's marker.
func TestWorkerMarkerDoesNotFollowSymlinks(t *testing.T) {
	c, _ := setup(t)
	outside := t.TempDir()
	write(t, filepath.Join(c.Dir, "data.txt"), "x")
	write(t, filepath.Join(c.Dir, ".wsnap", "initialized"), "x")
	if err := os.Symlink(outside, filepath.Join(c.Dir, ".wsnap", "worker-ready")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.WorkerPodUID = "pod-uid-1"
	errc := make(chan error, 1)
	go func() { errc <- c.RunWorker(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Lstat(filepath.Join(outside, "pod-uid-1")); err == nil {
			t.Errorf("worker wrote its readiness marker outside the volume: %s", filepath.Join(outside, "pod-uid-1"))
			return
		}
		select {
		case err := <-errc:
			t.Logf("RunWorker returned: %v", err)
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Log("marker not written outside")
}
