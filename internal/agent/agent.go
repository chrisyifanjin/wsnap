// Package agent is the wsnap agent. One binary runs as an in-pod sidecar
// (restore, periodic snapshots, final snapshot on SIGTERM), as a separate
// worker Pod beside the workload, or as a Job (mover, restorer,
// maintenance). It has no Kubernetes client: everything it learns about
// the cluster comes from the broker.
//
// Files by responsibility:
//
//   - client.go: talking to the broker (credentials, reports, retries);
//   - startup.go: initializing a volume (restore, or adopt existing data);
//   - snapshot.go: taking one snapshot (quiesce, engine, report);
//   - sidecar.go: the sidecar loop (schedule, requests, final snapshot);
//   - worker.go: worker mode (readiness gate, workload state);
//   - swap.go: restoring into a live volume and recovering a cut swap;
//   - jobs.go: mover, restorer and maintenance Jobs;
//   - volume.go: file operations confined to the volume (os.Root).
package agent

import (
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
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
	WorkerPodUID string
	// WorkloadGrace is how long the workload's containers may run after
	// its Pod is deleted (its terminationGracePeriodSeconds).
	WorkloadGrace     time.Duration
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
