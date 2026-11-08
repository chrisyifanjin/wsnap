// Package engine wraps snapshot technologies behind one interface. Each
// engine drives its own CLI (kopia, restic, or a user-supplied command).
package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

// StateDirName is the directory inside a workspace volume where the agent
// keeps its own marker files. Engines exclude it from snapshots.
const StateDirName = ".wsnap"

// excludedNames are never part of a snapshot. lost+found exists on every
// fresh ext4 volume and belongs to the filesystem, not the workspace.
var excludedNames = []string{StateDirName, "lost+found"}

// ErrRepositoryNotFound is returned by a read-only Init when the repository
// does not exist yet, which means the workspace has no snapshots.
var ErrRepositoryNotFound = errors.New("repository not found")

// Target is one workspace repository plus what is needed to open it.
type Target struct {
	Backend     protocol.Backend
	Credentials protocol.Credentials
	Password    string
	// Workspace identifies the snapshot source inside the repository.
	Workspace string
	// StateDir holds engine config and caches. It must be writable and must
	// not be inside the snapshotted directory.
	StateDir string
	// ReadOnly opens the repository without writing anything, as the
	// restore role requires.
	ReadOnly bool
	// Maintenance marks the maintenance role. Engines with an owner
	// concept use a separate identity for it.
	Maintenance bool
}

// Snapshot identifies one snapshot.
type Snapshot struct {
	ID   string
	Time time.Time
	// Partial means some files could not be read and are missing from the
	// snapshot (Kopia's ignored errors, restic's exit code 3). Such a
	// snapshot is kept, but never counts as final: the volume still holds
	// data that is in no snapshot.
	Partial bool
}

// Engine is a snapshot technology.
type Engine interface {
	// Init opens the repository. It creates the repository when it does not
	// exist yet and the target is writable. A read-only Init on a missing
	// repository returns an error wrapping ErrRepositoryNotFound.
	Init(ctx context.Context, t Target) error
	// Snapshot stores the contents of dir.
	Snapshot(ctx context.Context, t Target, dir string) (Snapshot, error)
	// Latest returns the newest snapshot, or nil when there is none.
	Latest(ctx context.Context, t Target) (*Snapshot, error)
	// Restore writes snapshot id into dir, which should be empty.
	Restore(ctx context.Context, t Target, id, dir string) error
	// Maintain applies the retention policy: it deletes the snapshots the
	// policy does not keep and frees their storage.
	Maintain(ctx context.Context, t Target, p Retention) error
	// WriterDeletePrefixes lists repository sub-prefixes that a writer must
	// be able to delete during a normal snapshot. Credential providers allow
	// delete on these prefixes only.
	WriterDeletePrefixes() []string
}

// New returns the engine for spec.
func New(spec protocol.Engine) (Engine, error) {
	switch spec.Type {
	case protocol.EngineKopia:
		return &Kopia{Binary: "kopia"}, nil
	case protocol.EngineRestic:
		return &Restic{Binary: "restic"}, nil
	case protocol.EngineExec:
		if len(spec.Command) == 0 {
			return nil, fmt.Errorf("exec engine needs a command")
		}
		return &Exec{Command: spec.Command}, nil
	}
	return nil, fmt.Errorf("unknown engine %q", spec.Type)
}

// WriterDeletePrefixes returns the writer delete prefixes of an engine type
// without constructing the engine.
func WriterDeletePrefixes(spec protocol.Engine) []string {
	e, err := New(spec)
	if err != nil {
		return nil
	}
	return e.WriterDeletePrefixes()
}

// cmdError keeps the tail of stderr, which is where the CLIs explain what
// went wrong.
type cmdError struct {
	args     []string
	exitCode int
	stderr   string
}

func (e *cmdError) Error() string {
	return fmt.Sprintf("%s exited with %d: %s", strings.Join(e.args, " "), e.exitCode, e.stderr)
}

// run executes a CLI with a clean environment. The agent's own environment
// is not inherited, so stray AWS_* or GOOGLE_* variables cannot change which
// repository or identity a command uses.
func run(ctx context.Context, dir string, env []string, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = append(baseEnv(), env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		code := -1
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		}
		return stdout.Bytes(), &cmdError{args: append([]string{name}, args...), exitCode: code, stderr: tail(stderr.String(), 2000)}
	}
	return stdout.Bytes(), nil
}

func baseEnv() []string {
	var env []string
	// STORAGE_EMULATOR_HOST points Google's storage client (used by both
	// kopia and restic) at an emulator; it is unset in production.
	for _, k := range []string{"PATH", "TMPDIR", "SSL_CERT_FILE", "SSL_CERT_DIR", "HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY", "STORAGE_EMULATOR_HOST"} {
		if v, ok := os.LookupEnv(k); ok {
			env = append(env, k+"="+v)
		}
	}
	return env
}

func tail(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) > n {
		return "..." + s[len(s)-n:]
	}
	return s
}

func exitCode(err error) int {
	if ce, ok := err.(*cmdError); ok {
		return ce.exitCode
	}
	return -1
}

// repoKey names a repository location for per-repository caches.
func repoKey(b protocol.Backend) string {
	raw, _ := json.Marshal(b)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:8])
}

// writeSecretFile writes a credential file readable only by the agent.
func writeSecretFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}

// Retention says which snapshots maintenance keeps: the newest KeepLast,
// the newest of each of the last KeepDaily days (UTC), and every snapshot
// in Keep. A Keep entry may be a prefix of an ID (a short ID).
type Retention struct {
	KeepLast  int
	KeepDaily int
	Keep      []string
}

type snapRef struct {
	id string
	t  time.Time
}

// forgettable returns the IDs to delete from snaps (oldest first).
func forgettable(snaps []snapRef, p Retention, now time.Time) []string {
	keepLast := max(p.KeepLast, 1)
	kept := map[int]bool{}
	for i := max(len(snaps)-keepLast, 0); i < len(snaps); i++ {
		kept[i] = true
	}
	if p.KeepDaily > 0 {
		// Days count back from today; snapshots are oldest first, so the
		// last one seen for a day is its newest.
		cutoff := now.UTC().Truncate(24*time.Hour).AddDate(0, 0, -(p.KeepDaily - 1))
		newestOfDay := map[time.Time]int{}
		for i, s := range snaps {
			if day := s.t.UTC().Truncate(24 * time.Hour); !day.Before(cutoff) {
				newestOfDay[day] = i
			}
		}
		for _, i := range newestOfDay {
			kept[i] = true
		}
	}
	var out []string
	for i, s := range snaps {
		if kept[i] || slices.ContainsFunc(p.Keep, func(k string) bool { return k != "" && strings.HasPrefix(s.id, k) }) {
			continue
		}
		out = append(out, s.id)
	}
	return out
}
