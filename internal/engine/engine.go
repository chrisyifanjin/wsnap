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
}

// Snapshot identifies one snapshot.
type Snapshot struct {
	ID   string
	Time time.Time
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
	// Maintain keeps the newest keepLast snapshots, deletes the rest and
	// frees unreferenced data.
	Maintain(ctx context.Context, t Target, keepLast int) error
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
