package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Kopia drives the kopia CLI (https://kopia.io).
//
// Every operation connects with a fresh config file, so short-lived
// credentials are picked up each time. The content cache stays in StateDir
// across operations.
type Kopia struct {
	Binary string
}

// kopiaUser is the snapshot source user. The hostname is the workspace, so
// the source reads wsnap@<workspace>:<path>.
const kopiaUser = "wsnap"

// kopiaMaintenanceUser owns maintenance. Kopia runs automatic maintenance
// after a snapshot when the connected identity owns it; a sidecar with a
// no-delete credential must never be that owner.
const kopiaMaintenanceUser = "wsnap-maintenance"

func (k *Kopia) WriterDeletePrefixes() []string {
	// Kopia writes a session marker blob ("s" prefix) at the start of every
	// write session and deletes it when the session commits. Verified
	// against MinIO with a session policy: without this, every snapshot
	// retries DeleteBlob ten times and fails.
	return []string{"s"}
}

func (k *Kopia) env(t Target) ([]string, error) {
	dir := filepath.Join(t.StateDir, "kopia")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	env := []string{
		"KOPIA_PASSWORD=" + t.Password,
		"KOPIA_CONFIG_PATH=" + filepath.Join(dir, "repository.config"),
		// One cache per repository: a sidecar can open two repositories (its
		// own, and a fork source), and Kopia must never read one
		// repository's cached indexes for another.
		"KOPIA_CACHE_DIRECTORY=" + filepath.Join(dir, "cache", repoKey(t.Backend)),
		"KOPIA_LOG_DIR=" + filepath.Join(dir, "logs"),
		"KOPIA_CHECK_FOR_UPDATES=false",
		"HOME=" + dir,
	}
	c := t.Credentials
	switch {
	case t.Backend.S3 != nil:
		env = append(env, "AWS_ACCESS_KEY_ID="+c.AccessKeyID, "AWS_SECRET_ACCESS_KEY="+c.SecretAccessKey)
		if c.SessionToken != "" {
			env = append(env, "AWS_SESSION_TOKEN="+c.SessionToken)
		}
	case t.Backend.Azure != nil:
		if c.AzureSAS != "" {
			env = append(env, "AZURE_STORAGE_SAS_TOKEN="+c.AzureSAS)
		} else {
			env = append(env, "AZURE_STORAGE_KEY="+c.AzureAccountKey)
		}
	}
	return env, nil
}

// storageArgs returns "<backend> <flags...>" for repository connect/create.
func (k *Kopia) storageArgs(t Target) ([]string, error) {
	b := t.Backend
	switch {
	case b.S3 != nil:
		args := []string{"s3", "--bucket=" + b.S3.Bucket, "--prefix=" + prefixDir(b.S3.Prefix), "--endpoint=" + b.S3.Endpoint}
		if b.S3.Region != "" {
			args = append(args, "--region="+b.S3.Region)
		}
		if b.S3.Insecure {
			args = append(args, "--disable-tls")
		}
		return args, nil
	case b.GCS != nil:
		if t.Credentials.GoogleCredentialsJSON == "" {
			return nil, fmt.Errorf("kopia's gcs backend needs a service account key; access tokens (gcs-downscoped) are only supported by restic")
		}
		file := filepath.Join(t.StateDir, "kopia", "gcs-credentials.json")
		// storageArgs runs before env, which creates the directory.
		if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
			return nil, err
		}
		if err := writeSecretFile(file, t.Credentials.GoogleCredentialsJSON); err != nil {
			return nil, err
		}
		return []string{"gcs", "--bucket=" + b.GCS.Bucket, "--prefix=" + prefixDir(b.GCS.Prefix), "--credentials-file=" + file}, nil
	case b.Azure != nil:
		args := []string{"azure", "--container=" + b.Azure.Container, "--prefix=" + prefixDir(b.Azure.Prefix), "--storage-account=" + b.Azure.Account}
		if s := b.Azure.EndpointSuffix; s != "" {
			args = append(args, "--storage-domain=blob."+s)
		}
		return args, nil
	case b.Filesystem != nil:
		return []string{"filesystem", "--path=" + b.Filesystem.Path}, nil
	}
	return nil, fmt.Errorf("backend has no storage type set")
}

// prefixDir turns "a/b" into "a/b/". Kopia concatenates the prefix with blob
// names, so without the slash blobs would land next to the directory.
func prefixDir(p string) string {
	p = strings.Trim(p, "/")
	if p == "" {
		return ""
	}
	return p + "/"
}

func (k *Kopia) kopia(ctx context.Context, t Target, dir string, args ...string) ([]byte, error) {
	env, err := k.env(t)
	if err != nil {
		return nil, err
	}
	return run(ctx, dir, env, k.Binary, args...)
}

func (k *Kopia) Init(ctx context.Context, t Target) error {
	storage, err := k.storageArgs(t)
	if err != nil {
		return err
	}
	// A stale config would make "connect" refuse or reuse old credentials.
	_ = os.Remove(filepath.Join(t.StateDir, "kopia", "repository.config"))
	if fs := t.Backend.Filesystem; fs != nil && !t.ReadOnly {
		// Kopia reports a missing directory as "cannot access storage path",
		// not as an uninitialized repository.
		if err := os.MkdirAll(fs.Path, 0o700); err != nil {
			return err
		}
	}
	// --no-persist-credentials keeps only the repository password out of the
	// config file (it arrives in KOPIA_PASSWORD on every call). Storage
	// credentials are still written to the config. Persisting the password
	// would use the OS keychain on macOS.
	user := kopiaUser
	if t.Maintenance {
		user = kopiaMaintenanceUser
	}
	common := []string{"--override-username=" + user, "--override-hostname=" + t.Workspace, "--no-persist-credentials"}
	connect := append(append([]string{"repository", "connect"}, storage...), common...)
	if t.ReadOnly {
		connect = append(connect, "--readonly")
	}
	_, err = k.kopia(ctx, t, "", connect...)
	if err == nil {
		return k.ensurePolicy(ctx, t)
	}
	if !kopiaNotInitialized(err) {
		return fmt.Errorf("kopia connect: %w", err)
	}
	if t.ReadOnly {
		return fmt.Errorf("kopia connect: %w", ErrRepositoryNotFound)
	}
	create := append(append([]string{"repository", "create"}, storage...), common...)
	if _, err := k.kopia(ctx, t, "", create...); err != nil {
		return fmt.Errorf("kopia create: %w", err)
	}
	return k.ensurePolicy(ctx, t)
}

// Repository creation and policy setup are separate writes. If the process
// stops between them, the next writer must finish setup before it snapshots.
func (k *Kopia) ensurePolicy(ctx context.Context, t Target) error {
	if t.ReadOnly {
		return nil
	}
	// Unreadable files are recorded in the snapshot manifest instead of
	// failing it: a partial snapshot beats none for a final snapshot or a
	// rescue.
	ignore := []string{"policy", "set", "--global", "--ignore-file-errors=true", "--ignore-dir-errors=true"}
	// A leading "/" anchors a rule at the snapshot root; without it Kopia
	// would also drop nested folders that happen to share the name.
	for _, n := range excludedNames {
		ignore = append(ignore, "--add-ignore=/"+n)
	}
	if _, err := k.kopia(ctx, t, "", ignore...); err != nil {
		return fmt.Errorf("kopia set ignore policy: %w", err)
	}
	// "repository create" makes its caller the maintenance owner, and Kopia
	// runs automatic maintenance as the owner after snapshots. A writer's
	// credential cannot delete, so hand ownership to the maintenance
	// identity at once.
	if !t.Maintenance {
		owner := "--owner=" + kopiaMaintenanceUser + "@" + t.Workspace
		if _, err := k.kopia(ctx, t, "", "maintenance", "set", owner); err != nil {
			return fmt.Errorf("kopia maintenance set: %w", err)
		}
	}
	return nil
}

func kopiaNotInitialized(err error) bool {
	ce, ok := err.(*cmdError)
	if !ok {
		return false
	}
	// A filesystem repository that was never created has no directory.
	return strings.Contains(ce.stderr, "repository not initialized") ||
		strings.Contains(ce.stderr, "cannot access storage path")
}

type kopiaSnapshot struct {
	ID        string    `json:"id"`
	StartTime time.Time `json:"startTime"`
}

func (k *Kopia) Snapshot(ctx context.Context, t Target, dir string) (Snapshot, error) {
	// Kopia's built-in retention runs after snapshot creation. Pin every
	// snapshot so wsnap's retention and cross-workspace references decide
	// what can be deleted. Maintain explicitly deletes expired snapshots.
	out, err := k.kopia(ctx, t, dir, "snapshot", "create", dir, "--json", "--pin=wsnap")
	if err != nil {
		return Snapshot{}, fmt.Errorf("kopia snapshot: %w", err)
	}
	var s kopiaSnapshot
	if err := json.Unmarshal(out, &s); err != nil || s.ID == "" {
		return Snapshot{}, fmt.Errorf("kopia snapshot: unexpected output %q", tail(string(out), 500))
	}
	return Snapshot{ID: s.ID, Time: s.StartTime}, nil
}

func (k *Kopia) list(ctx context.Context, t Target) ([]kopiaSnapshot, error) {
	out, err := k.kopia(ctx, t, "", "snapshot", "list", "--all", "--json")
	if err != nil {
		return nil, fmt.Errorf("kopia snapshot list: %w", err)
	}
	var list []kopiaSnapshot
	if err := json.Unmarshal(out, &list); err != nil {
		return nil, fmt.Errorf("kopia snapshot list: %w", err)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].StartTime.Before(list[j].StartTime) })
	return list, nil
}

func (k *Kopia) Latest(ctx context.Context, t Target) (*Snapshot, error) {
	list, err := k.list(ctx, t)
	if err != nil || len(list) == 0 {
		return nil, err
	}
	s := list[len(list)-1]
	return &Snapshot{ID: s.ID, Time: s.StartTime}, nil
}

func (k *Kopia) Restore(ctx context.Context, t Target, id, dir string) error {
	if _, err := k.kopia(ctx, t, "", "snapshot", "restore", id, dir); err != nil {
		return fmt.Errorf("kopia restore: %w", err)
	}
	return nil
}

func (k *Kopia) Maintain(ctx context.Context, t Target, p Retention) error {
	list, err := k.list(ctx, t)
	if err != nil {
		return err
	}
	snaps := make([]snapRef, len(list))
	for i, s := range list {
		snaps[i] = snapRef{s.ID, s.StartTime}
	}
	for _, id := range forgettable(snaps, p, time.Now()) {
		if _, err := k.kopia(ctx, t, "", "snapshot", "delete", id, "--delete"); err != nil {
			return fmt.Errorf("kopia snapshot delete: %w", err)
		}
	}
	// Maintenance runs only for the owner. Each workspace has its own
	// repository, so the maintenance Job can safely claim ownership.
	if _, err := k.kopia(ctx, t, "", "maintenance", "set", "--owner=me"); err != nil {
		return fmt.Errorf("kopia maintenance set: %w", err)
	}
	if _, err := k.kopia(ctx, t, "", "maintenance", "run", "--full"); err != nil {
		return fmt.Errorf("kopia maintenance run: %w", err)
	}
	return nil
}
