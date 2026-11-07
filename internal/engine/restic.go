package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Restic drives the restic CLI (https://restic.net).
type Restic struct {
	Binary string
}

// resticExitRepoMissing is restic's exit code for "repository does not
// exist" (restic 0.17 and later).
const resticExitRepoMissing = 10

// resticExitIncomplete is restic's exit code for "snapshot created, but
// some source files could not be read".
const resticExitIncomplete = 3

func (r *Restic) WriterDeletePrefixes() []string {
	// restic creates a lock file for every write command and removes it at
	// the end. Verified against MinIO with a session policy: without delete
	// on locks/ the backup succeeds but stale locks pile up and later block
	// "prune".
	return []string{"locks/"}
}

// repository returns the RESTIC_REPOSITORY value. See
// https://restic.readthedocs.io/en/stable/030_preparing_a_new_repo.html.
func (r *Restic) repository(t Target) (string, error) {
	b := t.Backend
	switch {
	case b.S3 != nil:
		scheme := "https"
		if b.S3.Insecure {
			scheme = "http"
		}
		return fmt.Sprintf("s3:%s://%s/%s/%s", scheme, b.S3.Endpoint, b.S3.Bucket, strings.Trim(b.S3.Prefix, "/")), nil
	case b.GCS != nil:
		return fmt.Sprintf("gs:%s:/%s", b.GCS.Bucket, strings.Trim(b.GCS.Prefix, "/")), nil
	case b.Azure != nil:
		return fmt.Sprintf("azure:%s:/%s", b.Azure.Container, strings.Trim(b.Azure.Prefix, "/")), nil
	case b.Filesystem != nil:
		return b.Filesystem.Path, nil
	}
	return "", fmt.Errorf("backend has no storage type set")
}

// env follows https://restic.readthedocs.io/en/stable/075_scripting.html.
func (r *Restic) env(t Target) ([]string, error) {
	dir := filepath.Join(t.StateDir, "restic")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	repo, err := r.repository(t)
	if err != nil {
		return nil, err
	}
	env := []string{
		"RESTIC_REPOSITORY=" + repo,
		"RESTIC_PASSWORD=" + t.Password,
		"RESTIC_CACHE_DIR=" + filepath.Join(dir, "cache"),
		"HOME=" + dir,
	}
	c := t.Credentials
	switch b := t.Backend; {
	case b.S3 != nil:
		env = append(env, "AWS_ACCESS_KEY_ID="+c.AccessKeyID, "AWS_SECRET_ACCESS_KEY="+c.SecretAccessKey)
		if c.SessionToken != "" {
			env = append(env, "AWS_SESSION_TOKEN="+c.SessionToken)
		}
		if b.S3.Region != "" {
			env = append(env, "AWS_DEFAULT_REGION="+b.S3.Region)
		}
	case b.GCS != nil:
		if c.GoogleAccessToken != "" {
			env = append(env, "GOOGLE_ACCESS_TOKEN="+c.GoogleAccessToken)
		} else {
			file := filepath.Join(dir, "gcs-credentials.json")
			if err := writeSecretFile(file, c.GoogleCredentialsJSON); err != nil {
				return nil, err
			}
			env = append(env, "GOOGLE_APPLICATION_CREDENTIALS="+file)
		}
	case b.Azure != nil:
		env = append(env, "AZURE_ACCOUNT_NAME="+b.Azure.Account)
		if c.AzureSAS != "" {
			env = append(env, "AZURE_ACCOUNT_SAS="+c.AzureSAS)
		} else {
			env = append(env, "AZURE_ACCOUNT_KEY="+c.AzureAccountKey)
		}
		if b.Azure.EndpointSuffix != "" {
			env = append(env, "AZURE_ENDPOINT_SUFFIX="+b.Azure.EndpointSuffix)
		}
	}
	return env, nil
}

// restic runs a command. Read-only targets pass --no-lock, because creating
// a lock is a write and the restore role cannot write.
func (r *Restic) restic(ctx context.Context, t Target, dir string, args ...string) ([]byte, error) {
	env, err := r.env(t)
	if err != nil {
		return nil, err
	}
	if t.ReadOnly {
		args = append([]string{"--no-lock"}, args...)
	}
	if az := t.Backend.Azure; az != nil && az.AccessTier != "" {
		args = append([]string{"-o", "azure.access-tier=" + az.AccessTier}, args...)
	}
	return run(ctx, dir, env, r.Binary, args...)
}

func (r *Restic) Init(ctx context.Context, t Target) error {
	_, err := r.restic(ctx, Target{
		Backend: t.Backend, Credentials: t.Credentials, Password: t.Password,
		Workspace: t.Workspace, StateDir: t.StateDir, ReadOnly: true,
	}, "", "cat", "config")
	if err == nil {
		return nil
	}
	if exitCode(err) != resticExitRepoMissing {
		return fmt.Errorf("restic open: %w", err)
	}
	if t.ReadOnly {
		return fmt.Errorf("restic open: %w", ErrRepositoryNotFound)
	}
	if _, err := r.restic(ctx, t, "", "init"); err != nil {
		return fmt.Errorf("restic init: %w", err)
	}
	return nil
}

func (r *Restic) Snapshot(ctx context.Context, t Target, dir string) (Snapshot, error) {
	// Backing up "." from inside dir makes restore land directly in the
	// target directory, whatever path the volume is mounted at.
	args := []string{"backup", ".", "--json", "--host=" + t.Workspace}
	// restic matches exclude patterns that start with "/" against the
	// absolute path it records, which has symlinks resolved. Anchor the
	// patterns at the resolved dir so only the top-level entries match.
	abs, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return Snapshot{}, err
	}
	for _, n := range excludedNames {
		args = append(args, "--exclude="+filepath.Join(abs, n))
	}
	out, err := r.restic(ctx, t, dir, args...)
	// Exit code 3: the snapshot was created but some files could not be
	// read. A partial snapshot beats none, above all for a final snapshot
	// or a rescue; the summary below still carries the snapshot ID.
	if err != nil && exitCode(err) != resticExitIncomplete {
		return Snapshot{}, fmt.Errorf("restic backup: %w", err)
	}
	// --json prints one status object per line and a summary at the end.
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var m struct {
			MessageType string `json:"message_type"`
			SnapshotID  string `json:"snapshot_id"`
		}
		if json.Unmarshal(sc.Bytes(), &m) == nil && m.MessageType == "summary" && m.SnapshotID != "" {
			return Snapshot{ID: m.SnapshotID, Time: time.Now().UTC()}, nil
		}
	}
	return Snapshot{}, fmt.Errorf("restic backup: no summary in output %q", tail(string(out), 500))
}

type resticSnapshot struct {
	ID   string    `json:"id"`
	Time time.Time `json:"time"`
}

func (r *Restic) list(ctx context.Context, t Target) ([]resticSnapshot, error) {
	out, err := r.restic(ctx, t, "", "snapshots", "--json")
	if err != nil {
		return nil, fmt.Errorf("restic snapshots: %w", err)
	}
	var list []resticSnapshot
	if err := json.Unmarshal(out, &list); err != nil {
		return nil, fmt.Errorf("restic snapshots: %w", err)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Time.Before(list[j].Time) })
	return list, nil
}

func (r *Restic) Latest(ctx context.Context, t Target) (*Snapshot, error) {
	list, err := r.list(ctx, t)
	if err != nil || len(list) == 0 {
		return nil, err
	}
	s := list[len(list)-1]
	return &Snapshot{ID: s.ID, Time: s.Time}, nil
}

func (r *Restic) Restore(ctx context.Context, t Target, id, dir string) error {
	if _, err := r.restic(ctx, t, "", "restore", id, "--target="+dir); err != nil {
		return fmt.Errorf("restic restore: %w", err)
	}
	return nil
}

func (r *Restic) Maintain(ctx context.Context, t Target, keepLast int) error {
	if keepLast < 1 {
		keepLast = 1
	}
	// Locks left by a killed sidecar would block prune.
	if _, err := r.restic(ctx, t, "", "unlock"); err != nil {
		return fmt.Errorf("restic unlock: %w", err)
	}
	if _, err := r.restic(ctx, t, "", "forget", "--keep-last="+strconv.Itoa(keepLast), "--prune"); err != nil {
		return fmt.Errorf("restic forget: %w", err)
	}
	return nil
}
