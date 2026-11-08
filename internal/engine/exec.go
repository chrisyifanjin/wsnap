package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Exec adapts any snapshot tool to wsnap through a small command contract,
// so new technologies need a script, not a code change.
//
// The agent runs `<command...> <verb>` with verb one of init, snapshot,
// latest, restore, maintain. The environment carries:
//
//	WSNAP_WORKSPACE         workspace identity
//	WSNAP_DIR               directory to snapshot or restore into
//	WSNAP_SNAPSHOT_ID       snapshot to restore (restore)
//	WSNAP_KEEP_LAST         retention (maintain)
//	WSNAP_KEEP_DAILY        also keep the newest snapshot of each of the last N days (maintain)
//	WSNAP_KEEP_IDS          newline-separated snapshot IDs to keep (maintain)
//	WSNAP_READ_ONLY         "true" for the restore role
//	WSNAP_STATE_DIR         private writable directory
//	WSNAP_PASSWORD          repository password
//	WSNAP_BACKEND           protocol.Backend as JSON
//	WSNAP_CREDENTIALS       protocol.Credentials as JSON
//	WSNAP_EXCLUDE           newline-separated names to leave out
//
// plus AWS_*, GOOGLE_ACCESS_TOKEN and AZURE_* variables for the backend.
// snapshot and latest print {"id": "...", "time": "<RFC 3339>"} on stdout;
// snapshot adds "partial": true when it skipped files it could not read;
// latest prints nothing when the repository is empty. init exits with
// status 3 when WSNAP_READ_ONLY is true and the repository does not exist.
// Any other nonzero exit status fails the operation.
type Exec struct {
	Command []string
}

func (e *Exec) WriterDeletePrefixes() []string { return nil }

func (e *Exec) env(t Target, dir, id string, keepLast int) ([]string, error) {
	if err := os.MkdirAll(t.StateDir, 0o700); err != nil {
		return nil, err
	}
	backend, err := json.Marshal(t.Backend)
	if err != nil {
		return nil, err
	}
	creds, err := json.Marshal(t.Credentials)
	if err != nil {
		return nil, err
	}
	env := []string{
		"WSNAP_WORKSPACE=" + t.Workspace,
		"WSNAP_DIR=" + dir,
		"WSNAP_SNAPSHOT_ID=" + id,
		"WSNAP_KEEP_LAST=" + strconv.Itoa(keepLast),
		"WSNAP_READ_ONLY=" + strconv.FormatBool(t.ReadOnly),
		"WSNAP_STATE_DIR=" + t.StateDir,
		"WSNAP_PASSWORD=" + t.Password,
		"WSNAP_BACKEND=" + string(backend),
		"WSNAP_CREDENTIALS=" + string(creds),
		"WSNAP_EXCLUDE=" + strings.Join(excludedNames, "\n"),
		"HOME=" + t.StateDir,
	}
	c := t.Credentials
	if c.AccessKeyID != "" {
		env = append(env, "AWS_ACCESS_KEY_ID="+c.AccessKeyID, "AWS_SECRET_ACCESS_KEY="+c.SecretAccessKey)
		if c.SessionToken != "" {
			env = append(env, "AWS_SESSION_TOKEN="+c.SessionToken)
		}
	}
	if c.GoogleAccessToken != "" {
		env = append(env, "GOOGLE_ACCESS_TOKEN="+c.GoogleAccessToken)
	}
	if c.AzureSAS != "" {
		env = append(env, "AZURE_STORAGE_SAS_TOKEN="+c.AzureSAS)
	}
	if c.AzureAccountKey != "" {
		env = append(env, "AZURE_STORAGE_KEY="+c.AzureAccountKey)
	}
	return env, nil
}

func (e *Exec) call(ctx context.Context, t Target, verb, dir, id string, keepLast int, extra ...string) ([]byte, error) {
	env, err := e.env(t, dir, id, keepLast)
	if err != nil {
		return nil, err
	}
	env = append(env, extra...)
	args := append(append([]string{}, e.Command[1:]...), verb)
	return run(ctx, dir, env, e.Command[0], args...)
}

type execSnapshot struct {
	ID      string    `json:"id"`
	Time    time.Time `json:"time"`
	Partial bool      `json:"partial"`
}

func parseExecSnapshot(out []byte) (*Snapshot, error) {
	s := strings.TrimSpace(string(out))
	if s == "" || s == "null" {
		return nil, nil
	}
	var v execSnapshot
	if err := json.Unmarshal([]byte(s), &v); err != nil || v.ID == "" {
		return nil, fmt.Errorf("exec engine: want {\"id\":...,\"time\":...}, got %q", tail(s, 300))
	}
	return &Snapshot{ID: v.ID, Time: v.Time, Partial: v.Partial}, nil
}

// execExitNotFound is the exec contract's "repository does not exist".
const execExitNotFound = 3

func (e *Exec) Init(ctx context.Context, t Target) error {
	_, err := e.call(ctx, t, "init", "", "", 0)
	if err != nil && t.ReadOnly && exitCode(err) == execExitNotFound {
		return fmt.Errorf("exec init: %w", ErrRepositoryNotFound)
	}
	return err
}

func (e *Exec) Snapshot(ctx context.Context, t Target, dir string) (Snapshot, error) {
	out, err := e.call(ctx, t, "snapshot", dir, "", 0)
	if err != nil {
		return Snapshot{}, err
	}
	s, err := parseExecSnapshot(out)
	if err != nil {
		return Snapshot{}, err
	}
	if s == nil {
		return Snapshot{}, fmt.Errorf("exec engine: snapshot printed no snapshot")
	}
	return *s, nil
}

func (e *Exec) Latest(ctx context.Context, t Target) (*Snapshot, error) {
	out, err := e.call(ctx, t, "latest", "", "", 0)
	if err != nil {
		return nil, err
	}
	return parseExecSnapshot(out)
}

func (e *Exec) Restore(ctx context.Context, t Target, id, dir string) error {
	_, err := e.call(ctx, t, "restore", dir, id, 0)
	return err
}

func (e *Exec) Maintain(ctx context.Context, t Target, p Retention) error {
	_, err := e.call(ctx, t, "maintain", "", "", p.KeepLast,
		"WSNAP_KEEP_IDS="+strings.Join(p.Keep, "\n"), "WSNAP_KEEP_DAILY="+strconv.Itoa(p.KeepDaily))
	return err
}
