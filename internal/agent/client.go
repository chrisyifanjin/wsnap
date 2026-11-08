package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/chrisyifanjin/wsnap/internal/engine"
	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

type brokerError struct {
	status int
	body   string
}

func (e *brokerError) Error() string { return fmt.Sprintf("broker returned %d: %s", e.status, e.body) }

func retryable(err error) bool {
	var be *brokerError
	if errors.As(err, &be) {
		return be.status >= 500 || be.status == http.StatusTooManyRequests
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// post retries network errors, 5xx and 429 with exponential backoff, so a
// broker restart does not fail a final snapshot.
func (c *Config) post(ctx context.Context, path string, in, out any) error {
	var err error
	backoff := 500 * time.Millisecond
	for attempt := 0; attempt < 5; attempt++ {
		if err = c.postOnce(ctx, path, in, out); err == nil || !retryable(err) {
			return err
		}
		c.Log.Warn("broker request failed; retrying", "path", path, "error", err, "in", backoff)
		select {
		case <-ctx.Done():
			return err
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return err
}

func (c *Config) postOnce(ctx context.Context, path string, in, out any) error {
	token, err := os.ReadFile(c.TokenFile)
	if err != nil {
		return fmt.Errorf("read token: %w", err)
	}
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BrokerURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 300 {
		return &brokerError{status: resp.StatusCode, body: strings.TrimSpace(string(data))}
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

// credentials returns a credential for role, reusing a cached one while it
// is valid. Restore and maintenance credentials are one-shot and never
// cached.
func (c *Config) credentials(ctx context.Context, role protocol.Role) (*protocol.CredentialsResponse, error) {
	cacheable := role == protocol.RoleSnapshot || role == protocol.RoleMover
	if cacheable {
		c.mu.Lock()
		cc, ok := c.cache[role]
		c.mu.Unlock()
		if ok && time.Now().Before(cc.validUntil) {
			return cc.resp, nil
		}
	}
	var resp protocol.CredentialsResponse
	if err := c.post(ctx, "/v1/credentials", protocol.CredentialsRequest{Role: role}, &resp); err != nil {
		return nil, err
	}
	c.adopt(&resp)
	if cacheable {
		until := time.Now().Add(unexpiringReuse)
		if exp := resp.Credentials.ExpiresAt; !exp.IsZero() {
			// An operation started with this credential may run for
			// OperationTimeout; it must not expire halfway. With the
			// defaults (1h credential, 30m operations) one credential
			// serves 25 minutes of snapshots.
			until = exp.Add(-max(credentialMargin, c.operationTimeout()+credentialMargin))
			if time.Until(exp) < c.operationTimeout() {
				c.Log.Warn("credentials expire before an operation's time limit; raise credentials.ttl or lower --operation-timeout",
					"expiresIn", time.Until(exp).Round(time.Second), "operationTimeout", c.operationTimeout())
			}
		}
		c.mu.Lock()
		if c.cache == nil {
			c.cache = map[protocol.Role]cachedCredentials{}
		}
		c.cache[role] = cachedCredentials{resp: &resp, validUntil: until}
		c.mu.Unlock()
	}
	return &resp, nil
}

// credentialsFor fetches a one-shot credential bound to a request.
func (c *Config) credentialsFor(ctx context.Context, req protocol.CredentialsRequest) (*protocol.CredentialsResponse, error) {
	var resp protocol.CredentialsResponse
	if err := c.post(ctx, "/v1/credentials", req, &resp); err != nil {
		return nil, err
	}
	c.adopt(&resp)
	return &resp, nil
}

// invalidate drops a cached credential after a failure, in case the
// failure came from a revoked or misconfigured credential.
func (c *Config) invalidate(role protocol.Role) {
	c.mu.Lock()
	delete(c.cache, role)
	c.mu.Unlock()
}

// strictReportFor is how long a Job keeps retrying a report. A variable
// for tests.
var strictReportFor = 5 * time.Minute

// report sends r to the broker. For a sidecar a lost report delays
// bookkeeping only: without a recorded final snapshot, the controller
// rescues the volume later. A Job (StrictReports) exists to produce the
// record, so it retries for longer and returns the error, and the Job
// fails instead of completing with nothing recorded.
func (c *Config) report(ctx context.Context, r protocol.Report) error {
	err := c.post(ctx, "/v1/report", r, nil)
	if c.StrictReports {
		deadline := time.Now().Add(strictReportFor)
		for err != nil && retryable(err) && time.Now().Before(deadline) && ctx.Err() == nil {
			c.Log.Warn("report failed; retrying", "operation", r.Operation, "error", err)
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
			}
			err = c.post(ctx, "/v1/report", r, nil)
		}
	}
	if err != nil {
		c.Log.Error("report failed", "operation", r.Operation, "error", err)
		if c.StrictReports {
			return fmt.Errorf("report %s: %w", r.Operation, err)
		}
	}
	return nil
}

// open fetches a credential for role and opens the repository with it.
func (c *Config) open(ctx context.Context, role protocol.Role) (*protocol.CredentialsResponse, engine.Engine, engine.Target, error) {
	creds, err := c.credentials(ctx, role)
	if err != nil {
		return nil, nil, engine.Target{}, err
	}
	eng, err := engine.New(creds.Engine)
	if err != nil {
		return creds, nil, engine.Target{}, err
	}
	t := engine.Target{
		Backend: creds.Backend, Credentials: creds.Credentials, Password: creds.RepositoryPassword,
		Workspace: creds.Workspace, StateDir: c.StateDir, ReadOnly: !role.CanWrite(),
		Maintenance: role == protocol.RoleMaintenance,
	}
	if !creds.Credentials.Scoped {
		c.Log.Warn("broker issued an unscoped credential; it can reach every workspace in the repository", "role", role)
	}
	return creds, eng, t, eng.Init(ctx, t)
}

func (c *Config) withOperationTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	d := c.operationTimeout()
	if d <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, d)
}

// operationTimeout is the repository's spec.operationTimeout once the
// broker has sent it, else the --operation-timeout flag.
func (c *Config) operationTimeout() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.repoOpTimeout > 0 {
		return c.repoOpTimeout
	}
	return c.OperationTimeout
}

func (c *Config) adopt(resp *protocol.CredentialsResponse) {
	if d := resp.OperationTimeout.Duration; d > 0 {
		c.mu.Lock()
		c.repoOpTimeout = d
		c.mu.Unlock()
	}
}
