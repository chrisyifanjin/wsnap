package credentials

import (
	"context"
	"fmt"
	"strings"
	"time"

	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/google/downscope"

	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

// GCSDownscoped exchanges a service account credential for an access token
// limited by a Credential Access Boundary. See
// https://cloud.google.com/iam/docs/downscoping-short-lived-credentials.
//
// Only restic accepts a raw access token (GOOGLE_ACCESS_TOKEN). Kopia's GCS
// backend needs a credentials file, so pair Kopia with the static provider
// or reach GCS through its S3-compatible XML API.
type GCSDownscoped struct {
	// AllowExternalAccount also accepts an external_account (workload
	// identity federation) configuration. Such a file names a token source
	// (a local file, a URL or a command) that the caller of Issue would
	// read or run. The broker reads the Secret from a tenant namespace, so
	// it never sets this; tests that log in with OIDC do.
	AllowExternalAccount bool
}

func (g GCSDownscoped) Issue(ctx context.Context, req Request) (protocol.Credentials, error) {
	var out protocol.Credentials
	if req.Backend.GCS == nil {
		return out, fmt.Errorf("provider gcs-downscoped needs a gcs backend")
	}
	keyJSON, err := secretValue(req.Secret, KeyGoogleCredentials)
	if err != nil {
		return out, err
	}
	credType := google.ServiceAccount
	if g.AllowExternalAccount && strings.Contains(keyJSON, `"external_account"`) {
		credType = google.ExternalAccount
	}
	root, err := google.CredentialsFromJSONWithType(ctx, []byte(keyJSON), credType, "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return out, fmt.Errorf("parse %s: %w", KeyGoogleCredentials, err)
	}
	ts, err := downscope.NewTokenSource(ctx, downscope.DownscopingConfig{
		RootSource: root.TokenSource,
		Rules:      GCSAccessBoundary(req.Role, req.Backend.GCS.Bucket, req.Backend.GCS.Prefix, req.WriterDeletePrefixes),
	})
	if err != nil {
		return out, fmt.Errorf("downscope: %w", err)
	}
	tok, err := ts.Token()
	if err != nil {
		return out, fmt.Errorf("downscoped token: %w", err)
	}
	out.GoogleAccessToken = tok.AccessToken
	out.ExpiresAt = tok.Expiry
	if out.ExpiresAt.IsZero() {
		out.ExpiresAt = time.Now().Add(time.Hour)
	}
	out.Scoped = true
	return out, nil
}

// GCSAccessBoundary returns the boundary rules for role. Readers get
// objectViewer. Writers add objectCreator, which can create objects but not
// overwrite or delete them, plus objectUser on the engine's writer-delete
// prefixes. Maintenance gets objectUser on the whole workspace prefix.
func GCSAccessBoundary(role protocol.Role, bucket, prefix string, writerDeletePrefixes []string) []downscope.AccessBoundaryRule {
	prefix = strings.Trim(prefix, "/") + "/"
	resource := "//storage.googleapis.com/projects/_/buckets/" + bucket
	objects := "projects/_/buckets/" + bucket + "/objects/"
	under := func(p string) *downscope.AvailabilityCondition {
		return &downscope.AvailabilityCondition{
			Expression: fmt.Sprintf("resource.name.startsWith('%s%s')", objects, p),
		}
	}
	rules := []downscope.AccessBoundaryRule{{
		AvailableResource:    resource,
		AvailablePermissions: []string{"inRole:roles/storage.objectViewer"},
		Condition: &downscope.AvailabilityCondition{
			Expression: fmt.Sprintf(
				"resource.name.startsWith('%s%s') || api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('%s')",
				objects, prefix, prefix),
		},
	}}
	switch {
	case role.CanDelete():
		rules = append(rules, downscope.AccessBoundaryRule{
			AvailableResource:    resource,
			AvailablePermissions: []string{"inRole:roles/storage.objectUser"},
			Condition:            under(prefix),
		})
	case role.CanWrite():
		rules = append(rules, downscope.AccessBoundaryRule{
			AvailableResource:    resource,
			AvailablePermissions: []string{"inRole:roles/storage.objectCreator"},
			Condition:            under(prefix),
		})
		for _, p := range writerDeletePrefixes {
			rules = append(rules, downscope.AccessBoundaryRule{
				AvailableResource:    resource,
				AvailablePermissions: []string{"inRole:roles/storage.objectUser"},
				Condition:            under(prefix + strings.TrimLeft(p, "/")),
			})
		}
	}
	return rules
}
