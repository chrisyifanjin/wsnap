// Package credentials turns the admin credential in a SnapshotRepository's
// Secret into a credential for one workspace and one role.
//
// Providers:
//
//   - static: passes the admin credential through. Not scoped. Use it for
//     development or single-tenant clusters only.
//   - s3-sts: calls STS AssumeRole with a session policy limited to the
//     workspace prefix and the role's actions. Works with AWS S3, MinIO and
//     Ceph RGW.
//   - gcs-downscoped: exchanges a service account credential for a
//     downscoped token with a Credential Access Boundary on the workspace
//     prefix.
//   - azure-sas: signs a service SAS for a container that holds only this
//     workspace.
package credentials

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

// Keys in a SnapshotRepository credentials Secret.
const (
	KeyAWSAccessKeyID     = "AWS_ACCESS_KEY_ID"
	KeyAWSSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
	KeyAWSSessionToken    = "AWS_SESSION_TOKEN"
	KeyGoogleCredentials  = "GOOGLE_CREDENTIALS_JSON"
	KeyAzureAccountKey    = "AZURE_ACCOUNT_KEY"
	// KeyMasterKey seeds the per-workspace repository passwords.
	KeyMasterKey = "WSNAP_MASTER_KEY"
)

// ProviderType names a credential provider.
type ProviderType string

const (
	ProviderStatic        ProviderType = "static"
	ProviderS3STS         ProviderType = "s3-sts"
	ProviderGCSDownscoped ProviderType = "gcs-downscoped"
	ProviderAzureSAS      ProviderType = "azure-sas"
)

// Request describes the credential an agent needs.
type Request struct {
	Role      protocol.Role
	Namespace string
	Workspace string
	// Backend already contains the per-workspace prefix.
	Backend protocol.Backend
	// Secret is the SnapshotRepository's credentials Secret data.
	Secret map[string][]byte
	// WriterDeletePrefixes come from the engine. See S3SessionPolicy.
	WriterDeletePrefixes []string
	// TTL is the requested lifetime. Providers may round it up to their
	// minimum (AWS STS: 15 minutes).
	TTL time.Duration
}

// Provider issues a credential for one workspace and role.
type Provider interface {
	Issue(ctx context.Context, req Request) (protocol.Credentials, error)
}

// RepositoryPassword derives the repository password of one workspace from
// the master key. A leaked password decrypts only that workspace.
func RepositoryPassword(masterKey []byte, namespace, workspace string) (string, error) {
	if len(masterKey) < 16 {
		return "", fmt.Errorf("secret key %s must be at least 16 bytes", KeyMasterKey)
	}
	m := hmac.New(sha256.New, masterKey)
	m.Write([]byte("wsnap-repository-password\x00" + namespace + "\x00" + workspace))
	return hex.EncodeToString(m.Sum(nil)), nil
}

func secretValue(secret map[string][]byte, key string) (string, error) {
	v, ok := secret[key]
	if !ok || len(v) == 0 {
		return "", fmt.Errorf("credentials secret has no key %q", key)
	}
	return string(v), nil
}
