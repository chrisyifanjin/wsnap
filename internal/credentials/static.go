package credentials

import (
	"context"
	"fmt"

	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

// Static passes the admin credential through unchanged. Every agent can then
// read and delete every workspace in the repository, so use it only where
// all workspaces trust each other.
type Static struct{}

func (Static) Issue(_ context.Context, req Request) (protocol.Credentials, error) {
	var c protocol.Credentials
	var err error
	switch {
	case req.Backend.S3 != nil:
		if c.AccessKeyID, err = secretValue(req.Secret, KeyAWSAccessKeyID); err != nil {
			return c, err
		}
		if c.SecretAccessKey, err = secretValue(req.Secret, KeyAWSSecretAccessKey); err != nil {
			return c, err
		}
		c.SessionToken = string(req.Secret[KeyAWSSessionToken])
	case req.Backend.GCS != nil:
		if c.GoogleCredentialsJSON, err = secretValue(req.Secret, KeyGoogleCredentials); err != nil {
			return c, err
		}
	case req.Backend.Azure != nil:
		if c.AzureAccountKey, err = secretValue(req.Secret, KeyAzureAccountKey); err != nil {
			return c, err
		}
	case req.Backend.Filesystem != nil:
		// The agent reaches the filesystem through its own volume mount.
	default:
		return c, fmt.Errorf("backend has no storage type set")
	}
	return c, nil
}
