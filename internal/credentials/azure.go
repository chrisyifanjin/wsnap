package credentials

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"

	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

// AzureSAS signs a service SAS for a container that holds exactly one
// workspace. A service SAS on a flat-namespace account cannot be limited to
// a blob name prefix (sr=d needs a hierarchical namespace), so isolation
// comes from the container boundary instead.
//
// The writer roles get Delete because restic removes its lock files and
// Kopia removes its session markers. A writer can therefore delete data in
// its own workspace container. Enable blob soft delete or versioning on the
// storage account to recover from that.
type AzureSAS struct {
	// ServiceURL overrides https://<account>.blob.<endpointSuffix>/, for
	// example to point at the Azurite emulator.
	ServiceURL string
}

// AzureWorkspaceContainer names the per-workspace container. Azure allows
// 3-63 lowercase letters, digits and single hyphens, so the workspace
// identity is hashed.
func AzureWorkspaceContainer(base, namespace, workspace string) string {
	sum := sha256.Sum256([]byte(namespace + "/" + workspace))
	suffix := hex.EncodeToString(sum[:])[:16]
	base = strings.Trim(strings.ToLower(base), "-")
	if max := 63 - len(suffix) - 1; len(base) > max {
		base = strings.TrimRight(base[:max], "-")
	}
	return base + "-" + suffix
}

// AzureSASPermissions returns the container SAS permissions for role.
func AzureSASPermissions(role protocol.Role) string {
	p := sas.ContainerPermissions{Read: true, List: true}
	if role.CanWrite() {
		p.Create, p.Write, p.Delete = true, true, true
	}
	return p.String()
}

func (a AzureSAS) Issue(ctx context.Context, req Request) (protocol.Credentials, error) {
	var out protocol.Credentials
	az := req.Backend.Azure
	if az == nil {
		return out, fmt.Errorf("provider azure-sas needs an azure backend")
	}
	key, err := secretValue(req.Secret, KeyAzureAccountKey)
	if err != nil {
		return out, err
	}
	cred, err := azblob.NewSharedKeyCredential(az.Account, key)
	if err != nil {
		return out, fmt.Errorf("azure shared key: %w", err)
	}
	if err := a.ensureContainer(ctx, az, cred); err != nil {
		return out, err
	}
	ttl := req.TTL
	if ttl <= 0 {
		ttl = time.Hour
	}
	now := time.Now().UTC()
	qp, err := sas.BlobSignatureValues{
		Protocol:      a.protocol(),
		StartTime:     now.Add(-5 * time.Minute), // tolerate clock skew
		ExpiryTime:    now.Add(ttl),
		Permissions:   AzureSASPermissions(req.Role),
		ContainerName: az.Container,
	}.SignWithSharedKey(cred)
	if err != nil {
		return out, fmt.Errorf("sign SAS: %w", err)
	}
	out.AzureSAS = qp.Encode()
	out.ExpiresAt = now.Add(ttl)
	out.Scoped = true
	return out, nil
}

func (a AzureSAS) protocol() sas.Protocol {
	if strings.HasPrefix(a.ServiceURL, "http://") {
		return sas.ProtocolHTTPSandHTTP
	}
	return sas.ProtocolHTTPS
}

func (a AzureSAS) serviceURL(az *protocol.AzureBackend) string {
	if a.ServiceURL != "" {
		return strings.TrimRight(a.ServiceURL, "/") + "/"
	}
	suffix := az.EndpointSuffix
	if suffix == "" {
		suffix = "core.windows.net"
	}
	return fmt.Sprintf("https://%s.blob.%s/", az.Account, suffix)
}

// ensureContainer creates the workspace container. A service SAS cannot
// create containers, so the broker does it with the account key.
func (a AzureSAS) ensureContainer(ctx context.Context, az *protocol.AzureBackend, cred *azblob.SharedKeyCredential) error {
	c, err := container.NewClientWithSharedKeyCredential(a.serviceURL(az)+az.Container, cred, nil)
	if err != nil {
		return err
	}
	if _, err := c.Create(ctx, nil); err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		return fmt.Errorf("create container %s: %w", az.Container, err)
	}
	return nil
}
