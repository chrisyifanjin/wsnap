// Package protocol defines the wire types exchanged between the wsnap broker
// (inside the controller) and wsnap agents (sidecars and Jobs). Agents never
// read Kubernetes objects; everything they need arrives in a
// CredentialsResponse.
package protocol

import "time"

// Role is what an agent asks to do. Each role gets a differently scoped
// credential, so a leaked credential grants only that role's access.
type Role string

const (
	// RoleRestore reads one workspace's snapshots. The broker issues it only
	// before the pod's workload containers have started.
	RoleRestore Role = "restore"
	// RoleSnapshot writes new snapshots of one workspace. It cannot delete.
	// The in-pod sidecar uses it, next to untrusted code.
	RoleSnapshot Role = "snapshot"
	// RoleMover is RoleSnapshot for controller-created rescue Jobs that run
	// outside the workspace pod.
	RoleMover Role = "mover"
	// RoleMaintenance can delete data. Only controller-created maintenance
	// Jobs receive it.
	RoleMaintenance Role = "maintenance"
)

// Valid reports whether r is a known role.
func (r Role) Valid() bool {
	switch r {
	case RoleRestore, RoleSnapshot, RoleMover, RoleMaintenance:
		return true
	}
	return false
}

// CanWrite reports whether the role may add data to the repository.
func (r Role) CanWrite() bool { return r != RoleRestore }

// CanDelete reports whether the role may remove data from the repository.
func (r Role) CanDelete() bool { return r == RoleMaintenance }

// EngineType names a snapshot technology.
type EngineType string

const (
	EngineKopia  EngineType = "kopia"
	EngineRestic EngineType = "restic"
	// EngineExec runs a user-supplied command that implements the exec
	// engine contract (see internal/engine/exec.go).
	EngineExec EngineType = "exec"
)

// Engine is the resolved engine configuration.
type Engine struct {
	Type EngineType `json:"type"`
	// Command is the exec engine's command. Unused for other engines.
	Command []string `json:"command,omitempty"`
}

// Backend is the storage location of exactly one workspace's repository.
// The broker has already appended the per-workspace path, so agents use it
// as is. Exactly one field is set.
type Backend struct {
	S3         *S3Backend         `json:"s3,omitempty"`
	GCS        *GCSBackend        `json:"gcs,omitempty"`
	Azure      *AzureBackend      `json:"azure,omitempty"`
	Filesystem *FilesystemBackend `json:"filesystem,omitempty"`
}

// S3Backend is any S3-compatible object store: AWS S3, MinIO, Ceph RGW,
// Cloudflare R2, Wasabi, Backblaze B2, or GCS through its XML API.
type S3Backend struct {
	// Endpoint is host[:port] without a scheme, e.g. s3.amazonaws.com.
	Endpoint string `json:"endpoint"`
	Bucket   string `json:"bucket"`
	// Prefix has no leading or trailing slash.
	Prefix string `json:"prefix"`
	Region string `json:"region,omitempty"`
	// Insecure selects plain HTTP. Use it only for local test stores.
	Insecure bool `json:"insecure,omitempty"`
}

// GCSBackend is Google Cloud Storage through its native API.
type GCSBackend struct {
	Bucket string `json:"bucket"`
	Prefix string `json:"prefix"`
}

// AzureBackend is Azure Blob Storage.
type AzureBackend struct {
	Account   string `json:"account"`
	Container string `json:"container"`
	Prefix    string `json:"prefix"`
	// EndpointSuffix defaults to core.windows.net. Sovereign clouds use
	// other suffixes, e.g. core.chinacloudapi.cn.
	EndpointSuffix string `json:"endpointSuffix,omitempty"`
	// AccessTier for new blobs; empty means the account default.
	AccessTier string `json:"accessTier,omitempty"`
}

// FilesystemBackend is a directory mounted into the agent container, for
// example from an NFS volume.
type FilesystemBackend struct {
	Path string `json:"path"`
}

// Credentials holds whatever the backend needs. The broker fills only the
// fields that match the backend and provider.
type Credentials struct {
	AccessKeyID     string `json:"accessKeyID,omitempty"`
	SecretAccessKey string `json:"secretAccessKey,omitempty"`
	SessionToken    string `json:"sessionToken,omitempty"`

	// GoogleAccessToken is an OAuth 2.0 access token, e.g. a downscoped one.
	GoogleAccessToken string `json:"googleAccessToken,omitempty"`
	// GoogleCredentialsJSON is a service account key file's contents.
	GoogleCredentialsJSON string `json:"googleCredentialsJSON,omitempty"`

	AzureAccountKey string `json:"azureAccountKey,omitempty"`
	// AzureSAS is a shared access signature without the leading "?".
	AzureSAS string `json:"azureSAS,omitempty"`

	// Scoped is true when the provider limited the credential to this
	// workspace and role. False means the agent holds the admin credential.
	Scoped bool `json:"scoped"`
	// ExpiresAt is zero for credentials that do not expire.
	ExpiresAt time.Time `json:"expiresAt,omitempty"`
}

// CredentialsRequest is the body of POST /v1/credentials.
type CredentialsRequest struct {
	Role Role `json:"role"`
	// Request names a WorkspaceRestore the caller claimed. It authorizes the
	// restore role after the workload started, and selects the snapshot.
	Request string `json:"request,omitempty"`
}

// CredentialsResponse tells an agent everything it needs for one operation.
type CredentialsResponse struct {
	Namespace string `json:"namespace"`
	Workspace string `json:"workspace"`
	Role      Role   `json:"role"`

	Engine      Engine      `json:"engine"`
	Backend     Backend     `json:"backend"`
	Credentials Credentials `json:"credentials"`
	// RepositoryPassword encrypts this workspace's repository. The broker
	// derives a different password for every workspace.
	RepositoryPassword string `json:"repositoryPassword"`

	// Interval is how often the sidecar takes a periodic snapshot.
	Interval Duration `json:"interval"`
	// OperationTimeout bounds one snapshot or restore (the repository's
	// spec.operationTimeout). Zero when the repository does not set it: the
	// agent then uses its --operation-timeout.
	OperationTimeout Duration `json:"operationTimeout,omitempty"`
	// KeepLast and KeepDaily are the retention for maintenance.
	KeepLast  int `json:"keepLast"`
	KeepDaily int `json:"keepDaily,omitempty"`

	// For the restore role: SnapshotID pins the snapshot to restore (empty
	// means latest). SourceWorkspace is set for a fork; Backend,
	// Credentials and RepositoryPassword then describe the source.
	SnapshotID      string `json:"snapshotID,omitempty"`
	SourceWorkspace string `json:"sourceWorkspace,omitempty"`
	// LatestSnapshotID is the newest snapshot the broker recorded, sent
	// when SnapshotID is empty. The agent restores it instead of the
	// engine's newest, which a deposed writer may have written after it
	// lost the writer role. Empty: the broker has no record; use the
	// engine's newest.
	LatestSnapshotID string `json:"latestSnapshotID,omitempty"`
	// KeepIDs, for the maintenance role, are snapshots that retention
	// must keep whatever their age: the recorded newest snapshot and every
	// snapshot a fork or a rollback pins.
	KeepIDs []string `json:"keepIDs,omitempty"`
	// RestoreFromGeneration is echoed in the restore report when the
	// restore used Workspace.spec.restoreFrom, so it is applied only once.
	RestoreFromGeneration int64 `json:"restoreFromGeneration,omitempty"`
}

// WaitResponse is the answer to POST /v1/wait: WorkspaceSnapshot requests
// for the caller's workspace. The broker holds the request open until there
// is one or the poll times out.
type WaitResponse struct {
	// Requests are WorkspaceSnapshot names.
	Requests []string `json:"requests"`
	// Restores are WorkspaceRestore names.
	Restores []string `json:"restores,omitempty"`
}

// Operation names what an agent reports on.
type Operation string

const (
	OpSnapshot    Operation = "snapshot"
	OpRestore     Operation = "restore"
	OpMaintenance Operation = "maintenance"
)

// Report is the body of POST /v1/report.
type Report struct {
	Role      Role      `json:"role"`
	Operation Operation `json:"operation"`
	// SnapshotID is the snapshot taken or restored. Empty for a restore
	// that found no snapshot.
	SnapshotID string `json:"snapshotID,omitempty"`
	// Final marks the last snapshot of a pod's life, taken on SIGTERM or by
	// a rescue Job. The controller releases protected volumes only after a
	// final snapshot.
	Final bool `json:"final,omitempty"`
	// NoData, from a mover, means the volume holds no workspace data (empty,
	// or its first restore was cut off), so there is nothing to save. The
	// broker marks the volume final without recording a snapshot.
	NoData bool `json:"noData,omitempty"`
	// Reused means the agent found no change since snapshot SnapshotID and
	// is confirming it instead of taking a new one. The broker records it
	// but does not count it toward maintenance.
	Reused bool `json:"reused,omitempty"`
	// Request names the WorkspaceSnapshot this snapshot serves.
	Request string `json:"request,omitempty"`
	// SourceWorkspace is set on a restore from another workspace (fork).
	SourceWorkspace       string    `json:"sourceWorkspace,omitempty"`
	RestoreFromGeneration int64     `json:"restoreFromGeneration,omitempty"`
	StartTime             time.Time `json:"startTime"`
	EndTime               time.Time `json:"endTime"`
	Error                 string    `json:"error,omitempty"`
}

// Duration is a time.Duration that encodes as a Go duration string.
type Duration struct{ time.Duration }

func (d Duration) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

func (d *Duration) UnmarshalText(b []byte) error {
	v, err := time.ParseDuration(string(b))
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}

// WorkerState answers POST /v1/worker-state. Stop: the workload Pod's
// containers have stopped, or its Pod is gone. Gone: the Pod object no
// longer exists (or a new Pod reuses its name); after a forced deletion
// its containers may still run for up to their grace period.
type WorkerState struct {
	Stop bool `json:"stop"`
	Gone bool `json:"gone,omitempty"`
}
