package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Labels and annotations that connect a workload to wsnap. They go on the
// Pod, usually through the pod template of whatever creates it.
const (
	// LabelWorkspace names the workspace a Pod belongs to. Pods that carry
	// it get credentials from the broker. The value must be a DNS-1123
	// label because it also names the Workspace object.
	LabelWorkspace = "wsnap.dev/workspace"
	// LabelRepository names the SnapshotRepository in the Pod's namespace.
	LabelRepository = "wsnap.dev/repository"
	// AnnotationVolume names the Pod volume that holds the workspace data.
	// It defaults to "workspace".
	AnnotationVolume = "wsnap.dev/volume"
	// AnnotationSidecar names the container that runs the wsnap agent. It
	// defaults to "wsnap". The broker treats every other container as
	// workload when it decides whether the restore role is still allowed.
	AnnotationSidecar = "wsnap.dev/sidecar"

	// LabelJob marks Jobs the controller creates, and their Pods. The value
	// is the Job's kind: JobMover, JobRestorer, JobMaintenance or JobWorker.
	LabelJob = "wsnap.dev/job"
	// LabelJobWorkspace names the workspace of a controller-created Job.
	// Jobs do not carry LabelWorkspace, which marks workload Pods.
	LabelJobWorkspace = "wsnap.dev/job-workspace"
	// LabelManagedBy marks objects the controller owns.
	LabelManagedBy = "app.kubernetes.io/managed-by"
	// ManagedByValue is the LabelManagedBy value of wsnap-owned objects.
	ManagedByValue = "wsnap"
	// LabelProtected marks PersistentVolumes whose reclaim policy wsnap
	// switched to Retain.
	LabelProtected = "wsnap.dev/protected"
	// AnnotationWorkspaceRef on a protected PV is "<namespace>/<workspace>".
	AnnotationWorkspaceRef = "wsnap.dev/workspace-ref"
	// AnnotationOriginalReclaimPolicy on a protected PV records the policy
	// to restore once the volume's data is safe.
	AnnotationOriginalReclaimPolicy = "wsnap.dev/original-reclaim-policy"
	// AnnotationRescue on a protected PV tracks the rescue of a released
	// volume: one of the Rescue* phases.
	AnnotationRescue = "wsnap.dev/rescue"
	// AnnotationVolumeName on a mover Job names the PV it snapshots. On a
	// WorkspaceSnapshot it targets that PV instead of the writer's volume
	// (the broker uses it to save a stopped writer's volume).
	AnnotationVolumeName = "wsnap.dev/volume-name"
	// Worker Jobs identify the workload Pod whose PVC they share.
	AnnotationTargetPodName = "wsnap.dev/target-pod-name"
	AnnotationTargetPodUID  = "wsnap.dev/target-pod-uid"
	// AnnotationRequest on a mover Job names the WorkspaceSnapshot it
	// serves.
	AnnotationRequest = "wsnap.dev/request"

	// DefaultVolumeName is the default of AnnotationVolume.
	DefaultVolumeName = "workspace"
	// DefaultSidecarName is the default of AnnotationSidecar.
	DefaultSidecarName = "wsnap"
)

// EngineSpec selects the snapshot technology.
type EngineSpec struct {
	// type is the snapshot technology.
	// +kubebuilder:validation:Enum=kopia;restic;exec
	Type string `json:"type"`
	// command is the exec engine command. The agent appends the verb (init,
	// snapshot, latest, restore, maintain). The command must exist in the
	// agent image.
	// +optional
	Command []string `json:"command,omitempty"`
}

// S3Backend is any S3-compatible store: AWS S3, MinIO, Ceph RGW,
// Cloudflare R2, Wasabi, Backblaze B2, or GCS through its XML API.
type S3Backend struct {
	// endpoint is host[:port] without a scheme.
	// +kubebuilder:default="s3.amazonaws.com"
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
	Bucket   string `json:"bucket"`
	// prefix is prepended to every workspace path.
	// +optional
	Prefix string `json:"prefix,omitempty"`
	// +optional
	Region string `json:"region,omitempty"`
	// insecure uses plain HTTP. Use it only for local test stores.
	// +optional
	Insecure bool `json:"insecure,omitempty"`
}

// GCSBackend is Google Cloud Storage through its native API.
type GCSBackend struct {
	Bucket string `json:"bucket"`
	// +optional
	Prefix string `json:"prefix,omitempty"`
}

// AzureBackend is Azure Blob Storage.
type AzureBackend struct {
	Account string `json:"account"`
	// container holds all workspaces with the static provider. With the
	// azure-sas provider it is the name prefix of one container per
	// workspace.
	Container string `json:"container"`
	// +optional
	Prefix string `json:"prefix,omitempty"`
	// endpointSuffix defaults to core.windows.net.
	// +optional
	EndpointSuffix string `json:"endpointSuffix,omitempty"`
	// accessTier for new blobs (restic only). Empty uses the account
	// default.
	// +kubebuilder:validation:Enum=Hot;Cool;Cold
	// +optional
	AccessTier string `json:"accessTier,omitempty"`
}

// FilesystemBackend is a directory mounted into the agent, for example an
// NFS export. Every agent that mounts it can reach every workspace below
// it, so it offers no isolation between workspaces. Use it for development
// or single-tenant clusters; use object storage with a scoped credential
// provider otherwise.
type FilesystemBackend struct {
	// path is the mount path inside the agent container.
	Path string `json:"path"`
}

// BackendSpec is where repositories live. Set exactly one field.
// +kubebuilder:validation:XValidation:rule="[has(self.s3), has(self.gcs), has(self.azure), has(self.filesystem)].filter(x, x).size() == 1",message="set exactly one of s3, gcs, azure, filesystem"
type BackendSpec struct {
	// +optional
	S3 *S3Backend `json:"s3,omitempty"`
	// +optional
	GCS *GCSBackend `json:"gcs,omitempty"`
	// +optional
	Azure *AzureBackend `json:"azure,omitempty"`
	// +optional
	Filesystem *FilesystemBackend `json:"filesystem,omitempty"`
}

// STSSpec configures the s3-sts provider.
type STSSpec struct {
	// endpoint is the STS URL with scheme. Empty means AWS STS. For MinIO
	// and Ceph RGW use the S3 endpoint URL.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
	// roleARN is required on AWS and ignored by MinIO.
	// +optional
	RoleARN string `json:"roleARN,omitempty"`
	// +optional
	Region string `json:"region,omitempty"`
}

// AzureSASSpec configures the azure-sas provider.
type AzureSASSpec struct {
	// serviceURL overrides https://<account>.blob.<endpointSuffix>/ for the
	// broker, for example to reach the Azurite emulator.
	// +optional
	ServiceURL string `json:"serviceURL,omitempty"`
}

// CredentialsSpec says how agents get credentials.
type CredentialsSpec struct {
	// secretName is a Secret in the repository's namespace. Only the
	// controller reads it. Keys: WSNAP_MASTER_KEY (required, at least 16
	// bytes), plus AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY,
	// GOOGLE_CREDENTIALS_JSON, or AZURE_ACCOUNT_KEY for the backend.
	SecretName string `json:"secretName"`
	// provider turns the admin credential into a per-workspace, per-role
	// credential. static hands out the admin credential unchanged.
	// +kubebuilder:validation:Enum=static;s3-sts;gcs-downscoped;azure-sas
	// +kubebuilder:default=static
	// +optional
	Provider string `json:"provider,omitempty"`
	// +optional
	STS *STSSpec `json:"sts,omitempty"`
	// +optional
	AzureSAS *AzureSASSpec `json:"azureSAS,omitempty"`
	// ttl is the lifetime of issued credentials. AWS STS enforces a
	// minimum of 15 minutes.
	// +kubebuilder:default="1h"
	// +optional
	TTL metav1.Duration `json:"ttl,omitempty"`
}

// ScheduleSpec controls periodic snapshots.
type ScheduleSpec struct {
	// interval between snapshots taken by the sidecar. The sidecar skips
	// a snapshot when nothing changed, so a short interval costs only a
	// directory scan for idle workspaces.
	// +kubebuilder:default="15m"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('10s')",message="interval must be at least 10s"
	// +optional
	Interval metav1.Duration `json:"interval,omitempty"`
}

// RetentionSpec controls maintenance.
type RetentionSpec struct {
	// keepLast is the number of snapshots maintenance keeps.
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=1
	// +optional
	KeepLast int32 `json:"keepLast,omitempty"`
	// keepDaily also keeps the newest snapshot of each of the last
	// keepDaily days (UTC). It bounds what a burst of snapshots can push
	// out of history: keepLast alone counts snapshots, and the workload
	// can trigger snapshots.
	// +kubebuilder:default=7
	// +kubebuilder:validation:Minimum=0
	// +optional
	KeepDaily *int32 `json:"keepDaily,omitempty"`
	// maintenanceEvery runs a maintenance Job after this many snapshots.
	// 0 disables maintenance.
	// +kubebuilder:default=20
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaintenanceEvery int32 `json:"maintenanceEvery,omitempty"`
}

// JobsSpec configures the Jobs the controller creates.
type JobsSpec struct {
	// image runs the wsnap agent with the engine CLIs. Defaults to the
	// controller's --agent-image.
	// +optional
	Image string `json:"image,omitempty"`
	// volumes are added to Job Pods, for example the NFS export of a
	// filesystem backend.
	// +optional
	Volumes []corev1.Volume `json:"volumes,omitempty"`
	// volumeMounts are added to the agent container of Job Pods.
	// +optional
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty"`
	// securityContext of Job Pods. A mover must be able to read every file
	// in the workspace volume.
	// +optional
	SecurityContext *corev1.PodSecurityContext `json:"securityContext,omitempty"`
}

// SnapshotRepositorySpec defines where and how workspaces are snapshotted.
type SnapshotRepositorySpec struct {
	Engine      EngineSpec      `json:"engine"`
	Backend     BackendSpec     `json:"backend"`
	Credentials CredentialsSpec `json:"credentials"`
	// +optional
	Schedule ScheduleSpec `json:"schedule,omitempty"`
	// +optional
	Retention RetentionSpec `json:"retention,omitempty"`
	// +optional
	Jobs JobsSpec `json:"jobs,omitempty"`
	// operationTimeout bounds one snapshot or restore operation, in the
	// sidecar and in Jobs. Default 30m. Raise it for very large
	// workspaces, and keep credentials.ttl at least 5m longer.
	// +optional
	OperationTimeout metav1.Duration `json:"operationTimeout,omitempty"`
}

// SnapshotRepositoryStatus is the observed state of a SnapshotRepository.
type SnapshotRepositoryStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// conditions: Ready, CredentialsScoped.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// SnapshotRepository is a snapshot technology, a storage backend and a
// credential policy. Every workspace gets its own repository below the
// backend path, encrypted with its own password.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=snaprepo
// +kubebuilder:printcolumn:name="Engine",type=string,JSONPath=`.spec.engine.type`
// +kubebuilder:printcolumn:name="Provider",type=string,JSONPath=`.spec.credentials.provider`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
type SnapshotRepository struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              SnapshotRepositorySpec   `json:"spec"`
	Status            SnapshotRepositoryStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SnapshotRepositoryList is a list of SnapshotRepository.
type SnapshotRepositoryList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SnapshotRepository `json:"items"`
}

// RestoreSource selects what the next restore of an empty volume uses
// instead of this workspace's latest snapshot.
type RestoreSource struct {
	// workspaceName forks from another workspace in the same namespace.
	// Empty means this workspace, e.g. to roll back to snapshotID.
	// +optional
	WorkspaceName string `json:"workspaceName,omitempty"`
	// snapshotID pins a snapshot. Empty means the source's latest.
	// +optional
	SnapshotID string `json:"snapshotID,omitempty"`
}

// Rescue phases: the value of AnnotationRescue on a protected PV.
//
// A protected PV whose claim is deleted goes to Released with its data
// intact. If its last writer took a final snapshot, the controller
// restores the original reclaim policy and Kubernetes reclaims it; no
// rescue phase is set. Otherwise:
//
//	(none) ──▶ running ──▶ done ──▶ reclaimed
//	              │
//	              └──▶ failed (kept for an operator; remove the annotation to retry)
//
// While a PV is running, or released without a final snapshot and not yet
// running, the broker makes the workspace's next writer wait, so it cannot
// restore older data than the rescue will save.
const (
	RescueRunning = "running"
	RescueDone    = "done"
	RescueFailed  = "failed"
	// RescueSkippedNewerWriter was set by earlier versions when a newer
	// writer existed. Writer epochs made the skip unnecessary; such PVs are
	// still left for an operator.
	RescueSkippedNewerWriter = "skipped-newer-writer"
)

// Kinds of Job the controller creates: the value of LabelJob and of
// JobRef.Kind.
const (
	// JobMover snapshots a volume no Pod is using (a rescue or a request).
	JobMover = "mover"
	// JobRestorer restores a snapshot into a volume no Pod is using.
	JobRestorer = "restorer"
	// JobMaintenance applies retention to a workspace repository.
	JobMaintenance = "maintenance"
	// JobWorker takes live snapshots beside a workload in worker mode.
	JobWorker = "worker"
)

// Executors: what took a snapshot or served a request (status.executor
// of SnapshotRecord, WorkspaceSnapshot and WorkspaceRestore).
const (
	ExecutorSidecar  = "sidecar"
	ExecutorWorker   = "worker"
	ExecutorMover    = "mover"
	ExecutorRestorer = "restorer"
)

// WorkspaceSpec is the durable identity of a workspace.
// A running Pod cannot change its executor placement, so mode is immutable.
// +kubebuilder:validation:XValidation:rule="has(oldSelf.execution) ? (has(self.execution) && self.execution.mode == oldSelf.execution.mode) : (!has(self.execution) || self.execution.mode == 'sidecar')",message="execution mode is immutable"
type WorkspaceSpec struct {
	// repositoryName is the SnapshotRepository holding this workspace.
	RepositoryName string `json:"repositoryName"`
	// execution selects where live snapshots run. The default is sidecar.
	// Worker requires a PVC that a second Pod can mount and a Workspace
	// created before its first workload Pod.
	// +optional
	Execution WorkspaceExecutionSpec `json:"execution,omitempty"`
	// restoreFrom applies to the next restore after it is set or changed,
	// and only once: status.restoreFromGeneration records that it was used.
	// Create a Workspace with restoreFrom before its first Pod to fork.
	// +optional
	RestoreFrom *RestoreSource `json:"restoreFrom,omitempty"`
}

// WorkspaceExecutionSpec chooses the live snapshot agent placement.
type WorkspaceExecutionSpec struct {
	// mode is sidecar (the default) or worker (a separate Pod sharing the PVC).
	// +kubebuilder:validation:Enum=sidecar;worker
	// +kubebuilder:default=sidecar
	// +optional
	Mode string `json:"mode,omitempty"`
}

const (
	ExecutionSidecar = "sidecar"
	ExecutionWorker  = "worker"
)

// ExecutionMode returns the effective mode, including older objects that
// predate this field.
func (s WorkspaceSpec) ExecutionMode() string {
	if s.Execution.Mode == "" {
		return ExecutionSidecar
	}
	return s.Execution.Mode
}

// VolumeRef is the volume currently holding a workspace's data.
type VolumeRef struct {
	// claimName is empty for emptyDir workspaces.
	// +optional
	ClaimName string `json:"claimName,omitempty"`
	// claimUID prevents a deleted claim's replacement from inheriting its identity.
	// +optional
	ClaimUID string `json:"claimUID,omitempty"`
	// volumeName is the bound PersistentVolume.
	// +optional
	VolumeName string `json:"volumeName,omitempty"`
	// volumeUID prevents a replacement PV with the same name from being trusted.
	// +optional
	VolumeUID string `json:"volumeUID,omitempty"`
	// kind is persistentVolumeClaim, ephemeral or emptyDir.
	Kind string `json:"kind"`
}

// JobRef identifies a Job the controller created.
type JobRef struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
	Kind string `json:"kind"`
	// The volume identity a mover or restorer was created to use. Empty for
	// maintenance Jobs, which do not mount a workspace PVC.
	// +optional
	ClaimName string `json:"claimName,omitempty"`
	// +optional
	ClaimUID string `json:"claimUID,omitempty"`
	// +optional
	VolumeName string `json:"volumeName,omitempty"`
	// +optional
	VolumeUID string `json:"volumeUID,omitempty"`
	// The workload Pod a worker Job serves. The Job's annotations can be
	// edited; this record cannot.
	// +optional
	TargetPodUID string `json:"targetPodUID,omitempty"`
	// +optional
	TargetPodName string `json:"targetPodName,omitempty"`
	// writerEpoch is the epoch a worker Job claimed for its workload Pod.
	// The broker sets it; it orders a late report against a newer writer.
	// +optional
	WriterEpoch int64 `json:"writerEpoch,omitempty"`
}

// PodRef identifies one Pod incarnation.
type PodRef struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
}

// SnapshotRecord describes one snapshot.
type SnapshotRecord struct {
	ID   string      `json:"id"`
	Time metav1.Time `json:"time"`
	// executor is sidecar or mover.
	Executor string `json:"executor"`
	// +optional
	Pod *PodRef `json:"pod,omitempty"`
	// volumeName is the PV a mover snapshotted.
	// +optional
	VolumeName string `json:"volumeName,omitempty"`
	// final is true for the last snapshot of a Pod's life.
	// +optional
	Final bool `json:"final,omitempty"`
	// epoch is the writer epoch of the data in this snapshot: the
	// workspace's writerEpoch for a sidecar, the volume's epoch for a Job.
	// A snapshot of an older epoch never replaces lastSnapshot.
	// +optional
	Epoch int64 `json:"epoch,omitempty"`
}

// RestoreRecord describes the last restore.
type RestoreRecord struct {
	// snapshotID is empty when the repository had no snapshot yet.
	// +optional
	SnapshotID string `json:"snapshotID,omitempty"`
	// sourceWorkspace is set when the restore came from another workspace.
	// +optional
	SourceWorkspace string      `json:"sourceWorkspace,omitempty"`
	Time            metav1.Time `json:"time"`
	Pod             PodRef      `json:"pod"`
}

// WorkspaceStatus is what wsnap knows about a workspace.
type WorkspaceStatus struct {
	// +optional
	Volume *VolumeRef `json:"volume,omitempty"`
	// writerPod is the last Pod that received a writing credential. The
	// controller releases a protected volume only after a final snapshot
	// from this Pod (or from a mover).
	// +optional
	WriterPod *PodRef `json:"writerPod,omitempty"`
	// writerEpoch increases each time another Pod becomes the writer. The
	// broker stamps it on the writer's PV, so data on a volume can be
	// ordered against the snapshot history.
	// +optional
	WriterEpoch int64 `json:"writerEpoch,omitempty"`
	// +optional
	LastSnapshot *SnapshotRecord `json:"lastSnapshot,omitempty"`
	// supersededSnapshots are rescue snapshots of older writer epochs: data
	// of a volume that was saved after a newer writer had already
	// snapshotted. Restores never pick them, but maintenance keeps them, so
	// an operator can restore one by ID. The newest 10 are kept here;
	// remove an entry to let maintenance delete that snapshot.
	// +optional
	SupersededSnapshots []SnapshotRecord `json:"supersededSnapshots,omitempty"`
	// +optional
	LastRestore *RestoreRecord `json:"lastRestore,omitempty"`
	// +optional
	LastError string `json:"lastError,omitempty"`
	// +optional
	SnapshotsSinceMaintenance int32 `json:"snapshotsSinceMaintenance,omitempty"`
	// +optional
	LastMaintenance *metav1.Time `json:"lastMaintenance,omitempty"`
	// restoreFromGeneration is the metadata.generation whose restoreFrom
	// a restore already used.
	// +optional
	RestoreFromGeneration int64 `json:"restoreFromGeneration,omitempty"`
	// jobs lists the Jobs the controller created for this workspace. The
	// broker serves only Jobs listed here: status is written by wsnap, so
	// a namespace user cannot forge an entry by copying a Job's labels.
	// +listType=map
	// +listMapKey=uid
	// +optional
	Jobs []JobRef `json:"jobs,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Workspace is the durable record of one workspace. It outlives the Pods
// and volumes that hold the workspace at any moment. The controller creates
// it the first time it sees a Pod with the wsnap.dev/workspace label.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ws
// +kubebuilder:printcolumn:name="Repository",type=string,JSONPath=`.spec.repositoryName`
// +kubebuilder:printcolumn:name="Volume",type=string,JSONPath=`.status.volume.kind`
// +kubebuilder:printcolumn:name="Last snapshot",type=date,JSONPath=`.status.lastSnapshot.time`
// +kubebuilder:printcolumn:name="Executor",type=string,JSONPath=`.status.lastSnapshot.executor`
type Workspace struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              WorkspaceSpec   `json:"spec"`
	Status            WorkspaceStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// WorkspaceList is a list of Workspace.
type WorkspaceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Workspace `json:"items"`
}

// WorkspaceSnapshotSpec requests one snapshot.
type WorkspaceSnapshotSpec struct {
	// workspaceName in the same namespace.
	WorkspaceName string `json:"workspaceName"`
}

// Snapshot request phases.
const (
	SnapshotPending   = "Pending"
	SnapshotRunning   = "Running"
	SnapshotSucceeded = "Succeeded"
	SnapshotFailed    = "Failed"
)

// WorkspaceSnapshotStatus is the outcome of a snapshot request.
type WorkspaceSnapshotStatus struct {
	// +optional
	Phase string `json:"phase,omitempty"`
	// claimedBy is the UID of the Pod (sidecar or Job) serving the request.
	// +optional
	ClaimedBy string `json:"claimedBy,omitempty"`
	// +optional
	SnapshotID string `json:"snapshotID,omitempty"`
	// executor is sidecar (a Pod was running) or mover (the volume was idle).
	// +optional
	Executor string `json:"executor,omitempty"`
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
}

// WorkspaceSnapshot asks for a snapshot now, from any client: a platform
// API, an SDK, kubectl or a scheduler. A running Pod's sidecar takes it; if
// no Pod runs and the workspace volume is a PVC, a mover Job takes it.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=wssnap
// +kubebuilder:printcolumn:name="Workspace",type=string,JSONPath=`.spec.workspaceName`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Snapshot",type=string,JSONPath=`.status.snapshotID`
// +kubebuilder:printcolumn:name="Executor",type=string,JSONPath=`.status.executor`
type WorkspaceSnapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              WorkspaceSnapshotSpec   `json:"spec"`
	Status            WorkspaceSnapshotStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// WorkspaceSnapshotList is a list of WorkspaceSnapshot.
type WorkspaceSnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WorkspaceSnapshot `json:"items"`
}

// WorkspaceRestoreSpec requests a restore of a workspace's current volume.
type WorkspaceRestoreSpec struct {
	// workspaceName in the same namespace.
	WorkspaceName string `json:"workspaceName"`
	// snapshotID to restore. Empty means the source's latest snapshot.
	// +optional
	SnapshotID string `json:"snapshotID,omitempty"`
	// sourceWorkspace restores another workspace's snapshot into this one.
	// Empty means this workspace (a rollback).
	// +optional
	SourceWorkspace string `json:"sourceWorkspace,omitempty"`
}

// WorkspaceRestore replaces the contents of a workspace's volume with a
// snapshot, from any client. A running writer Pod's sidecar restores in
// place (pausing the agent first if the quiesce handshake is enabled); an
// idle PVC is restored by a Job. Afterwards the restored state is
// snapshotted, so "latest" matches the volume. The status reuses
// WorkspaceSnapshotStatus; snapshotID is the snapshot that was restored.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=wsrestore
// +kubebuilder:printcolumn:name="Workspace",type=string,JSONPath=`.spec.workspaceName`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Snapshot",type=string,JSONPath=`.status.snapshotID`
// +kubebuilder:printcolumn:name="Executor",type=string,JSONPath=`.status.executor`
type WorkspaceRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              WorkspaceRestoreSpec    `json:"spec"`
	Status            WorkspaceSnapshotStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// WorkspaceRestoreList is a list of WorkspaceRestore.
type WorkspaceRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WorkspaceRestore `json:"items"`
}

// Request is implemented by WorkspaceSnapshot and WorkspaceRestore, which
// share routing and status handling.
// +kubebuilder:object:generate=false
type Request interface {
	GetWorkspaceName() string
	GetRequestStatus() *WorkspaceSnapshotStatus
}

func (r *WorkspaceSnapshot) GetWorkspaceName() string                   { return r.Spec.WorkspaceName }
func (r *WorkspaceSnapshot) GetRequestStatus() *WorkspaceSnapshotStatus { return &r.Status }
func (r *WorkspaceRestore) GetWorkspaceName() string                    { return r.Spec.WorkspaceName }
func (r *WorkspaceRestore) GetRequestStatus() *WorkspaceSnapshotStatus  { return &r.Status }
