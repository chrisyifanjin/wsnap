# Verification

This document says what is tested, how to run each test, and what the
tests found.

## Layers

| Layer | Command | What it proves |
|---|---|---|
| Unit | `go test ./...` | Each package's logic, including every accept and deny path of the broker and the webhook, every state of the rescue and request state machines, and agent data safety (crash at each point of a swap, termination during a snapshot, symlink attacks). |
| Engine, filesystem | `go test ./internal/engine` | Kopia, restic and the exec example: create, two snapshots, read-only restore, retention, excluded state, unreadable files. |
| Engine, S3 with STS | see below (MinIO) | Each role's credential does exactly what it should against real session policies. |
| Engine, second S3 | see below (SeaweedFS) | The S3 path does not depend on MinIO. |
| Engine, GCS | see below (fake-gcs-server) | Kopia with a service account file; restic with an access token (the `gcs-downscoped` path). |
| Engine, Azure | see below (Azurite over TLS) | Kopia with an account key and with `azure-sas`; restic with an account key. Production-style URLs. |
| Credentials | `go test ./internal/credentials` | Session policies, Credential Access Boundaries, Azure SAS against Azurite. |
| Race detector | `go test -race ./internal/...` | No data races in the agent, broker, controller and providers. |
| Mutation checks | a script that removes one guard at a time | Each test fails when the guard it covers is removed. |
| End to end | `test/e2e/run.sh` | The whole system on kind, with agent-sandbox, MinIO, and failures injected. |

## How to run the store tests

```sh
# MinIO with STS (use a non-root MinIO user)
WSNAP_TEST_S3_ENDPOINT=127.0.0.1:9000 WSNAP_TEST_S3_BUCKET=wsnap \
WSNAP_TEST_S3_ACCESS_KEY=broker WSNAP_TEST_S3_SECRET_KEY=... go test ./internal/engine

# A second S3 implementation, static credentials
WSNAP_TEST_S3_STATIC_ENDPOINT=127.0.0.1:8333 WSNAP_TEST_S3_STATIC_BUCKET=wsnap-sw go test ./internal/engine

# GCS emulator
STORAGE_EMULATOR_HOST=127.0.0.1:4443 WSNAP_TEST_GCS_BUCKET=wsnap-gcs go test ./internal/engine

# Azure SAS provider against Azurite
WSNAP_TEST_AZURITE=http://127.0.0.1:10000/devstoreaccount1 go test ./internal/credentials
```

The Azure engine tests need TLS and production-style host names, and Go on
macOS ignores `SSL_CERT_FILE`. Run them in Linux, inside the agent image:

```sh
GOOS=linux CGO_ENABLED=0 go test -c -o engine.test ./internal/engine
# Azurite with --cert/--key on :10443; *.localtest.me resolves to 127.0.0.1
docker run --rm --network container:<azurite> -v $PWD:/t --entrypoint /t/engine.test \
  -e SSL_CERT_FILE=/t/cert.pem -e WSNAP_TEST_AZURE_SUFFIX=localtest.me:10443 \
  -e WSNAP_TEST_AZURE_KEY=<azurite key> wsnap-agent:dev -test.run 'TestRoundTrip/.*/azure'
```

## Real clouds

`.github/workflows/cloud-conformance.yml` runs the engine round trip
(snapshot, restore, maintenance) and `TestScopedIsolation` (workspace b's
credential cannot open workspace a's repository) against real AWS S3,
Google Cloud Storage and Azure Blob, each with its scoped provider. It
runs weekly and on demand. A cloud runs only when its repository
variables are set. The login uses GitHub OIDC, so GitHub stores no cloud
key. Use a dedicated bucket or account, with a lifecycle rule that deletes
`conformance/` objects after one day.

**Status:** the workflow has not run against a real cloud yet. Until it
does, the tables above are the evidence: S3 with STS against MinIO, GCS
against an emulator, and Azure against Azurite.

| Cloud | Repository variables | Cloud-side setup |
|---|---|---|
| AWS (`s3-sts`) | `WSNAP_AWS_CI_ROLE_ARN`, `WSNAP_AWS_REGION`, `WSNAP_AWS_BUCKET`, `WSNAP_AWS_WORKSPACE_ROLE_ARN` | The CI role trusts the GitHub OIDC provider (`token.actions.githubusercontent.com`, subject `repo:<owner>/wsnap:*`) and may `sts:AssumeRole` the workspace role. It plays the broker. The workspace role trusts the CI role and allows `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` and `s3:ListBucket` on the bucket. wsnap's session policy narrows that to one workspace and role. |
| GCP (`gcs-downscoped`) | `WSNAP_GCP_WORKLOAD_IDENTITY_PROVIDER`, `WSNAP_GCP_SERVICE_ACCOUNT`, `WSNAP_GCP_BUCKET` | A workload identity pool with a GitHub provider. The service account has `roles/storage.objectAdmin` on the bucket and grants `roles/iam.workloadIdentityUser` to the repository's principal set. The tests use the external-account credential file as the broker's `GOOGLE_CREDENTIALS_JSON`. |
| Azure (`azure-sas`) | `WSNAP_AZURE_CLIENT_ID`, `WSNAP_AZURE_TENANT_ID`, `WSNAP_AZURE_SUBSCRIPTION_ID`, `WSNAP_AZURE_ACCOUNT` | An app registration with a federated credential for the repository, and the role `Storage Account Key Operator Service Role` on the storage account. The workflow reads the account key, as the broker's `AZURE_ACCOUNT_KEY`. Each run creates per-workspace containers; delete old ones from time to time. |

## End-to-end scenarios

`test/e2e/run.sh` runs on a kind cluster. Use three nodes for the
multi-node scenario: `kind create cluster --name wsnap --config test/e2e/kind-multinode.yaml`.

| Scenario | What it does |
|---|---|
| `setup` | Installs agent-sandbox, MinIO and wsnap. Checks that repositories report `Ready` and `CredentialsScoped`. |
| `webhook` | Injects the sidecar into a bare Pod and into an agent-sandbox Sandbox. Refuses unsafe Pods. With the controller stopped, refuses labeled Pods and admits unlabeled ones. |
| `gateway` | The REST API with RBAC (200, 403, 401). A request file written through the agent-sandbox `sandbox-router` and `sandboxd`. |
| `pvc` | Kopia with STS: PV protection, final snapshot, warm resume, maintenance after changed snapshots, reclaim, cold restore. |
| `ephemeral` | restic with STS: a crashed Pod (sandbox killed on its node), the PVC deleted with it, a mover rescue, restore in a new Pod. |
| `sandbox` | agent-sandbox `Sandbox`: suspend, resume, delete, re-create. |
| `emptydir` | The exec engine with a filesystem backend. A final snapshot that confirms an unchanged workspace. The workload container gets 401 from the broker. |
| `singlewriter` | A second Pod waits for the first, then restores its final snapshot within seconds. |
| `requests` | Snapshot and restore requests served by the sidecar and by Jobs, a request file, an in-place rollback, a fork across engines. |
| `chaos` | Controller stopped while a Pod dies, leader killed during a rescue, storage stopped and started, sidecar killed with SIGKILL, a crash with an immediate replacement Pod. |
| `tls` | The broker over HTTPS: sidecars and Jobs verify its CA; an untrusted CA and plain HTTP are refused. |
| `multinode` | Restore on another node, a mover that follows a node-local PV, a drain that takes a final snapshot. |
| `worker` | Worker mode: the workload Pod gets only a gate; a worker on its node serves a live request, takes the final snapshot with the last write after the workload stops, and restores a new workload on a new PVC. A `ReadWriteOncePod` claim is refused. |
| `runtimes` | Installs a pinned, checksum-verified gVisor release on every node and a `gvisor` RuntimeClass. Sidecar mode with the workload and the sidecar inside one gVisor sandbox: request snapshot, final snapshot, cold restore. Worker mode with a gVisor workload and a default-runtime worker sharing the PVC: live snapshot, final snapshot with the last write, restore. Each Pod's kernel is checked to be gVisor. |
| `kata` | Installs Kata Containers 4.2.0 (Rust runtime, QEMU; checksum verified) on one node with a `kata` RuntimeClass, and runs the same checks as `runtimes`: sidecar mode inside the microVM, and a default-runtime worker beside a Kata workload sharing the PVC over virtio-fs. Each Pod is checked to run its own guest kernel. Needs KVM; CI loads `vhost_vsock` before creating the cluster and enlarges the Kata node's `/dev/shm` (Kata keeps guest RAM there; Docker's default is 64 MiB). |

The scenarios form six groups that share no state:

| Group | Scenarios |
|---|---|
| `controlplane` | `webhook`, `gateway`, `tls`, `multinode` |
| `volumes` | `pvc`, `ephemeral`, `sandbox`, `emptydir`, `singlewriter`, `requests` |
| `failures` | `chaos` |
| `worker` | `worker` |
| `runtimes` | `runtimes` |
| `kata` | `kata` |

`test/e2e/run.sh` with no arguments runs `setup` and then all six groups
on one cluster. `test/e2e/run.sh setup volumes` runs one group. CI runs
each group on its own cluster at the same time. When a run fails, the
script prints the Pods, volumes, wsnap objects, events and logs, and CI
uploads the kind node logs.

The harness uses `kubectl exec` only to act as the agent (to write files).
wsnap itself never uses exec.

## What the verification found

The tests and two independent reviews found these bugs. Each fix has a
test that fails without it.

Found by the end-to-end runs:

- The controller read a just-created Job from a stale cache, took it for
  missing, and reset a running rescue. The PV stayed bound forever. The same
  pattern existed on the request path.
- A long poll outlived its Pod: the Pod's network went away without closing
  the TCP connection, and the broker claimed a request for a dead Pod.
- Kopia shared one cache directory between two repositories.
- Kopia wrote its GCS credentials file before creating its directory, so
  Kopia with GCS failed on first use.
- A restorer Job could ask the broker before its request was marked
  Running, and was refused.
- A Pod waiting for another writer backed off to one minute, so it started
  long after the writer stopped.

Found by the first independent review:

- A rescue could store an empty or half-restored volume as the newest
  snapshot.
- Two Pods starting at once could both become writers (check, then set).
- A rollback to an older snapshot was undone when the Pod stopped without
  changes.
- A final snapshot could be recorded on another Pod's PV.
- Request Jobs could run two at a time on one volume, and a timed-out
  request left its Job running.
- A failed rescue restarted by itself after the Job's TTL.
- A stale cache read could make the controller delete a PV.
- The workload could plant symlinks in `.wsnap` that the root sidecar
  followed.
- Debug containers and shared process namespaces could reach the sidecar's
  secrets.
- The S3 list permission matched sibling workspaces' keys.
- The webhook certificate was renewed only at start, and a rotation could
  break one replica.
- A writer on a lost node blocked replacements forever.
- A cached credential could expire during a long snapshot.
- restic's exit code 3 (some files unreadable) failed every snapshot.

Found by the second independent review, and fixed:

- A Pod on a volume that already had data started its workload before the
  broker accepted it as the writer. The startup probe now checks a marker on
  the sidecar's private volume, written only after writer credentials are
  granted (`TestWarmVolumeWaitsForWriterClaimBeforeStartup`).
- A late rescue could roll back a newer writer that had already stopped.
  The controller then decided by snapshot history. (The third review
  replaced this with writer epochs.)
- The mover trusted a marker file the workload can delete. It now skips
  only an empty volume or an interrupted first restore.
- A namespace user could forge a wsnap Job and get the delete credential.
  The broker now serves only Jobs that the controller recorded in the
  Workspace status, and marks only PVs of the caller's workspace as saved.
- Reports from a Pod that was not the writer, or from a Pod that did not
  claim the request, changed the status. They are now refused.
- A request timed out while its Job was still running. Only waiting
  requests time out now.
- The webhook certificate renewal waited forever on a cache it could not
  fill. It now reads from the API server.
- During a controller rollout, the webhook could be unreachable for a few
  seconds (found by the end-to-end TLS scenario). Readiness now includes
  the webhook, and a terminating Pod keeps serving for 10 seconds.
- The termination grace period did not leave the final snapshot enough
  time when the workload exits slowly. It now adds a workload budget
  (`wsnap.dev/workload-grace`, default 30 s).
- An unused `restoreFrom` could apply at a much later cold restore. A Pod's
  admission now uses it up.
- Kopia's automatic maintenance could run under the sidecar's no-delete
  credential. Maintenance now has its own Kopia identity.
- Kopia excluded nested folders named `.wsnap` or `lost+found`. The rules
  are now anchored at the snapshot root.
- Rejected tokens caused a TokenReview each time. They are remembered for
  10 seconds.

Found by the third independent review, and fixed. Five of these were
first shown by the reviewer's own probe tests.

- **The mover failed open.** Any error while it checked the volume, for
  example `.wsnap` owned by another uid because the workload Pod set
  `runAsUser`, meant "nothing to save", and the controller then reclaimed
  the PV. Now an error fails the Job, and the PV stays `RescueFailed`
  (`TestMoverFailsClosed`, `TestNotWorkspaceDataReportsErrorsAsErrors`).
  Every agent runs as uid 0, whatever the Pod says, and a root agent takes
  over a `.wsnap` that another uid created
  (`TestRootAgentTakesOverForeignStateDir`, run as root in the agent image).
- **A stale `restoring` marker** (a crash between two marker writes) made
  every later rescue skip the volume. The mover now skips only when
  `restoring` exists without `initialized`, and a warm start removes the
  stale marker (`TestMoverSavesAfterCrashBetweenMarkers`). A volume left
  mid-swap is finished and saved, not skipped.
- **The history-based rescue skip lost work.** If the newest writer
  crashed before its first snapshot, its volume was skipped, and the next
  Pod restored the previous writer's data. Now each new writer starts a
  **writer epoch**. The broker stamps it on the writer's PV and on each
  snapshot record, and a snapshot of an older epoch never replaces
  `lastSnapshot`. Every released volume is rescued, and restores use the
  broker's `lastSnapshot`, not the engine's newest snapshot
  (`TestWriterEpochs`, `TestOlderEpochNeverReplacesLatest`,
  `TestOlderEpochVolumeIsRescued`). This also fences a deposed writer: while
  the broker has a record, its late uploads are not restored.
- **The forged-Job fix was bypassable.** A bare Pod with an
  `ownerReference` to a registered Job got the Job's role, even after the
  Job finished. A Job pre-created under wsnap's predictable name was
  registered. Now a Job Pod must run the Job's template (every container
  that can read the token, every template volume), and the Job must be
  unfinished. A same-name Job that wsnap never registered is deleted and
  created again (`TestJobPodMustRunTheJobTemplate`,
  `TestUnregisteredJobWithOurNameIsReplaced`).
- **Transient API errors became final 4xx answers.** A TokenReview outage
  gave 401 and was cached; a failed Pod read gave 403; a failed Secret read
  gave 424. The agent does not retry 4xx, so a final snapshot was lost.
  Upstream failures now answer 503, and outages are never cached
  (`TestTransientAPIErrorsAreRetryable`).
- **A claimed request whose answer was lost stayed Running for 2 hours.**
  The long poll now delivers the caller's own Running claims again; the
  sidecar ignores names it is serving or just served
  (`TestWaitAndCompleteRequests`).
- **The controller failed a request that a terminating sidecar was still
  completing.** A claim now stays while the claimant Pod exists
  (`TestTerminatingClaimantKeepsItsClaim`).
- **A rollback destroyed unsaved work, and Succeeded came too early.** An
  in-place restore and a restorer Job now snapshot the live data first,
  and report the request only after the restored state is in a snapshot
  (`TestRollbackSavesLiveWorkFirst`).
- **Maintenance could delete a pinned snapshot.** A fork's or rollback's
  pinned snapshot, an unfinished `WorkspaceRestore`'s snapshot, and the
  recorded newest snapshot are now kept by all three engines
  (`TestMaintenanceKeepsPinnedSnapshots`; `TestRoundTrip` with real Kopia,
  restic and the exec example).
- **The credential cache never hit** with the defaults (1 h credentials,
  1 h operation limit). The default operation limit is now 30 minutes
  (`TestCredentialsAreReusedWithDefaults`).
- **Kopia made the writer the maintenance owner** at `repository create`
  (measured with Kopia 0.23.1). The agent now hands ownership to the
  maintenance identity at once (`TestKopiaWriterDoesNotOwnMaintenance`).
- **A long restore could never finish.** The startup probe gave up after
  30 minutes and restarted the restore from zero. It now allows 4 hours.
- **Two Jobs could start on one volume** (the reviewer's suspicion,
  confirmed with a test): the snapshot and restore reconcilers run at the
  same time, and both could pass "no other Job on this volume". Job
  creation is now serialized per volume in the leader
  (`TestSnapshotAndRestoreNeverStartTwoJobsOnOneVolume`: 2 Jobs without the
  lock, 1 with it).

Each fix was checked with a mutation: with the fix removed, its test
fails. 22 mutations are killed. One mutation first survived, because
another guard caught the same input earlier; a direct test of the guard
now kills it.

Found by the fourth independent review, and fixed:

- **A lost report let the controller delete a rescued PV.** A Job's
  report failure only logged, the Job completed, and the PV was reclaimed
  while `lastSnapshot` still named older data. Jobs now retry reports for
  5 minutes and fail without them, a mover reports "nothing to save"
  explicitly, and a rescue counts as done only when the broker's final
  mark is on the PV (`TestJobFailsWhenItsReportIsLost`,
  `TestCompleteJobWithoutRecordedSnapshotKeepsTheVolume`).
- **A Job could run beside a starting or stopping writer** on a generic
  ephemeral volume. The controller now waits for any wsnap Pod that mounts
  the claim (including `<pod>-<volume>` claims), the broker refuses Job
  credentials while such a Pod exists, and a sidecar is admitted before it
  finishes a leftover swap (`TestJobWaitsForAStartingOrStoppingWriter`,
  `TestJobWaitsForWorkloadOnItsVolume`).
- **Untrusted code could wipe history** by triggering about 30 snapshots,
  which started count-based maintenance. Retention now also keeps the
  newest snapshot of each of the last 7 days (`retention.keepDaily`),
  maintenance runs at most hourly, and request-file snapshots are spaced
  30 s apart (`TestRetentionKeepsDailyHistory`,
  `TestMaintenanceRunsAtMostHourly`, `TestRequestFilesAreSpaced`).
- **A rollback lost writes made during the download.** The safety
  snapshot now runs after the download, right before the swap
  (`TestRollbackKeepsWritesMadeDuringTheDownload`).
- **A new Pod restored stale data while a failed writer's volume was still
  bound** (a hard eviction gives no final snapshot). The broker now asks
  for a snapshot of exactly that volume and makes restores wait for it
  (`TestRestoreWaitsForAStoppedWritersBoundVolume`,
  `TestTargetedRequestSnapshotsThatVolume`).
- **Superseded rescue snapshots were pruned** although the docs said they
  stay. They are now kept in `status.supersededSnapshots` (newest 10) and
  pinned against maintenance.
- **A missing repository started an empty workspace** even when the broker
  had recorded a snapshot. It is now an error
  (`TestMissingRepositoryWithRecordedSnapshotFails`).
- **A lost Workspace-create race ignored `restoreFrom`.** The broker now
  re-reads the winner's object (`TestLostCreateRaceUsesTheWinnersWorkspace`).
- **The workload could mount an API token or the sidecar's state volume**
  (which holds the engine configuration with storage credentials). The
  webhook and the broker now share one isolation check that refuses both
  (`TestWorkloadCannotReachAgentSecretsOrTheAPI`).
- **Some outages still answered 4xx** (gateway TokenReview and access
  reviews, restore-request reads). They now answer 503.
- **The workload kept the API token** (found by the end-to-end suite
  while checking the fix above). The ServiceAccount admission plugin runs
  before mutating webhooks, so a Pod that did not set
  `automountServiceAccountToken` itself already had `kube-api-access-*`
  mounted into every container; the webhook's `false` came too late, and
  the broker checked only the field. The webhook now removes API-token
  volumes and their mounts (`TestAdmissionPluginTokenIsRemoved`).
- Restored data and swap renames are flushed to disk before their
  completion markers, and a full Job registry drops entries of Jobs that
  are gone before those of running Jobs (`TestJobRegistryKeepsRunningJobs`).

Found by follow-up Kopia probes, and fixed:

- Kopia's built-in retention removed older snapshots after a burst of
  saves, before wsnap could apply its own retention or protect a fork or
  rollback target. Every Kopia snapshot is now pinned at creation; wsnap's
  maintenance explicitly deletes those outside its retention set
  (`TestKopiaKeepsSnapshotsUntilWsnapMaintenance`).
- A crash after Kopia created a repository but before it set the ignore
  policy left later snapshots capturing `.wsnap` state. A writer now
  finishes policy and maintenance-owner setup when it reconnects to an
  existing repository (`TestKopiaRepairsInterruptedInitialization`).

Found by the fifth independent review, and fixed:

- **A copied Job Pod could read the delete credential** from the state
  volume (Kopia's `repository.config`). An added container in a Job Pod
  may now mount no template volume but ConfigMaps
  (`TestJobPodMustRunTheJobTemplate`).
- **Garbage tokens caused unlimited TokenReviews.** Failed logins are now
  limited per client IP (429), and cache-miss TokenReviews have a global
  cap (`TestGarbageTokensDoNotFloodTheAPIServer`).
- **Kopia's own retention deleted pre-upgrade snapshots** that had no pin.
  The global policy now turns Kopia's retention off; wsnap's maintenance
  decides (`TestKopiaKeepsSnapshotsFromBeforePins`: 10 of 18 snapshots
  survived without the fix).
- **`--operation-timeout` could not be set.** `spec.operationTimeout` on
  the repository now reaches the sidecar and Jobs, Job deadlines grow with
  it, and validation refuses a credential TTL that would expire mid
  operation.
- **A partial mover snapshot counted as final.** Snapshots that skipped
  unreadable files (Kopia's ignored errors, restic exit 3, exec
  `"partial": true`) are recorded but never final, and a mover fails, so
  the PV is kept (`TestPartialMoverSnapshotKeepsTheVolume`).
- A targeted rescue request is never claimed by a sidecar, and the Pod
  controller reads the Pod live before it marks a PV's writer.

Found by the sixth independent review, and fixed:

- **The forged-Job fix could be bypassed with an exec hook or probe** on
  the agent container, which runs inside it and can read the token. Job
  Pods must now match the template's hooks, probes, working directory and
  TTY settings too (`TestJobPodMustRunTheJobTemplate`).
- **A writer on a lost node: the rescue was never asked for.** The broker
  now asks for it, without blocking the replacement; when it lands it is
  kept as superseded (`TestLostWriterVolumeIsRescuedWithoutBlocking`).
- **Rescue failures were silent.** The alert read `status.lastError`,
  which the next success clears. New gauges count failed rescue requests
  and workspaces with superseded snapshots until an operator acts
  (`WsnapRescueRequestFailed`, `WsnapSupersededSnapshots`).
- **A restore whose report was lost ran twice**, rolling back work done
  after it. The sidecar now records completed restores on its private
  volume and resends the saved report (`TestRestoreRequestRunsOnce`).
- **A sidecar not yet admitted could clear a volume** another Pod was
  restoring into. It is now admitted first.
- **A partial safety snapshot was followed by the swap**, deleting files
  in no snapshot. It now stops the restore
  (`TestPartialSafetySnapshotKeepsLiveData`).
- **"Remove the annotation to retry" did not retry** a failed rescue. It
  now starts a new mover (`TestRemovingFailedAnnotationRetriesTheRescue`).
- **The workload could lock its sidecar out** with bad tokens (the per-IP
  limit added after review 5). Only TokenReviews of unknown tokens are
  limited now; a known valid token always passes
  (`TestGarbageTokensDoNotFloodTheAPIServer`).
- **CI skipped the real-engine tests.** CI and releases now install Kopia
  and restic at the Dockerfile's versions.
- Documented: TLS is required with untrusted workloads (`CAP_NET_RAW`),
  and the workload must exit on SIGTERM. The second was found by the
  end-to-end suite, whose test workload ignored SIGTERM.

PVC identity and provisioning checks:

- `Workspace.status.volume` records the claim and PV UIDs as well as their
  names. A writer cannot receive credentials until both sides of the
  binding agree. Protection refuses a PV already owned by another
  workspace (`TestResolveVolumeRequiresClaimAndPVIdentity`).
- An idle request Job refuses a replacement claim with the same name;
  a registered mover or restorer checks the same identity again when its
  Pod asks for credentials. The controller records that identity in
  Workspace status, so editing the Job's annotations cannot change it
  (`TestIdleRequestRejectsReplacedClaim`, `TestJobRoles`).
- A rescue PVC is pinned to its released PV and the PV is pre-bound to
  the new claim UID. A preexisting same-name claim cannot cause a mover
  Job to start (`TestRescueRefusesPreexistingSameNameClaim`).
- The sample `LimitRange`, `ResourceQuota`, and StorageClass admission
  policy were applied to an isolated Kubernetes 1.37 namespace. A claim
  using the reviewed class was admitted; an unreviewed class and a 20Gi
  claim above the 10Gi limit were rejected.

## Known limits

- A root workload can race the staging directory during an in-place
  restore (see [security.md](security.md)).
- Gateway requests are created by the controller's service account after a
  SubjectAccessReview of the caller, so admission policies that target the
  caller do not apply to them.
- If a Workspace moves to another `SnapshotRepository`, its
  `lastSnapshot` names a snapshot in the old repository. Clear
  `status.lastSnapshot` when you move a workspace.
- A restore must finish within `--operation-timeout` (default 30 minutes),
  and the repository's `credentials.ttl` must be at least 5 minutes longer.
  Raise both for very large workspaces.
- If the controller stops after creating a rescue PVC but before recording
  its UID on the PV, it leaves the PV retained and refuses an automatic
  retry. An operator must inspect and remove that orphaned rescue PVC
  before retrying; it will not mount a same-name claim without proof of
  ownership.
