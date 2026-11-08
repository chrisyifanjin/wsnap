# API reference

## Custom resources

API group `wsnap.dev`, version `v1alpha1`. The group name is a placeholder
and can change before `v1beta1`.

All four custom resources are namespaced. Put a `SnapshotRepository`, its
Secret, and the Pods and request objects that use it in the same namespace.
The controller creates a `Workspace` when the first labeled Pod starts.

### SnapshotRepository

Where and how workspaces are stored.

| Field | Type | Meaning |
|---|---|---|
| `spec.engine.type` | `kopia` \| `restic` \| `exec` | Snapshot technology. |
| `spec.engine.command` | `[]string` | Command of the `exec` engine. |
| `spec.backend.s3` | object | `endpoint` (host[:port], default `s3.amazonaws.com`), `bucket`, `prefix`, `region`, `insecure` (HTTP). |
| `spec.backend.gcs` | object | `bucket`, `prefix`. |
| `spec.backend.azure` | object | `account`, `container`, `prefix`, `endpointSuffix`, `accessTier` (restic; `Hot`, `Cool`, `Cold`). |
| `spec.backend.filesystem` | object | `path` inside the agent container. |
| `spec.credentials.secretName` | string | Secret with `WSNAP_MASTER_KEY` and the backend key. Only the controller reads it. |
| `spec.credentials.provider` | `static` \| `s3-sts` \| `gcs-downscoped` \| `azure-sas` | How credentials are limited. Default `static`. |
| `spec.credentials.sts` | object | `endpoint`, `roleARN`, `region`. |
| `spec.credentials.azureSAS.serviceURL` | string | Overrides the blob endpoint (for an emulator). |
| `spec.credentials.ttl` | duration | Credential lifetime. Default `1h`. |
| `spec.schedule.interval` | duration | Snapshot interval. Default `15m`, minimum `10s`. |
| `spec.retention.keepLast` | int | Snapshots kept by maintenance. Default `10`. |
| `spec.retention.keepDaily` | int | Also keep the newest snapshot of each of the last N days. Default `7`. |
| `spec.retention.maintenanceEvery` | int | New snapshots between maintenance runs. Default `20`; `0` turns it off. |
| `spec.jobs.image` | string | Image for Jobs. Default: the controller's `--agent-image`. |
| `spec.jobs.volumes`, `spec.jobs.volumeMounts` | lists | Extra volumes for Jobs, for example an NFS backend. |
| `spec.jobs.securityContext` | object | Pod security context of Jobs. |
| `status.conditions` | list | `Ready` (configuration is valid), `CredentialsScoped`. |

Secret keys: `WSNAP_MASTER_KEY` (required, 16 bytes or more), and
`AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (S3), `GOOGLE_CREDENTIALS_JSON`
(GCS), or `AZURE_ACCOUNT_KEY` (Azure).

### Workspace

The durable record of one workspace. The controller creates it at the
first Pod. You can create it first, to set `restoreFrom`.

| Field | Meaning |
|---|---|
| `spec.repositoryName` | The SnapshotRepository. |
| `spec.execution.mode` | `sidecar` (default) or `worker`: the agent runs in a separate Pod on the workload's node and PVC. Cannot change after creation. |
| `spec.restoreFrom.workspaceName` | Fork: restore another workspace's snapshot into the next empty volume. |
| `spec.restoreFrom.snapshotID` | Restore this snapshot instead of the newest. |
| `status.volume` | `kind` (`persistentVolumeClaim`, `ephemeral`, `emptyDir`), `claimName`, `volumeName` of the writer's volume. |
| `status.writerPod` | The Pod that writes the workspace. |
| `status.writerEpoch` | Increases each time another Pod becomes the writer. |
| `status.lastSnapshot` | `id`, `time`, `executor` (`sidecar`, `mover`), `pod`, `volumeName`, `final`, `epoch`. `executor` is `worker` in worker mode. Restores use `id`. A snapshot of an older `epoch` never replaces it. |
| `status.lastRestore` | `snapshotID`, `sourceWorkspace`, `time`, `pod`. |
| `status.supersededSnapshots` | Rescue snapshots of older writer epochs (newest 10). Maintenance keeps them; restore one by ID with a `WorkspaceRestore`. |
| `status.lastError` | The last failure. |
| `status.snapshotsSinceMaintenance`, `status.lastMaintenance` | Maintenance bookkeeping. |
| `status.restoreFromGeneration` | The generation whose `restoreFrom` was used (or skipped because the volume already had data). |
| `status.jobs` | Jobs the controller created for this workspace, with the PVC and PV UIDs they may use and, for a worker, its workload Pod and writer epoch. The broker serves only these. |

### WorkspaceSnapshot and WorkspaceRestore

Requests. Both have `spec.workspaceName`. `WorkspaceRestore` also has
`spec.snapshotID` (empty: the newest) and `spec.sourceWorkspace` (empty:
the same workspace).

| Status field | Meaning |
|---|---|
| `phase` | `Pending`, `Running`, `Succeeded` or `Failed`. |
| `executor` | `sidecar`, `worker`, `mover` or `restorer`. |
| `claimedBy` | UID of the Pod that serves the request. |
| `snapshotID` | The snapshot taken, or the snapshot restored. |
| `message` | The reason for a failure or a wait. |

A request fails after 10 minutes without an executor. A request that a
sidecar claimed fails after 2 hours without a report, or is routed again
when the claimant Pod is gone. A restore request succeeds only after the
restored state is in a new snapshot.

### Labels and annotations

| Key | On | Meaning |
|---|---|---|
| `wsnap.dev/workspace` | Pod (label) | Workspace identity. A DNS-1123 label. |
| `wsnap.dev/repository` | Pod (label) | SnapshotRepository name. |
| `wsnap.dev/volume` | Pod | Workspace volume name. Default `workspace`. |
| `wsnap.dev/sidecar` | Pod | Sidecar container name. Default `wsnap`. |
| `wsnap.dev/inject` | Pod | `"false"`: the Pod brings its own sidecar. |
| `wsnap.dev/quiesce-timeout`, `wsnap.dev/final-timeout`, `wsnap.dev/workload-grace` | Pod | Sidecar timing and the grace period. |
| `wsnap.dev/protected`, `wsnap.dev/workspace-ref`, `wsnap.dev/original-reclaim-policy`, `wsnap.dev/writer-pod-uid`, `wsnap.dev/writer-epoch`, `wsnap.dev/final-pod-uid`, `wsnap.dev/rescue` | PV | wsnap's bookkeeping. Do not edit, except to retry a failed rescue (remove `wsnap.dev/rescue`). |
| `wsnap.dev/requested-by` | request | The user that created it through the gateway API. |

## Broker HTTP API

The broker serves HTTPS on `:8443` (`wsnap-broker.wsnap-system.svc`). See
[security.md](security.md#transport) for its certificate.

### Agent endpoints

Agents authenticate with a projected service account token (audience
`wsnap`).

| Method and path | Body | Answer |
|---|---|---|
| `POST /v1/credentials` | `{"role": "restore"\|"snapshot"\|"mover"\|"maintenance", "request": "<WorkspaceRestore>"}` | Engine, backend, scoped credential, repository password, interval, retention. For `restore`: `snapshotID` (pinned) or `latestSnapshotID` (recorded newest). For `maintenance`: `keepIDs`. |
| `POST /v1/report` | operation, role, snapshot ID, final, reused, request, error | `204`. |
| `POST /v1/wait` | `{}` | Long poll (up to 50 s): `{"requests": [...], "restores": [...]}` for the caller's workspace: new claims, and the caller's own unfinished claims again. |
| `POST /v1/worker-state` | `{}` | Worker Jobs only: `{"stop": true}` when the workload Pod has stopped (gone, finished, or replaced by a new Pod with the same name). |

Errors: 4xx means "refused; do not retry" (409: wait for another writer,
a rescue, or a Job). 503 means "try again": the API server, STS, or an
identity provider did not answer.

### Gateway endpoints

Callers authenticate with their own Kubernetes token. The broker authorizes
each call with a SubjectAccessReview.

| Method and path | Needs RBAC | Body |
|---|---|---|
| `POST /v1/namespaces/{ns}/workspaces/{ws}/snapshots` | `create workspacesnapshots` | `{"wait": true, "timeoutSeconds": 45}` |
| `POST /v1/namespaces/{ns}/workspaces/{ws}/restores` | `create workspacerestores` | `{"snapshotID": "...", "sourceWorkspace": "...", "wait": true}` |
| `POST /v1/namespaces/{ns}/workspaces/{ws}/forks` | `create workspacesnapshots` and `create workspaces` | `{"name": "child", "repository": "..."}`: snapshots the parent now, then creates the child pinned to that snapshot |
| `GET /v1/namespaces/{ns}/workspacesnapshots/{name}` | `get workspacesnapshots` | — |
| `GET /v1/namespaces/{ns}/workspacerestores/{name}` | `get workspacerestores` | — |

Answer: `{"name": "...", "phase": "...", "snapshotID": "...", "executor": "...", "message": "..."}`.
With `wait`, the broker answers when the request finishes, or after the
timeout (at most 50 s) with the current phase.

```sh
TOKEN=$(kubectl -n team-a create token platform)
kubectl -n wsnap-system get secret wsnap-broker-cert -o jsonpath='{.data.ca\.crt}' | base64 -d > broker-ca.crt
curl --cacert broker-ca.crt -X POST -H "Authorization: Bearer $TOKEN" -d '{"wait":true}' \
  https://wsnap-broker.wsnap-system.svc:8443/v1/namespaces/team-a/workspaces/user-42/snapshots
```

## Exec engine contract

The agent runs `<command> <verb>`, with the verb `init`, `snapshot`,
`latest`, `restore` or `maintain`. The environment holds `WSNAP_WORKSPACE`,
`WSNAP_DIR`, `WSNAP_SNAPSHOT_ID`, `WSNAP_KEEP_LAST`, `WSNAP_READ_ONLY`,
`WSNAP_STATE_DIR`, `WSNAP_PASSWORD`, `WSNAP_KEEP_IDS` (one snapshot ID
per line; `maintain` must keep them), `WSNAP_KEEP_DAILY` (`maintain` also
keeps the newest snapshot of each of the last N days), `WSNAP_BACKEND` (JSON),
`WSNAP_CREDENTIALS` (JSON), `WSNAP_EXCLUDE`, and the `AWS_*`,
`GOOGLE_ACCESS_TOKEN` or `AZURE_*` variables of the backend.

- `snapshot` and `latest` print `{"id": "...", "time": "<RFC 3339>"}`.
  `latest` prints nothing for an empty repository.
- `init` exits with status 3 when `WSNAP_READ_ONLY=true` and the repository
  does not exist.
- Any other nonzero status fails the operation.

`examples/exec-engine/tar-engine.sh` is a 40-line example.
