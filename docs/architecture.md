# Architecture

This document explains how wsnap works. It names each part, shows how the
parts talk to each other, and describes each flow step by step.

## The problem

An agent works in a directory: the workspace. The workspace lives on a
volume in a Pod. Pods are temporary. A Pod stops when its node fails, when
the platform suspends an idle agent, or when a user deletes a sandbox. When
the Pod stops, the workspace must not be lost, and the next Pod must start
with it.

wsnap keeps a workspace alive across Pods. It stores snapshots of the
workspace volume in object storage. It restores the newest snapshot into
the next Pod before the agent starts.

## Parts

```mermaid
flowchart LR
  subgraph cluster[Kubernetes cluster]
    subgraph ctrl[wsnap-controller Deployment, 2 replicas]
      B[Broker<br/>HTTP :8080]
      W[Admission webhook<br/>:9443]
      R[Reconcilers<br/>leader only]
    end
    subgraph pod[Workspace Pod]
      S[wsnap sidecar<br/>native sidecar]
      A[Agent container<br/>any image]
      V[(Workspace volume)]
      S --- V
      A --- V
    end
    J[Jobs<br/>mover, restorer, maintenance]
    API[(Kubernetes API)]
  end
  OS[(Object storage<br/>one repository per workspace)]
  GW[Gateway or platform]

  S -- credentials, reports, long poll --> B
  J -- credentials, reports --> B
  B -- TokenReview, SubjectAccessReview --> API
  W -- injects sidecar --> pod
  R -- watches Pods, PVs, requests --> API
  R -- creates --> J
  S -- snapshots, restores --> OS
  J -- snapshots, restores --> OS
  GW -- HTTP gateway API --> B
```

| Part | Where it runs | What it does |
|---|---|---|
| Broker | controller, every replica | Gives each agent a credential for one workspace and one role. Records snapshot and restore results. Serves the long poll and the gateway API. |
| Admission webhook | controller, every replica | Adds the sidecar to Pods with the `wsnap.dev/workspace` label. Refuses Pods that it cannot protect. |
| Pod reconciler | controller, leader | Creates the Workspace object. Switches the workspace PV to the `Retain` reclaim policy. Records the Pod that writes the PV. |
| PV reconciler | controller, leader | Handles a released PV: reclaims it when its data is in a snapshot, or rescues it with a mover Job. |
| Request reconcilers | controller, leader | Routes `WorkspaceSnapshot` and `WorkspaceRestore` requests to the sidecar or to a Job. |
| Repository reconciler | controller, leader | Checks each `SnapshotRepository` and reports `Ready`. |
| Workspace reconciler | controller, leader | Starts maintenance Jobs that apply retention. |
| Sidecar | each workspace Pod | Restores before the agent starts. Takes snapshots. Takes a final snapshot when the Pod stops. Serves requests. |
| Mover Job | workspace namespace | Takes a snapshot of a volume that has no running Pod. |
| Restorer Job | workspace namespace | Restores into a volume that has no running Pod. |
| Maintenance Job | workspace namespace | Deletes old snapshots and frees storage. |

All agents (sidecar and Jobs) are one binary, `wsnap-agent`, with different
modes. The agent never reads Kubernetes objects. It gets everything from the
broker.

## Objects

| Object | Who creates it | Purpose |
|---|---|---|
| `SnapshotRepository` | administrator | The snapshot engine (Kopia, restic or exec), the storage backend, and the credential provider. |
| `Workspace` | the controller, at the first Pod | The durable record of one workspace: its volume, its writer Pod, its last snapshot and restore. It outlives every Pod. |
| `WorkspaceSnapshot` | a client | A request: take a snapshot now. |
| `WorkspaceRestore` | a client | A request: restore a snapshot into the current volume. |

A Pod joins a workspace with two labels:

```yaml
labels:
  wsnap.dev/workspace: user-42-project-x   # the workspace identity
  wsnap.dev/repository: default            # the SnapshotRepository
```

All Pods with the same `wsnap.dev/workspace` label in one namespace share
one workspace history.

## Storage layout

Each workspace has its own repository below the backend path:

```
<backend>/<prefix>/<namespace>/<workspace>/
```

With the `azure-sas` provider, each workspace has its own container
instead. Each repository is encrypted with its own password. The broker
derives the password from `WSNAP_MASTER_KEY` with HMAC-SHA256. One leaked
password opens one workspace only.

## Flows

### 1. A Pod starts

```mermaid
sequenceDiagram
  participant K as kubelet
  participant S as sidecar
  participant B as broker
  participant O as object storage
  participant A as agent container
  K->>S: start (native sidecar starts first)
  S->>S: volume has .wsnap/initialized?
  alt warm volume (initialized)
    S->>S: skip restore
  else empty volume
    S->>B: credentials(role=restore)
    B->>B: TokenReview, Pod checks, single-writer check
    B-->>S: read-only credential and the recorded newest snapshot ID
    S->>O: restore that snapshot into .wsnap/staging
    S->>S: rename staged entries into place
    S->>S: write .wsnap/initialized
  end
  S->>B: credentials(role=snapshot); claim writer, new writer epoch
  S->>S: write private startup-ready marker
  K->>S: startupProbe: wsnap-agent ready
  K->>A: start (only after the probe passes)
```

The agent container cannot start before the restore ends and the broker
accepts this Pod as the workspace writer. The sidecar restores the snapshot
that the broker recorded as newest (`status.lastSnapshot`), not the
engine's newest snapshot. A deposed writer can still upload, but the
broker never records its snapshots, so they are never restored. The startup probe checks a marker
on the sidecar's private volume, not the reusable workspace volume. A restore
that stops halfway leaves the `.wsnap/restoring` marker. The next attempt
deletes the partial data and restores again.

### 2. The Pod runs

The sidecar checks the volume at each interval. It computes a fingerprint:
path, size, mode, modification time, change time and inode of each entry.
If the fingerprint did not change, the sidecar skips the snapshot. An idle
workspace costs one directory scan per interval.

### 3. The Pod stops

1. The kubelet stops the agent container first.
2. The kubelet sends SIGTERM to the sidecar.
3. The sidecar lets a running snapshot finish.
4. The sidecar takes the final snapshot. If nothing changed, it confirms
   the last snapshot as final and uploads nothing.
5. The broker marks the PV: "the last writer took a final snapshot".

### 4. The volume is released

A PVC is deleted with its Pod (generic ephemeral volume) or with its owner
(agent-sandbox `Sandbox`). A PVC that is being deleted cannot be mounted.
So wsnap switched the PV to `Retain` earlier, and the data stays on the
released PV.

```mermaid
stateDiagram-v2
  [*] --> Bound: workspace in use (PV policy Retain)
  Bound --> Released: PVC deleted
  Released --> Reclaimed: last writer took a final snapshot
  Released --> Running: no final snapshot
  Running --> Done: mover Job saved the data, or found none
  Running --> Failed: mover Job failed, timed out (2 h), or could not check the volume
  Done --> Reclaimed
  Reclaimed --> [*]: original reclaim policy restored
  Failed --> [*]: kept for an operator
```

During a rescue, wsnap binds the released PV to a new "rescue" PVC and runs
a mover Job on it. The mover saves nothing if the volume is empty, or if
its first restore stopped halfway (`restoring` without `initialized`).
Such a volume holds no workspace data. Any other doubt fails the Job, so
the PV is kept: a mover never lets wsnap delete data it did not save.

### Writer epochs

Two volumes of one workspace can be released in any order: a Pod on a lost
node, then its replacement that crashed before its first snapshot. wsnap
orders their data with **writer epochs**:

1. Each time another Pod becomes the writer, the broker increases
   `status.writerEpoch` (in the same compare-and-set) and stamps the epoch
   on the writer's PV (`wsnap.dev/writer-epoch`).
2. A sidecar's snapshot record carries the current epoch. A mover's record
   carries its volume's epoch, copied to the Job when it is created.
3. A record of an older epoch never replaces `status.lastSnapshot`.

So every released volume is rescued, and the data of the newest writer
always ends up as `lastSnapshot`, which restores use. An older volume's
data stays in the repository, reachable by its snapshot ID.

### 5. A client asks for a snapshot or a restore

```mermaid
sequenceDiagram
  participant C as client (kubectl, SDK, gateway API)
  participant K as Kubernetes API
  participant R as request reconciler
  participant B as broker
  participant S as sidecar
  C->>K: create WorkspaceSnapshot or WorkspaceRestore
  K-->>B: informer event wakes the waiting long poll
  alt writer Pod runs
    R->>K: phase Pending
    B->>K: claim: phase Running, claimedBy = Pod UID
    B-->>S: request name
    S->>S: snapshot, or restore in place
    S->>B: report with the request name
    B->>K: phase Succeeded or Failed
  else no Pod, PVC exists
    R->>K: phase Running
    R->>K: create a mover or restorer Job on the PVC
  else nothing to act on
    R->>K: phase Failed, with the reason
  end
```

Each sidecar keeps one long-poll connection to the broker. An informer
event wakes only the waiters of that workspace. No component polls the API
server, and no component uses `kubectl exec`.

Each sidecar keeps one long-poll connection. If the answer to a poll is
lost, the next poll delivers the same claimed requests again; the sidecar
ignores the names it is serving or just served. A claim stays while the
claimant Pod exists, so a terminating sidecar can finish it.

An in-place restore first downloads the snapshot into `.wsnap/staging`.
Right before the swap, it snapshots the live data, so a rollback keeps all
work up to that moment (with `wsnap.dev/quiesce-timeout`, also writes made
during that snapshot). The swap uses two phases recorded in `.wsnap/swap`.
Phase 1 moves the live entries to `.wsnap/trash`. Phase 2 moves the staged
entries into place. A sidecar (or mover) that crashes at any point
finishes the swap when it starts again. After the swap, the sidecar takes
a new snapshot, and only then reports the request. Then the newest
snapshot always matches the volume, and `Succeeded` means the restored
state is durable.

### 6. Fork and rollback

`Workspace.spec.restoreFrom` selects what the next restore of an empty
volume uses:

- `workspaceName` set: fork. The new workspace starts from another
  workspace's snapshot. Its own snapshots go to its own repository.
- only `snapshotID` set: rollback to an older snapshot of the same
  workspace.

The broker applies `restoreFrom` once per change of the spec. It records the
generation in `status.restoreFromGeneration`.

## Roles and credentials

| Role | Who can get it | What it allows |
|---|---|---|
| `restore` | a workspace Pod before its workload starts, or a Pod that claimed a `WorkspaceRestore` | read this workspace |
| `snapshot` | the workspace's writer Pod | write this workspace; delete only engine scratch objects |
| `mover` | Pods of wsnap mover and restorer Jobs | same as `snapshot` |
| `maintenance` | Pods of wsnap maintenance Jobs | read, write and delete this workspace |

See [security.md](security.md) for how each provider limits a credential.

## Consistency rules

- **One writer.** Only one live Pod writes a workspace. The broker sets the
  writer with a compare-and-set on the Workspace status.
- **One Job per volume.** A writer waits while a mover or restorer works on
  its volume. A second request waits for the first Job. The leader
  serializes "check, then create a Job" per volume.
- **History moves forward.** `status.lastSnapshot` only takes snapshots of
  the same or a newer writer epoch.
- **Pinned snapshots stay.** Maintenance keeps the recorded newest
  snapshot and every snapshot that a fork, a rollback, or an unfinished
  restore names.
- **Restore after rescue.** A restore waits while a released volume of the
  workspace waits for its rescue snapshot.
- **Truthful history.** After any restore, the newest snapshot matches the
  volume.
- **No destructive action on a cache read.** Before the controller deletes
  data or undoes a rescue, it reads the object from the API server.
