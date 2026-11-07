# Keep an agent workspace across Pods

wsnap snapshots a Kubernetes workspace while its Pod runs. It restores
the last recorded snapshot before the next agent starts. The admission
webhook adds a sidecar to a labeled Pod, so the agent image needs no wsnap
code. A controller can run a Job to save an accessible PV after its PVC is
deleted without a final snapshot.

!!! warning "Prototype"
    wsnap uses the `wsnap.dev/v1alpha1` API. No release has been published
    yet. Install from source and expect API changes.

## Choose a workspace volume

| Volume | Normal stop | Crash without a final snapshot |
|---|---|---|
| Standalone PVC | Next Pod can reuse the PVC. | Next Pod can reuse an accessible PVC. If it is deleted and the PV is released, a mover can save it. |
| Generic ephemeral PVC | Final snapshot restores into the next Pod's new PVC. | A mover saves the released PV if accessible; otherwise restore the last completed snapshot. |
| `emptyDir` | Final snapshot restores into the next Pod. | Restore the last completed snapshot. There is no PV to rescue. |

If the volume or node is permanently lost, recovery starts from the last
completed snapshot. Writes after it may be lost. Snapshot intervals are
the intended cadence; storage or broker failures can delay a save.

## Start here

1. [Install wsnap and create a repository](operations.md#install).
2. [Protect a Pod or agent-sandbox Sandbox](operations.md#protect-a-workload).
3. [Understand the recovery flow](architecture.md#the-problem).

## Capabilities

wsnap supports Kopia, restic, and an exec engine; S3-compatible storage,
GCS, Azure Blob, and shared filesystem storage; scheduled, final, and
requested snapshots; rollback and workspace forks. The credential broker
issues a credential for a workspace and role when the backend supports
scoping. See the [security model](security.md) for each provider's limits.

The [API reference](api.md) lists the custom resources and request
contracts. The [verification guide](verification.md) explains the test
matrix and failure scenarios.
