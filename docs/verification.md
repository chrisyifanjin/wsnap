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
| Mutation checks | by hand, one guard at a time | Each test fails when the guard it covers is removed. |
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

Found by the second independent review and fixed:

- A warm volume's initialized marker let the workload start before the
  broker accepted the Pod as writer. The startup probe now checks a marker
  on the sidecar's private volume, written only after writer credentials are
  granted. `TestWarmVolumeWaitsForWriterClaimBeforeStartup` holds the broker
  at a conflict and checks that startup remains blocked.

## Open issues

A second independent review found these problems. They are not fixed yet.
Each item gives the problem, the result, and the planned fix.

### Data loss

1. **A late rescue can replace newer work.**
   The controller skips a rescue only while the newer writer Pod exists.
   When that Pod has already stopped, the rescue runs.
   Result: the old data becomes the newest snapshot.
   Fix: compare the PV's writer with the workspace's last snapshot, not
   with running Pods.

2. **The workload can delete the marker that the mover trusts.**
   A root workload can delete `.wsnap` (for example with `git clean -fdx`).
   The mover then thinks the volume holds no workspace data.
   Result: the mover saves nothing, and Kubernetes deletes the PV.
   Fix: skip the snapshot only for an empty volume or an interrupted
   restore, and record initialization outside the volume.

3. **A namespace user can forge a wsnap Job.**
   The broker accepts a Job by its labels.
   A user can copy the labels into a new Job.
   Result: that user gets the maintenance (delete) credential, and can mark
   a PV as saved when it is not.
   Fix: record the UIDs of wsnap Jobs where only the controller writes,
   and check the PV's workspace before a final mark.

### Wrong status

4. **The broker accepts reports from a Pod that is not the writer.**
   Result: the last snapshot and request results can show the wrong
   snapshot.
   Fix: accept snapshot reports only from the writer Pod, and complete a
   request only for the Pod that claimed it.

5. **A request times out while its work continues.**
   The timeout counts from creation, also while a Job or sidecar works.
   Result: a large first snapshot always fails, and a restore can finish
   after its request shows "Failed".
   Fix: time out only waiting requests; limit running work by the Job
   deadline and a heartbeat.

### Availability

6. **The webhook certificate is not renewed while the controller runs.**
   The renewal reads the webhook configuration through a cache that it
   has no permission to fill.
   Result: renewal waits forever. After about 335 days, all labeled Pods
   are refused.
   Fix: read the configuration directly from the API server.

### Security and documentation

7. **A user who can edit a SnapshotRepository holds the admin credential.**
   That user can select the `static` provider, set Job images and volumes,
   and point STS or Azure endpoints to any URL.
   Fix: document this as an administrator right, or move these fields to a
   cluster-scoped object.

8. **Smaller items.**
   - With the default settings, credentials are never reused (the README
     says they are).
   - Kopia excludes every folder named `.wsnap` or `lost+found`, not only
     the top-level ones.
   - Invalid tokens are not rate limited before the TokenReview.
   - Gateway requests run as the controller, so admission policies for the
     caller do not apply.

### To check

- The final snapshot can get only 2 seconds when the workload ignores
  SIGTERM.
- Partial snapshots (unreadable files) count as final.
- The broker reads Jobs from a cache before it admits a writer.
- An unused `restoreFrom` can apply at a much later cold restore.
- A root workload can still race the staging directory.
