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

Found by the second independent review, and fixed:

- A Pod on a volume that already had data started its workload before the
  broker accepted it as the writer. The startup probe now checks a marker on
  the sidecar's private volume, written only after writer credentials are
  granted (`TestWarmVolumeWaitsForWriterClaimBeforeStartup`).
- A late rescue could roll back a newer writer that had already stopped.
  The controller now decides by snapshot history, not by running Pods.
- The mover trusted a marker file the workload can delete. It now skips
  only an empty volume or an interrupted restore.
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

## Known limits

- A root workload can race the staging directory during an in-place
  restore (see [security.md](security.md)).
- Gateway requests are created by the controller's service account after a
  SubjectAccessReview of the caller, so admission policies that target the
  caller do not apply to them.
