# Operations

This document tells you how to install, configure, monitor and remove
wsnap.

## Requirements

- Kubernetes 1.29 or later (native sidecars). Tested on 1.37.
- A StorageClass for workspace volumes. For node-local disks, use a dynamic
  local provisioner (TopoLVM, OpenEBS LVM LocalPV, rancher local-path) with
  `volumeBindingMode: WaitForFirstConsumer`.
- Object storage: S3-compatible, Google Cloud Storage, or Azure Blob. A
  shared filesystem (NFS) also works, without isolation.

## Install

```sh
make images            # or build: docker build --target controller / --target agent
kubectl apply -k config/default
```

This installs the CRDs, the controller (2 replicas, leader election, PDB),
the broker Service, the webhook Service, the
`MutatingWebhookConfiguration`, and RBAC.

The controller flags:

| Flag | Default | Meaning |
|---|---|---|
| `--agent-image` | `ghcr.io/chrisyifanjin/wsnap-agent:dev` | Image of the sidecar and the Jobs. |
| `--broker-url` | `http://wsnap-broker.wsnap-system.svc:8080` | URL that agents use. Use `https://` with TLS. |
| `--tls-cert-file`, `--tls-key-file` | empty | Serve the broker over TLS. Reloaded when the files change. |
| `--agent-ca-configmap` | empty | ConfigMap (key `ca.crt`) that Jobs mount to trust the broker. |
| `--allow-automounted-tokens` | `false` | Accept Pods that mount the default API token. Do not use this with untrusted code. |
| `--pod-rate`, `--pod-burst` | `1`, `20` | Broker requests per second per Pod. |
| `--webhook-manage-certs` | `true` | Generate the webhook certificate. Set `false` to use cert-manager. |
| `--leader-elect` | `false` (`true` in the manifests) | Run reconcilers on one replica. |
| `--release-volumes` | — | Release protected PVs and exit. See [Uninstall](#uninstall). |

## Create a repository

1. Create a Secret with the master key and the backend credential:

   ```sh
   kubectl create secret generic wsnap-creds \
     --from-literal=WSNAP_MASTER_KEY="$(openssl rand -hex 32)" \
     --from-literal=AWS_ACCESS_KEY_ID=... \
     --from-literal=AWS_SECRET_ACCESS_KEY=...
   ```

   Keep `WSNAP_MASTER_KEY` safe. Without it, no workspace can be restored.

2. Create a `SnapshotRepository`. Choose one backend.

**AWS S3 with STS (recommended on AWS).** The access key belongs to a user
that may assume `roleARN`.

```yaml
apiVersion: wsnap.dev/v1alpha1
kind: SnapshotRepository
metadata: {name: default}
spec:
  engine: {type: kopia}
  backend:
    s3: {bucket: my-workspaces, prefix: prod, region: us-east-1}
  credentials:
    secretName: wsnap-creds
    provider: s3-sts
    sts: {roleARN: arn:aws:iam::123456789012:role/wsnap-workspaces}
```

**MinIO or Ceph RGW with STS.** Use a normal user, not the MinIO root user.
The broker calls STS on the S3 endpoint.

```yaml
  backend:
    s3: {endpoint: minio.storage.svc:9000, bucket: wsnap, insecure: true}
  credentials: {secretName: wsnap-creds, provider: s3-sts}
```

**Google Cloud Storage.** The Secret key is `GOOGLE_CREDENTIALS_JSON`.
Downscoped tokens work with restic only.

```yaml
  engine: {type: restic}
  backend:
    gcs: {bucket: my-workspaces, prefix: prod}
  credentials: {secretName: wsnap-creds, provider: gcs-downscoped}
```

**Azure Blob.** The Secret key is `AZURE_ACCOUNT_KEY`. With `azure-sas`,
`container` is a name prefix; each workspace gets its own container. Use
Kopia with `azure-sas`.

```yaml
  engine: {type: kopia}
  backend:
    azure: {account: mystorage, container: wsnap}
  credentials: {secretName: wsnap-creds, provider: azure-sas}
```

**Any other tool (exec engine).** The command must exist in the agent image.
See `internal/engine/exec.go` for the contract.

```yaml
  engine: {type: exec, command: [/usr/local/bin/my-snapshot-tool]}
```

3. Check it:

   ```sh
   kubectl get snaprepo
   # NAME      ENGINE   PROVIDER   READY
   # default   kopia    s3-sts     True
   ```

   `Ready=False` explains the problem: a missing Secret, a short master key,
   or an engine and provider that do not work together.

### Schedule and retention

| Field | Default | Meaning |
|---|---|---|
| `schedule.interval` | `15m` | Time between snapshots. Minimum `10s`. Idle workspaces skip the snapshot. |
| `retention.keepLast` | `10` | Snapshots that maintenance keeps. |
| `retention.maintenanceEvery` | `20` | Run maintenance after this many new snapshots. `0` turns it off. |
| `credentials.ttl` | `1h` | Lifetime of issued credentials. AWS STS needs at least `15m`. |

## Protect a workload

Add two labels and a volume named `workspace`:

```yaml
metadata:
  labels:
    wsnap.dev/workspace: user-42-project-x
    wsnap.dev/repository: default
spec:
  containers:
  - name: agent
    image: any/agent:latest
    volumeMounts: [{name: workspace, mountPath: /home/agent}]
  volumes:
  - name: workspace
    ephemeral:
      volumeClaimTemplate:
        spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 10Gi}}}
```

The webhook adds the sidecar. Optional annotations:

| Annotation | Meaning |
|---|---|
| `wsnap.dev/volume` | Name of the workspace volume, if not `workspace`. |
| `wsnap.dev/quiesce-timeout` | Ask the agent to pause before each snapshot (see below). |
| `wsnap.dev/final-timeout` | Time limit for the final snapshot. Default `50s`. |
| `wsnap.dev/inject: "false"` | The Pod brings its own sidecar. The webhook checks it. |

For agent-sandbox, put the labels in `Sandbox.spec.podTemplate.metadata`
and the volume in `volumeClaimTemplates`.

### Quiesce handshake

With `wsnap.dev/quiesce-timeout`, the sidecar asks the agent to pause
before each snapshot:

1. The sidecar writes a token to `/workspace/.wsnap/quiesce/request`.
2. The agent finishes its writes and copies the token to `.wsnap/quiesce/ack`.
3. The sidecar takes the snapshot.
4. The sidecar writes the token to `.wsnap/quiesce/thaw`. The agent continues.

An agent that does not answer within the timeout gets a crash-consistent
snapshot.

## Ask for snapshots and restores

```sh
# Snapshot now
kubectl create -f - <<EOF
apiVersion: wsnap.dev/v1alpha1
kind: WorkspaceSnapshot
metadata: {generateName: manual-}
spec: {workspaceName: user-42-project-x}
EOF

# Roll back the running Pod to a snapshot
kubectl create -f - <<EOF
apiVersion: wsnap.dev/v1alpha1
kind: WorkspaceRestore
metadata: {generateName: rollback-}
spec: {workspaceName: user-42-project-x, snapshotID: 0a1b2c...}
EOF

kubectl get wssnap,wsrestore
```

From inside the sandbox, any process can ask for a checkpoint:

```sh
touch /workspace/.wsnap/requests/before-refactor
cat /workspace/.wsnap/results/before-refactor.json
# {"snapshotID":"...","time":"2026-11-07T16:40:13Z"}
```

Over HTTP, see the gateway API in [api.md](api.md).

## Monitor

`kubectl get workspaces` shows each workspace's volume, last snapshot and
executor. `status.lastError` holds the last failure.

Metrics on `:8081/metrics`:

| Metric | Labels | Meaning |
|---|---|---|
| `wsnap_broker_requests_total` | `endpoint`, `role`, `code` | Broker requests and their results. |
| `wsnap_credential_issue_seconds` | `role` | Time to issue a credential, including STS. |
| `wsnap_agent_reports_total` | `operation`, `result` | Snapshots, restores and maintenance: `ok`, `reused` or `error`. |

Alert on a rising `result="error"` rate, and on workspaces whose
`status.lastSnapshot.time` is older than a few intervals.

## Troubleshoot

| Symptom | Cause and action |
|---|---|
| Pod stays in `PodInitializing` | The sidecar waits. Read `kubectl logs <pod> -c wsnap`. A `409` means another Pod still writes the workspace, a rescue runs, or a Job works on the volume; it ends by itself. |
| Pod refused at creation | The webhook explains why in the error message. |
| `Ready=False` on a repository | Read the condition message. |
| PV stays `Released` with `wsnap.dev/rescue=failed` | The mover failed or ran out of time (2 h). Fix the cause, then remove the annotation to retry. |
| PV stays `Released` with `wsnap.dev/rescue=skipped-newer-writer` | A later Pod was recorded as writer on another volume, even if that Pod has stopped. The PV holds older, unsaved data; recover it by hand or delete it. |
| A writer on a lost node blocks a new Pod | The block ends 2 minutes after the old Pod's deletion deadline. |

## Uninstall

1. Stop the workspace Pods, so each takes its final snapshot.
2. Release the protected PVs:

   ```sh
   kubectl -n wsnap-system run release --rm -it --restart=Never \
     --image=<controller image> --overrides='{"spec":{"serviceAccountName":"wsnap-controller"}}' \
     -- --release-volumes
   ```

   The command restores each PV's original reclaim policy. It refuses PVs
   whose data is in no snapshot and exits with code 2. Add `--force` to
   release them anyway.
3. `kubectl delete -k config/default`.

Repository data stays in object storage. Delete it by prefix (or
container) when you no longer need it.
