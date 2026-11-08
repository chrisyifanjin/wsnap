# Operations

This document tells you how to install, configure, monitor and remove
wsnap.

## Requirements

- Kubernetes 1.30 or later: native sidecars, and the `sleep` preStop
  action of the controller (on by default from 1.30). Tested on 1.37.
- A Pod Security level of `baseline` or `privileged` in workspace
  namespaces: the agent runs as root with a few capabilities.
- A default dynamic StorageClass for the sample Pod, or set
  `storageClassName` in its volume claim. For node-local disks, use a dynamic
  local provisioner (TopoLVM, OpenEBS LVM LocalPV, rancher local-path) with
  `volumeBindingMode: WaitForFirstConsumer`.
- Object storage: S3-compatible, Google Cloud Storage, or Azure Blob. A
  shared filesystem (NFS) also works, without isolation.

## Install

There is no published release yet. Install from source on a kind cluster:

```sh
kind create cluster --name wsnap
make images
kind load docker-image wsnap-controller:dev wsnap-agent:dev --name wsnap
kubectl apply -k config/default
kubectl -n wsnap-system rollout status deployment/wsnap-controller
```

If you already have a cluster, build and push both images to a registry
that its nodes can pull from. Change the controller image and its
`--agent-image` argument in `config/manager/manager.yaml` before applying
`config/default`. For a released version, use the `install.yaml` attached
to the release instead; it pins both image tags.

The sample volume needs a StorageClass. Check yours with
`kubectl get storageclass` before creating a Pod.

### Limit which Secrets wsnap can read

By default the controller may `get` Secrets in every namespace, because
each SnapshotRepository's Secret lives in its own namespace. To allow only
the namespaces you choose, install `config/namespaced-secrets` instead of
`config/default`, and bind the reader role in each namespace that has
repositories:

```sh
kubectl apply -k config/namespaced-secrets
kubectl -n agents apply -f config/samples/secret-access.yaml
```

A repository in a namespace without the binding reports `Ready=False` with
reason `SecretForbidden`, and wsnap checks again every minute. wsnap never
lists or watches Secrets.

### Safe PVC provisioning

Pick a StorageClass whose CSI driver, encryption, topology, and reclaim
policy meet your requirements. Use `WaitForFirstConsumer` for topology-bound
storage; Kubernetes waits for a Pod's scheduling decision before provisioning.
Give each workspace Pod or Sandbox an explicit class and storage request.
wsnap does not create its primary PVC. Its temporary rescue PVC pins
`volumeName` to the already released PV and uses that PV's class, access
modes, volume mode, and capacity. A rescue needs room in the namespace quota
for one extra PVC and its requested capacity until it finishes.

An administrator can apply the example policy after adapting its namespace,
class, and limits:

```sh
kubectl apply -f config/samples/storage-policy.yaml
kubectl label namespace agents wsnap.dev/storage-policy=safe
```

The `LimitRange` caps each claim; the `ResourceQuota` caps total requested
storage and claim count. The `ValidatingAdmissionPolicy` enforces the
reviewed StorageClass in labeled namespaces. Install the policy before
labeling a namespace, and make its class match the PVs wsnap may rescue.
Only cluster administrators should edit the policy or namespace label.
Before enabling it on an existing namespace, check all of its PVCs and
workload templates for a class mismatch.

wsnap records the PVC and PV UIDs in `Workspace.status.volume`. On an
upgrade, an idle workspace whose status lacks these fields cannot start an
idle request Job: start a Pod on the existing claim first so wsnap can
verify its live binding and record the identities. A name reused for a new
PVC or PV never inherits the old status record.
Drain in-flight mover and restorer Jobs before this upgrade: older Job
registrations lack the volume identity and will be refused credentials.
Keep any affected PV retained, inspect the Job and claim, then use the
documented rescue retry procedure after the upgrade.

`wsnap-controller --version` and `wsnap-agent version` print the build.

This installs the CRDs, the controller (2 replicas, leader election, PDB),
the broker Service, the webhook Service, the
`MutatingWebhookConfiguration`, and RBAC.

The controller flags:

| Flag | Default | Meaning |
|---|---|---|
| `--agent-image` | `ghcr.io/chrisyifanjin/wsnap-agent:dev` | Image of the sidecar and the Jobs. |
| `--broker-addr` | `:8443` | Broker listen address. |
| `--broker-url` | `https://wsnap-broker.wsnap-system.svc:8443` | URL that agents use. |
| `--broker-manage-certs` | `true` | Without `--tls-cert-file`, generate the broker's certificate into the Secret `wsnap-broker-cert` and give agents its CA in `WSNAP_CA_PEM`. Set `false` (and an `http://` URL) only to serve plain HTTP. |
| `--broker-plain-addr` | empty | Also serve plain HTTP here (`:8080`) for agents started before an upgrade to TLS. |
| `--tls-cert-file`, `--tls-key-file` | empty | Serve the broker with your own certificate (cert-manager). Reloaded when the files change. |
| `--agent-ca-configmap` | empty | With your own certificate: ConfigMap (key `ca.crt`) in workload namespaces that sidecars and Jobs mount to trust the broker. |
| `--allow-automounted-tokens` | `false` | Accept Pods that mount the default API token. Do not use this with untrusted code. |
| `--pod-rate`, `--pod-burst` | `1`, `20` | Broker requests per second per Pod. |
| `--webhook-manage-certs` | `true` | Generate the webhook certificate. Set `false` to use cert-manager. |
| `--leader-elect` | `false` (`true` in the manifests) | Run reconcilers on one replica. |
| `--release-volumes` | — | Release protected PVs and exit. See [Uninstall](#uninstall). |

## Create a repository

Put the `SnapshotRepository`, its Secret, and every Pod that uses it in
the **same namespace**. The following examples use `agents`:

1. Create that namespace and a Secret with the master key and backend
   credential:

   ```sh
   kubectl create namespace agents
   kubectl -n agents create secret generic wsnap-creds \
     --from-literal=WSNAP_MASTER_KEY="$(openssl rand -hex 32)" \
     --from-literal=AWS_ACCESS_KEY_ID=... \
     --from-literal=AWS_SECRET_ACCESS_KEY=...
   ```

   Keep `WSNAP_MASTER_KEY` safe. Without it, no workspace can be restored.

2. Create a `SnapshotRepository` in `agents`. Choose one backend. The
   following fragments show the relevant `spec` fields; the S3 example
   is a complete object. For that example, edit the bucket, region and
   role ARN in `config/samples/repository-s3-sts.yaml`, then run
   `kubectl -n agents apply -f config/samples/repository-s3-sts.yaml`.

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
   kubectl -n agents get snaprepo
   # NAME      ENGINE   PROVIDER   READY
   # default   kopia    s3-sts     True
   ```

   `Ready=False` explains a configuration problem: for example, a missing
   Secret, a short master key, or an incompatible engine and provider.
   `Ready=True` does **not** test access to object storage; the first
   snapshot does that. Check `CredentialsScoped=True` to confirm that
   the agent receives scoped credentials. The `static` provider reports
   `CredentialsScoped=False` because it gives the agent the admin key.

### Schedule and retention

| Field | Default | Meaning |
|---|---|---|
| `schedule.interval` | `15m` | Time between snapshots. Minimum `10s`. Idle workspaces skip the snapshot. |
| `retention.keepLast` | `10` | Snapshots that maintenance keeps. |
| `retention.keepDaily` | `7` | Also keep the newest snapshot of each of the last N days (UTC). A burst of snapshots cannot push older days out. |
| `retention.maintenanceEvery` | `20` | Run maintenance after this many new snapshots, at most once an hour. `0` turns it off. |
| `credentials.ttl` | `1h` | Lifetime of issued credentials. AWS STS needs at least `15m`. Keep it at least 5 minutes longer than the agent's `--operation-timeout` (default `30m`); raise both for very large workspaces. |

## Protect a workload

The simplest example is [pod.yaml](https://github.com/chrisyifanjin/wsnap/blob/main/config/samples/pod.yaml), which
uses a generic ephemeral volume. Apply it in the same namespace as the
repository:

```sh
kubectl -n agents apply -f config/samples/pod.yaml
kubectl -n agents wait --for=condition=Ready pod/my-agent --timeout=5m
kubectl -n agents get workspace user-42-project-x
```

The webhook adds the sidecar and waits for the restore before starting
the workload. To protect your own Pod, add the same two labels and mount
a volume named `workspace`:

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
| `wsnap.dev/workload-grace` | Time the workload takes to exit after SIGTERM. Default `30s`. The webhook sets the grace period to this plus the final timeout plus 10 s. |
| `wsnap.dev/inject: "false"` | The Pod brings its own sidecar. The webhook checks it. |

**The workload must exit on SIGTERM.** Kubernetes stops the sidecar only
after the workload containers exit. A workload that ignores SIGTERM (for
example a bare `sleep` as PID 1, which ignores signals it has no handler
for) uses up the whole grace period, and the sidecar gets 2 seconds for its
final snapshot. A PVC-backed workspace is then rescued later; an emptyDir
workspace loses the changes since its last snapshot. Run the workload
under an init that forwards signals (`tini`, `dumb-init`) or trap TERM.

For agent-sandbox, put the labels in `Sandbox.spec.podTemplate.metadata`
and the volume in `volumeClaimTemplates`, as in
[sandbox.yaml](https://github.com/chrisyifanjin/wsnap/blob/main/config/samples/sandbox.yaml). Install agent-sandbox
before applying that sample. The webhook injects the sidecar for both
samples.

Use a generic ephemeral PVC, or a standalone PVC whose PV is retained,
when a mover must rescue a volume after its PVC is deleted without a final
snapshot. With `emptyDir`, wsnap can take a final
snapshot during a normal stop, but it cannot rescue data after the Pod
disappears. If the underlying disk is lost, any volume type can restore
only the last completed snapshot.

### Worker mode: snapshots from a separate Pod

By default the agent runs as a sidecar in the workload Pod. In worker
mode, it runs in a separate Pod instead, so the workload Pod holds no
wsnap container and no wsnap token. Create the Workspace before the
first workload Pod:

```yaml
apiVersion: wsnap.dev/v1alpha1
kind: Workspace
metadata: {name: user-42-project-x, namespace: agents}
spec:
  repositoryName: default
  execution: {mode: worker}
```

Then label the workload Pod as usual. What happens:

1. The webhook adds a gate init container (`wsnap-gate`) to the workload
   Pod. It has no credentials; it waits for a marker file on the volume.
2. When the Pod is scheduled, the controller creates a worker Job on the
   same node that mounts the same PVC.
3. The worker restores the workspace, writes the marker, and the gate
   lets the workload start.
4. The worker snapshots while the workload runs and serves requests.
5. When the workload Pod stops, the worker takes the final snapshot and
   finishes. If the worker gets SIGTERM first (a drain), it waits up to
   half its final timeout for the workload to stop.

Limits:

- The workspace volume must be a PVC or a generic ephemeral PVC that
  two Pods on one node can mount (`ReadWriteOnce` or `ReadWriteMany`). The
  webhook refuses `emptyDir` and `ReadWriteOncePod`.
- The mode cannot change after the Workspace exists. A Workspace that the
  controller created for a Pod uses sidecar mode.
- The workload starts only after its worker is scheduled and has
  restored. If the controller is down, the workload waits at the gate.

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

Health gauges, computed from the informer cache at each scrape. Every
replica serves them; use `max()`:

| Metric | Labels | Meaning |
|---|---|---|
| `wsnap_workspaces_failing` | — | Workspaces with `status.lastError` set. |
| `wsnap_volumes_rescue` | `phase` (`running`, `failed`) | Protected PVs being rescued, or kept after a failed rescue. |
| `wsnap_build_info` | `version` | Always 1. |

No metric carries a workspace or Pod name, so the series count stays
fixed as workspaces grow. `config/prometheus/rules.yaml` holds ready
alerts (Prometheus Operator): a failed rescue (critical: unsaved data
waits for an operator), a stuck rescue, failing workspaces, a high agent
error rate, and broker 503s.

Do not alert on the age of `status.lastSnapshot.time`: an idle workspace
skips its snapshots on purpose.

## Upgrade

Apply the new `install.yaml`. The controller rolls one replica at a time
(PDB), and the webhook stays available. Running sidecars keep their old
image until their Pods restart; the broker accepts older agents within one
minor version.

Upgrading to the release with writer epochs: existing PVs have no
`wsnap.dev/writer-epoch` annotation and count as epoch 0, and existing
snapshot records count as epoch 0. PVs that an earlier version marked
`wsnap.dev/rescue=skipped-newer-writer` stay as they are; remove the
annotation to rescue them (their data is kept without replacing newer
history).

Upgrading from a plain-HTTP broker (`http://...:8080`) to the TLS default:
running sidecars keep the old URL until their Pods restart. Add
`--broker-plain-addr=:8080` to the controller for the upgrade, so they can
still take their final snapshots. Remove it when every Pod started before
the upgrade is gone.

## Troubleshoot

| Symptom | Cause and action |
|---|---|
| Pod stays in `PodInitializing` | The sidecar waits until the broker admits the Pod as the writer. Read `kubectl logs <pod> -c wsnap`. A `409` means another Pod still writes the workspace, a rescue runs, or a Job works on the volume; it ends by itself. |
| Pod refused at creation | The webhook explains why in the error message. |
| `Ready=False` on a repository | Read the condition message. |
| PV stays `Released` with `wsnap.dev/rescue=failed` | The mover failed, ran out of time (2 h), or could not check the volume. Read the mover Pod's log, fix the cause, then remove the annotation to retry. |
| PV stays `Released` with `wsnap.dev/rescue=skipped-newer-writer` | Set only by versions before writer epochs. Remove the annotation; the PV is then rescued, and its data is kept without replacing newer history. |
| The broker logs "snapshot of an older writer epoch kept, but not as the newest" | A rescue saved an older writer's volume after a newer writer's snapshot. The data is in the repository under the logged snapshot ID; restore it with a `WorkspaceRestore` if you need it. |
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
