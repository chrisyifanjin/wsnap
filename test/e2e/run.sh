#!/usr/bin/env bash
# End-to-end test on a kind cluster. It exercises every executor, every
# engine, both scoped and static credentials, and four kinds of workspace
# volume:
#
#   pvc        Pod + standalone PVC        kopia  + S3 (STS)   warm resume, cold restore, maintenance
#   ephemeral  generic ephemeral volume    restic + S3 (STS)   no sidecar: rescue by a mover Job
#   sandbox    agent-sandbox Sandbox       restic + S3 (STS)   suspend/resume, delete, recreate
#   emptydir   emptyDir                    exec   + filesystem final snapshot on SIGTERM, reused final
#   singlewriter two Pods, one workspace       kopia  + S3 (STS)   second Pod waits, then restores the first's final
#   requests   control-plane CRs and data-plane files: capture/restore in the Pod or by a Job, fork
#   chaos      broker down, leader killed mid-rescue, storage outage, sidecar SIGKILL, crash + replacement race
#   tls        broker with an operator's certificate: sidecars and Jobs verify its CA; plain HTTP and an untrusted CA are refused
#              (every other scenario runs over the TLS certificate that the broker manages itself)
#   multinode  (needs 2+ workers) cross-node restore; the mover follows a node-local PV; drain takes a final snapshot
#   webhook    sidecar injection into bare Pods and Sandboxes; unsafe Pods refused; fail closed while down
#   gateway    REST gateway API gated by RBAC; data-plane request through the agent-sandbox router and sandboxd
#   worker     worker mode: live snapshots from a separate Pod on the same node and PVC; RWOP claims refused
#   runtimes   gVisor (runsc) workload Pods: sidecar mode inside the sandbox, worker mode beside it
#   kata       opt-in: Kata Containers workload Pods (needs /dev/kvm); see docs/verification.md for its status
#
# The harness writes files through "kubectl exec" only to play the agent;
# wsnap itself never uses exec.
# Usage: test/e2e/run.sh [setup] [scenario|group...]
#
# The groups split the scenarios into sets of about equal length that
# share no state, so CI runs each set on its own cluster in parallel.
# Without arguments, every group runs in order on one cluster.
# SKIP_IMAGE_BUILD=1 makes setup load images that are already built.
set -euo pipefail

ROOT=${ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}
M="$ROOT/test/e2e/manifests"
CLUSTER=${CLUSTER:-wsnap}
NS=e2e
AGENT_SANDBOX_DIR=${AGENT_SANDBOX_DIR:-$HOME/code/agent-sandbox}
AGENT_SANDBOX_MANIFEST=${AGENT_SANDBOX_MANIFEST:-https://github.com/kubernetes-sigs/agent-sandbox/releases/download/v1.0.10/sandbox.yaml}

log() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
pass() { printf '\033[32mPASS\033[0m %s\n' "$*"; }
die() { printf '\033[31mFAIL\033[0m %s\n' "$*"; exit 1; }

# wait_for <seconds> <description> <command...>
wait_for() {
	local timeout=$1 what=$2; shift 2
	local end=$((SECONDS + timeout))
	until "$@" >/dev/null 2>&1; do
		((SECONDS < end)) || die "timed out after ${timeout}s waiting for: $what"
		sleep 2
	done
}

ws_field() { kubectl -n "$NS" get workspace "$1" -o jsonpath="{$2}" 2>/dev/null; }

# Predicates for wait_for. They run on every poll; a "$(...)" argument to
# wait_for would be evaluated only once.
ws_is() { [[ $(ws_field "$1" "$2") == "$3" ]]; }
ws_changed() { local v; v=$(ws_field "$1" "$2"); [[ -n $v && $v != "$3" ]]; }
pv_policy_is() { [[ $(kubectl get pv "$1" -o jsonpath='{.spec.persistentVolumeReclaimPolicy}' 2>/dev/null) == "$2" ]]; }

# fresh_workspace drops the Workspace record so earlier runs cannot satisfy
# a check. The repository data in the backend stays.
repo_reason_is() { [[ $(kubectl -n "$1" get snaprepo "$2" -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}') == "$3" ]]; }
repo_condition() { [[ $(kubectl -n "$NS" get snaprepo "$1" -o jsonpath="{.status.conditions[?(@.type==\"$2\")].status}") == "$3" ]]; }

# tick writes a file so the sidecar sees a change, then checks for a
# completed maintenance. Idle workspaces take no snapshots.
tick_until_maintenance() {
	write_file "$1" /workspace/tick "$RANDOM" >/dev/null 2>&1 || true
	ws_changed "$2" .status.lastMaintenance ""
}

req_phase() { kubectl -n "$NS" get "$1" "$2" -o jsonpath='{.status.phase}' 2>/dev/null; }
req_is() { [[ $(req_phase "$1" "$2") == "$3" ]]; }
req_field() { kubectl -n "$NS" get "$1" "$2" -o jsonpath="{$3}" 2>/dev/null; }

apply_request() { # kind name workspace [snapshotID] [sourceWorkspace]
	# Requests are one-shot objects. Reapplying a name from an earlier run
	# preserves its terminal status and can make a rerun pass or fail against
	# stale results instead of exercising the agent.
	kubectl -n "$NS" delete "$1" "$2" --ignore-not-found --wait=true >/dev/null
	if [[ $1 == wssnap ]]; then
		printf 'apiVersion: wsnap.dev/v1alpha1\nkind: WorkspaceSnapshot\nmetadata: {name: %s, namespace: %s}\nspec: {workspaceName: %s}\n' "$2" "$NS" "$3" | kubectl apply -f -
	else
		printf 'apiVersion: wsnap.dev/v1alpha1\nkind: WorkspaceRestore\nmetadata: {name: %s, namespace: %s}\nspec: {workspaceName: %s, snapshotID: "%s", sourceWorkspace: "%s"}\n' "$2" "$NS" "$3" "${4:-}" "${5:-}" | kubectl apply -f -
	fi
}

leader_pod() { kubectl -n wsnap-system get lease wsnap-controller.wsnap.dev -o jsonpath='{.spec.holderIdentity}' | cut -d_ -f1; }
pv_annotation() { kubectl get pv "$1" -o jsonpath="{.metadata.annotations.wsnap\.dev/$2}" 2>/dev/null; }
pv_annotation_is() { [[ $(pv_annotation "$1" "$2") == "$3" ]]; }
pv_phase_is() { [[ $(kubectl get pv "$1" -o jsonpath='{.status.phase}' 2>/dev/null) == "$2" ]]; }
logs_contain() { kubectl -n "$NS" logs "$1" -c wsnap 2>/dev/null | grep -q "$2"; }
sidecar_restarts() { kubectl -n "$NS" get pod "$1" -o jsonpath='{.status.initContainerStatuses[?(@.name=="wsnap")].restartCount}'; }
sidecar_restarted() { (( $(sidecar_restarts "$1") > $2 )); }
# An API error must not count as "nothing there": these predicates answer
# true only when the API server confirmed the absence.
no_pods() { local out; out=$(kubectl -n "$1" get pods -l "$2" -o name 2>/dev/null) && [[ -z $out ]]; }
not_found() { local out; out=$(kubectl get "$@" 2>&1) && return 1; [[ $out == *NotFound* || $out == *"not found"* ]]; }
pv_rescue_started() { local r; r=$(pv_annotation "$1" rescue); [[ $r == running || $r == done ]] || pv_gone "$1"; }
# retry_request asks for a snapshot again until one succeeds.
retry_request() {
	result_has "$1" "$2" snapshotID && return 0
	kubectl -n "$NS" exec "$1" -c agent -- sh -c "rm -f /workspace/.wsnap/results/$2.json; touch /workspace/.wsnap/requests/$2" >/dev/null 2>&1
	sleep 8
	result_has "$1" "$2" snapshotID
}
crash_sidecar() {
	local id node
	id=$(kubectl -n "$NS" get pod "$1" -o jsonpath='{.status.initContainerStatuses[?(@.name=="wsnap")].containerID}')
	node=$(kubectl -n "$NS" get pod "$1" -o jsonpath='{.spec.nodeName}')
	docker exec "$node" crictl stop --timeout 0 "${id#containerd://}" >/dev/null
}
# crash_pod kills a Pod the way a node failure would: no graceful stop,
# no final snapshot. The kubelet is stopped meanwhile, so it can neither
# recreate the sandbox (it once did, and the restarted sidecar took a
# normal final snapshot) nor start a graceful stop that races the kill.
crash_pod() {
	local node sandbox
	node=$(kubectl -n "$NS" get pod "$1" -o jsonpath='{.spec.nodeName}')
	sandbox=$(docker exec "$node" crictl pods --name "^$1\$" --namespace "$NS" -q)
	docker exec "$node" systemctl stop kubelet
	docker exec "$node" crictl stopp "$sandbox" >/dev/null
	kubectl -n "$NS" delete pod "$1" --grace-period=0 --force >/dev/null 2>&1
	docker exec "$node" systemctl start kubelet
	wait_for 120 "$1 gone after the crash" pod_gone "$1"
}
pod_gone() { not_found -n "$NS" pod "$1"; }
result_has() { kubectl -n "$NS" exec "$1" -c agent -- grep -q "$3" "/workspace/.wsnap/results/$2.json" 2>/dev/null; }
result_id() { kubectl -n "$NS" exec "$1" -c agent -- cat "/workspace/.wsnap/results/$2.json" | sed -n 's/.*"snapshotID":"\([^"]*\)".*/\1/p'; }

fresh_workspace() { kubectl -n "$NS" delete workspace "$1" --ignore-not-found --wait=true >/dev/null; }

# pod_yaml <name> <workspace> <repository> <volume-kind: pvc:<claim>|ephemeral|emptydir> <sidecar: yes|no>
pod_spec() {
	local volume=$2 sidecar=$3 repo=$1
	cat <<EOF
    automountServiceAccountToken: false
    terminationGracePeriodSeconds: 60
EOF
	if [[ $sidecar == yes ]]; then
		cat <<EOF
    initContainers:
    - name: wsnap
      image: wsnap-agent:dev
      imagePullPolicy: IfNotPresent
      restartPolicy: Always
      args: [sidecar, --dir=/workspace, --final-timeout=50s, --operation-timeout=${SIDECAR_OP_TIMEOUT:-60s}${SIDECAR_TLS_ARGS:-}]
      startupProbe:
        exec: {command: [/usr/local/bin/wsnap-agent, ready, --state-dir=/var/lib/wsnap]}
        periodSeconds: 1
        failureThreshold: 600
      volumeMounts:
      - {name: workspace, mountPath: /workspace}
      - {name: wsnap-token, mountPath: /var/run/secrets/wsnap, readOnly: true}
      - {name: wsnap-state, mountPath: /var/lib/wsnap}
EOF
		if [[ ${WSNAP_TLS:-} == trusted ]]; then
			printf '      - {name: wsnap-ca, mountPath: /var/run/wsnap-ca, readOnly: true}\n'
		fi
		if [[ $repo == tar-fs ]]; then
			cat <<EOF
      - {name: backend, mountPath: /backend}
EOF
		fi
	fi
	cat <<EOF
    containers:
    - name: agent
      image: alpine:3.22
      imagePullPolicy: IfNotPresent
      # Exit on SIGTERM, as a real workload must: a bare "sleep" as PID 1
      # ignores it, eats the grace period, and leaves the sidecar 2 s for
      # its final snapshot.
      command: [sh, -c, 'trap "exit 0" TERM; sleep infinity & wait']
      volumeMounts:
      - {name: workspace, mountPath: /workspace}
    volumes:
EOF
	case $volume in
	pvc:*) printf '    - name: workspace\n      persistentVolumeClaim: {claimName: %s}\n' "${volume#pvc:}" ;;
	ephemeral)
		cat <<EOF
    - name: workspace
      ephemeral:
        volumeClaimTemplate:
          spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 1Gi}}}
EOF
		;;
	emptydir) printf '    - name: workspace\n      emptyDir: {}\n' ;;
	sandbox) ;; # agent-sandbox adds the volume from volumeClaimTemplates
	esac
	if [[ $sidecar == yes ]]; then
		cat <<EOF
    - name: wsnap-token
      projected:
        sources:
        - serviceAccountToken: {audience: wsnap, expirationSeconds: 600, path: token}
    - name: wsnap-state
      emptyDir: {}
EOF
		if [[ ${WSNAP_TLS:-} == trusted ]]; then
			printf '    - name: wsnap-ca\n      configMap: {name: wsnap-ca}\n'
		fi
		if [[ $repo == tar-fs ]]; then
			printf '    - name: backend\n      hostPath: {path: /var/wsnap-backend, type: DirectoryOrCreate}\n'
		fi
	fi
}

# NODE_SELECTOR pins the next apply_pod to a node.
apply_pod() { # name workspace repository volume sidecar
	{
		cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $1
  namespace: $NS
  labels: {wsnap.dev/workspace: $2, wsnap.dev/repository: $3}
spec:
EOF
		pod_spec "$3" "$4" "$5" | sed 's/^  //'
		if [[ -n ${NODE_SELECTOR:-} ]]; then
			printf '  nodeSelector: {kubernetes.io/hostname: %s}\n' "$NODE_SELECTOR"
		fi
		if [[ -n ${RUNTIME_CLASS:-} ]]; then
			printf '  runtimeClassName: %s\n' "$RUNTIME_CLASS"
		fi
	} | kubectl apply -f -
}

apply_pvc() {
	kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: $1, namespace: $NS}
spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 1Gi}}}
EOF
}

apply_sandbox() { # name workspace repository operatingMode
	{
		cat <<EOF
apiVersion: agents.x-k8s.io/v1beta1
kind: Sandbox
metadata: {name: $1, namespace: $NS}
spec:
  operatingMode: $4
  volumeClaimTemplates:
  - metadata: {name: workspace}
    spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 1Gi}}}
  podTemplate:
    metadata:
      labels: {wsnap.dev/workspace: $2, wsnap.dev/repository: $3}
    spec:
EOF
		pod_spec "$3" sandbox yes | sed 's/^/  /'
	} | kubectl apply -f -
}

pv_of() { kubectl -n "$NS" get pvc "$1" -o jsonpath='{.spec.volumeName}'; }
pv_gone() { not_found pv "$1"; }
pvc_gone() { not_found -n "$NS" pvc "$1"; }
pod_ready() { [[ $(kubectl -n "$NS" get pod "$1" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}') == True ]]; }
read_file() { kubectl -n "$NS" exec "$1" -c agent -- cat "$2"; }
write_file() { kubectl -n "$NS" exec "$1" -c agent -- sh -c "echo '$3' > $2"; }

setup() {
	log "build and load images"
	if [[ -z ${SKIP_IMAGE_BUILD:-} ]]; then
		docker build -q --target controller -t wsnap-controller:dev "$ROOT" >/dev/null
		docker build -q --target agent -t wsnap-agent:dev "$ROOT" >/dev/null
	fi
	# Only locally built images need loading; the node pulls public ones.
	# (kind cannot import multi-arch images that are only partly present.)
	for img in wsnap-controller:dev wsnap-agent:dev; do
		kind load docker-image --name "$CLUSTER" "$img" >/dev/null
	done
	log "install agent-sandbox, MinIO and wsnap"
	kubectl apply --server-side -f "$AGENT_SANDBOX_MANIFEST" >/dev/null
	# The setup Job is not idempotent (policy attach fails when attached).
	kubectl -n storage delete job minio-setup --ignore-not-found --wait=true >/dev/null 2>&1 || true
	kubectl apply -f "$M/minio.yaml" >/dev/null
	# The TLS scenario patches the Deployment outside kubectl apply's field
	# ownership. Recreate it so a later setup really returns to the defaults.
	kubectl -n wsnap-system delete deploy wsnap-controller --ignore-not-found --wait=true >/dev/null
	# Least privilege: the controller reads Secrets only where a namespace
	# binds wsnap-secret-reader.
	kubectl apply -k "$ROOT/config/namespaced-secrets" >/dev/null
	kubectl delete clusterrolebinding wsnap-secret-reader --ignore-not-found >/dev/null
	kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
	kubectl -n "$NS" apply -f "$ROOT/config/samples/secret-access.yaml" >/dev/null
	kubectl -n wsnap-system rollout restart deploy/wsnap-controller >/dev/null
	kubectl -n storage wait --for=condition=complete job/minio-setup --timeout=180s >/dev/null
	kubectl -n wsnap-system rollout status deploy/wsnap-controller --timeout=180s >/dev/null
	kubectl -n agent-sandbox-system rollout status deploy/agent-sandbox-controller --timeout=180s >/dev/null
	kubectl apply -f "$M/repositories.yaml" >/dev/null
	for r in kopia-s3 kopia-keep restic-s3 tar-fs; do
		wait_for 60 "repository $r Ready" repo_condition "$r" Ready True
	done
	repo_condition tar-fs CredentialsScoped False || die "static provider not flagged as unscoped"
	repo_condition kopia-s3 CredentialsScoped True || die "s3-sts provider not flagged as scoped"
	kubectl -n wsnap-system get secret wsnap-broker-cert >/dev/null || die "the broker did not create its TLS certificate"
	# A namespace without the RoleBinding: wsnap may not read its Secret.
	kubectl create namespace e2e-nobind --dry-run=client -o yaml | kubectl apply -f - >/dev/null
	kubectl -n e2e-nobind create secret generic creds --from-literal=WSNAP_MASTER_KEY=0123456789abcdef --dry-run=client -o yaml | kubectl apply -f - >/dev/null
	printf 'apiVersion: wsnap.dev/v1alpha1\nkind: SnapshotRepository\nmetadata: {name: r, namespace: e2e-nobind}\nspec: {engine: {type: restic}, backend: {filesystem: {path: /x}}, credentials: {secretName: creds}}\n' | kubectl apply -f - >/dev/null
	wait_for 60 "SecretForbidden without a RoleBinding" repo_reason_is e2e-nobind r SecretForbidden
	kubectl delete namespace e2e-nobind --wait=false >/dev/null
	pass "setup (repositories Ready, CredentialsScoped reported; broker on TLS; Secrets readable only where bound)"
}

scenario_pvc() {
	log "pvc: Pod + standalone PVC, kopia + S3 with STS-scoped tokens"
	fresh_workspace ws-pvc
	apply_pvc pvc-a >/dev/null
	apply_pod pvc-1 ws-pvc kopia-s3 pvc:pvc-a yes >/dev/null
	wait_for 180 "pvc-1 ready" pod_ready pvc-1
	write_file pvc-1 /workspace/hello.txt v1
	local pv; pv=$(pv_of pvc-a)
	local claim_uid pv_uid
	claim_uid=$(kubectl -n "$NS" get pvc pvc-a -o jsonpath='{.metadata.uid}')
	pv_uid=$(kubectl get pv "$pv" -o jsonpath='{.metadata.uid}')
	[[ $(ws_field ws-pvc .status.volume.claimUID) == "$claim_uid" && $(ws_field ws-pvc .status.volume.volumeUID) == "$pv_uid" ]] ||
		die "Workspace did not record the bound PVC and PV UIDs"
	[[ $(kubectl get pv "$pv" -o jsonpath='{.spec.persistentVolumeReclaimPolicy}') == Retain ]] || die "PV $pv not protected"
	pass "PV $pv switched to Retain; Workspace recorded its PVC and PV UIDs"

	kubectl -n "$NS" delete pod pvc-1 --wait=true >/dev/null
	[[ $(ws_field ws-pvc .status.lastSnapshot.final) == true ]] || die "no final snapshot after graceful delete"
	[[ $(ws_field ws-pvc .status.lastSnapshot.executor) == sidecar ]] || die "final snapshot not by sidecar"
	pass "graceful delete took a final snapshot ($(ws_field ws-pvc .status.lastSnapshot.id))"

	apply_pod pvc-2 ws-pvc kopia-s3 pvc:pvc-a yes >/dev/null
	wait_for 120 "pvc-2 ready" pod_ready pvc-2
	[[ $(read_file pvc-2 /workspace/hello.txt) == v1 ]] || die "warm resume lost data"
	kubectl -n "$NS" logs pvc-2 -c wsnap | grep -q "already initialized" || die "warm resume ran a restore"
	pass "warm resume on the same PVC: data present, restore skipped"

	log "pvc: maintenance after 3 snapshots of changed data"
	wait_for 300 "maintenance" tick_until_maintenance pvc-2 ws-pvc
	pass "maintenance job ran with the maintenance role ($(ws_field ws-pvc .status.lastMaintenance))"

	write_file pvc-2 /workspace/hello.txt v2
	kubectl -n "$NS" delete pod pvc-2 --wait=true >/dev/null
	kubectl -n "$NS" delete pvc pvc-a --wait=true >/dev/null
	wait_for 120 "PV $pv reclaimed" pv_gone "$pv"
	pass "deleting the PVC after a final snapshot reclaimed PV $pv"

	apply_pvc pvc-b >/dev/null
	apply_pod pvc-3 ws-pvc kopia-s3 pvc:pvc-b yes >/dev/null
	wait_for 180 "pvc-3 ready" pod_ready pvc-3
	[[ $(read_file pvc-3 /workspace/hello.txt) == v2 ]] || die "cold restore returned $(read_file pvc-3 /workspace/hello.txt)"
	[[ -n $(ws_field ws-pvc .status.lastRestore.snapshotID) ]] || die "restore not recorded"
	pass "cold restore into a new PVC returned the latest data"
	kubectl -n "$NS" delete pod pvc-3 --wait=true >/dev/null
	kubectl -n "$NS" delete pvc pvc-b >/dev/null
}

scenario_ephemeral() {
	log "ephemeral: generic ephemeral volume, injected sidecar, restic + S3 STS: crash, then rescue by mover"
	fresh_workspace ws-eph
	apply_pod eph-1 ws-eph restic-s3 ephemeral no >/dev/null
	wait_for 180 "eph-1 ready" pod_ready eph-1
	write_file eph-1 /workspace/notes.txt rescued
	local pv; pv=$(pv_of eph-1-workspace)
	wait_for 60 "PV protected" pv_policy_is "$pv" Retain
	[[ $(kubectl -n "$NS" get pod eph-1 -o jsonpath='{.spec.initContainers[0].name}') == wsnap ]] || die "webhook did not inject the sidecar"
	pass "webhook injected the sidecar into a Pod that only set labels; PV $pv protected"

	crash_pod eph-1
	wait_for 240 "rescue snapshot" ws_is ws-eph .status.lastSnapshot.executor mover
	[[ $(ws_field ws-eph .status.lastSnapshot.volumeName) == "$pv" ]] || die "mover snapshotted the wrong volume"
	pass "the Pod crashed (sandbox killed on the node); the PVC died with it; a mover snapshotted the released PV"
	wait_for 120 "PV $pv reclaimed after rescue" pv_gone "$pv"
	pass "PV $pv reclaimed after the rescue"

	apply_pod eph-2 ws-eph restic-s3 ephemeral yes >/dev/null
	wait_for 180 "eph-2 ready" pod_ready eph-2
	[[ $(read_file eph-2 /workspace/notes.txt) == rescued ]] || die "rescued data not restored"
	pass "a new Pod restored the rescued data"
	kubectl -n "$NS" delete pod eph-2 --wait=true >/dev/null
}

scenario_sandbox() {
	log "sandbox: agent-sandbox Sandbox, restic + S3 STS"
	fresh_workspace ws-sb
	apply_sandbox sb-1 ws-sb restic-s3 Running >/dev/null
	wait_for 180 "sandbox pod" kubectl -n "$NS" get pod sb-1
	wait_for 180 "sb-1 ready" pod_ready sb-1
	write_file sb-1 /workspace/state.txt sandbox-v1
	local pv; pv=$(pv_of workspace-sb-1)

	kubectl -n "$NS" patch sandbox sb-1 --type merge -p '{"spec":{"operatingMode":"Suspended"}}' >/dev/null
	wait_for 120 "sandbox pod deleted" pod_gone sb-1
	[[ $(ws_field ws-sb .status.lastSnapshot.final) == true ]] || die "suspend did not take a final snapshot"
	kubectl -n "$NS" get pvc workspace-sb-1 >/dev/null || die "suspend deleted the PVC"
	pass "Suspended: pod gone, final snapshot taken, PVC kept"

	kubectl -n "$NS" patch sandbox sb-1 --type merge -p '{"spec":{"operatingMode":"Running"}}' >/dev/null
	wait_for 180 "resumed pod" kubectl -n "$NS" get pod sb-1
	wait_for 180 "sb-1 ready again" pod_ready sb-1
	[[ $(read_file sb-1 /workspace/state.txt) == sandbox-v1 ]] || die "resume lost data"
	pass "Running again: warm resume on the retained PVC"

	write_file sb-1 /workspace/state.txt sandbox-v2
	kubectl -n "$NS" delete sandbox sb-1 --wait=true >/dev/null
	wait_for 120 "PV $pv reclaimed" pv_gone "$pv"
	[[ $(ws_field ws-sb .status.lastSnapshot.executor) == sidecar ]] || die "unexpected rescue after sandbox delete"
	pass "deleting the Sandbox deleted its PVC; the final snapshot made the PV safe to reclaim"

	apply_sandbox sb-2 ws-sb restic-s3 Running >/dev/null
	wait_for 180 "sb-2 pod" kubectl -n "$NS" get pod sb-2
	wait_for 180 "sb-2 ready" pod_ready sb-2
	[[ $(read_file sb-2 /workspace/state.txt) == sandbox-v2 ]] || die "new Sandbox did not restore the workspace"
	pass "a new Sandbox with the same workspace label restored it"
	kubectl -n "$NS" delete sandbox sb-2 --wait=true >/dev/null
}

scenario_emptydir() {
	log "emptydir: emptyDir workspace, exec engine (tar) + filesystem backend, static provider"
	fresh_workspace ws-tmp
	# The test's filesystem backend is a node-local hostPath, so both Pods
	# must run on one node; a real filesystem backend is shared (NFS).
	local node; node=$(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' -o jsonpath='{.items[0].metadata.name}')
	[[ -n $node ]] || node=$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')
	local NODE_SELECTOR=$node
	apply_pod tmp-1 ws-tmp tar-fs emptydir yes >/dev/null
	wait_for 120 "tmp-1 ready" pod_ready tmp-1
	write_file tmp-1 /workspace/scratch.txt ephemeral-data
	kubectl -n "$NS" delete pod tmp-1 --wait=true >/dev/null
	[[ $(ws_field ws-tmp .status.lastSnapshot.final) == true ]] || die "no final snapshot for emptyDir"
	apply_pod tmp-2 ws-tmp tar-fs emptydir yes >/dev/null
	wait_for 120 "tmp-2 ready" pod_ready tmp-2
	[[ $(read_file tmp-2 /workspace/scratch.txt) == ephemeral-data ]] || die "emptyDir data not restored"
	pass "emptyDir workspace survived its Pod through the exec engine"
	local restored; restored=$(ws_field ws-tmp .status.lastRestore.snapshotID)

	log "isolation: the workload container cannot get credentials"
	local code
	code=$(kubectl -n "$NS" exec tmp-2 -c agent -- sh -c \
		'wget -q -S -O- --no-check-certificate --post-data={\"role\":\"snapshot\"} https://wsnap-broker.wsnap-system.svc:8443/v1/credentials 2>&1 | grep -o "HTTP/1.1 [0-9]*" | head -1' || true)
	[[ $code == "HTTP/1.1 401" ]] || die "broker answered the workload container with: $code"
	kubectl -n "$NS" exec tmp-2 -c agent -- test ! -e /var/run/secrets/wsnap/token || die "workload container sees the token"
	pass "workload container: no token mounted, broker returns 401"
	kubectl -n "$NS" delete pod tmp-2 --wait=true >/dev/null
	[[ $(ws_field ws-tmp .status.lastSnapshot.id) == "$restored" && $(ws_field ws-tmp .status.lastSnapshot.final) == true ]] ||
		die "unchanged workspace took a new final snapshot instead of confirming $restored"
	pass "unchanged workspace: final snapshot confirmed the restored one ($restored), no new upload"
}

scenario_singlewriter() {
	log "singlewriter: two Pods with one workspace label"
	fresh_workspace ws-one
	apply_pvc one-a >/dev/null
	apply_pvc one-b >/dev/null
	apply_pod one-1 ws-one kopia-s3 pvc:one-a yes >/dev/null
	wait_for 180 "one-1 ready" pod_ready one-1
	write_file one-1 /workspace/owner.txt first-writer
	apply_pod one-2 ws-one kopia-s3 pvc:one-b yes >/dev/null
	wait_for 120 "second Pod refused as writer" logs_contain one-2 "is written by pod one-1"
	pod_ready one-2 && die "a second writer became ready while the first still runs"
	local stopped
	pass "second Pod waits: the broker allows one live writer per workspace"
	kubectl -n "$NS" delete pod one-1 --wait=true >/dev/null
	stopped=$SECONDS
	wait_for 180 "one-2 ready" pod_ready one-2
	((SECONDS - stopped <= 20)) || die "second Pod took $((SECONDS - stopped))s to start after the first stopped"
	[[ $(read_file one-2 /workspace/owner.txt) == first-writer ]] || die "second Pod did not restore the first Pod's final snapshot"
	pass "after the first Pod stopped, the second restored its final snapshot within $((SECONDS - stopped))s"
	local epoch pv_epoch
	epoch=$(kubectl -n "$NS" get workspace ws-one -o jsonpath='{.status.writerEpoch}')
	pv_epoch=$(kubectl get pv "$(kubectl -n "$NS" get pvc one-b -o jsonpath='{.spec.volumeName}')" -o jsonpath='{.metadata.annotations.wsnap\.dev/writer-epoch}')
	[[ $epoch == 2 && $pv_epoch == 2 ]] || die "writer epoch $epoch, one-b PV epoch $pv_epoch; want 2 and 2"
	pass "writer epochs: the second writer runs in epoch 2, stamped on its PV"
	kubectl -n "$NS" delete pod one-2 --wait=true >/dev/null
	kubectl -n "$NS" delete pvc one-a one-b >/dev/null
}

scenario_requests() {
	log "requests: control plane (CRs) and data plane (files); work done by the Pod or a Job"
	# A failed earlier run may have left the claim and its initialized data.
	# Start from an empty volume so this scenario tests a fresh restore path.
	kubectl -n "$NS" delete pod req-1 req-2 fork-1 fork-2 --ignore-not-found --wait=true >/dev/null
	kubectl -n "$NS" delete pvc req-a --ignore-not-found --wait=true >/dev/null
	fresh_workspace ws-req
	apply_pvc req-a >/dev/null
	apply_pod req-1 ws-req kopia-keep pvc:req-a yes >/dev/null
	wait_for 180 "req-1 ready" pod_ready req-1
	# The repository intentionally survives test runs, so a new PVC may
	# restore files from the previous run. Establish the baseline explicitly.
	kubectl -n "$NS" exec req-1 -c agent -- rm -f /workspace/added-later.txt
	write_file req-1 /workspace/state.txt v1

	apply_request wssnap snap-live ws-req >/dev/null
	wait_for 60 "live snapshot request" req_is wssnap snap-live Succeeded
	[[ $(req_field wssnap snap-live .status.executor) == sidecar ]] || die "live request not served by the sidecar"
	local v1; v1=$(req_field wssnap snap-live .status.snapshotID)
	pass "WorkspaceSnapshot on a live Pod: the sidecar took $v1 (woken by the broker long poll, no exec)"

	write_file req-1 /workspace/state.txt v2
	kubectl -n "$NS" exec req-1 -c agent -- touch /workspace/.wsnap/requests/checkpoint-1
	wait_for 30 "file trigger result" result_has req-1 checkpoint-1 snapshotID
	local v2; v2=$(result_id req-1 checkpoint-1)
	[[ -n $v2 && $v2 != "$v1" ]] || die "file trigger did not produce a new snapshot ($v2)"
	pass "data-plane request file (as the agent or sandboxd would write it): snapshot $v2"

	write_file req-1 /workspace/state.txt v3
	write_file req-1 /workspace/added-later.txt junk
	apply_request wsrestore rollback ws-req "$v1" >/dev/null
	wait_for 90 "in-place rollback" req_is wsrestore rollback Succeeded
	[[ $(read_file req-1 /workspace/state.txt) == v1 ]] || die "rollback did not restore v1"
	kubectl -n "$NS" exec req-1 -c agent -- test ! -e /workspace/added-later.txt || die "rollback kept a file created after the snapshot"
	wait_for 30 "post-rollback snapshot" ws_changed ws-req .status.lastSnapshot.id "$v2"
	# The live v3 is saved before the swap: "snapshot taken" comes before
	# "restored in place" in the sidecar log.
	kubectl -n "$NS" logs req-1 -c wsnap | grep -E '"msg":"(snapshot taken|restored in place)"' | tail -3 |
		grep -o '"msg":"[a-z ]*"' | tr '\n' ' ' | grep -q '"msg":"snapshot taken" "msg":"restored in place" "msg":"snapshot taken"' ||
		die "the rollback did not snapshot the live data first"
	pass "WorkspaceRestore on a live Pod: saved the live data, rolled back to $v1 in place, then re-snapshotted"

	kubectl -n "$NS" delete pod req-1 --wait=true >/dev/null
	apply_request wssnap snap-idle ws-req >/dev/null
	wait_for 120 "idle snapshot request" req_is wssnap snap-idle Succeeded
	[[ $(req_field wssnap snap-idle .status.executor) == mover ]] || die "idle request not served by a mover"
	pass "WorkspaceSnapshot with no Pod: a mover Job snapshotted the idle PVC"

	apply_request wsrestore restore-idle ws-req "$v2" >/dev/null
	wait_for 120 "idle restore request" req_is wsrestore restore-idle Succeeded
	[[ $(req_field wsrestore restore-idle .status.executor) == restorer ]] || die "idle restore not served by a restorer"
	apply_pod req-2 ws-req kopia-keep pvc:req-a yes >/dev/null
	wait_for 120 "req-2 ready" pod_ready req-2
	[[ $(read_file req-2 /workspace/state.txt) == v2 ]] || die "restorer did not put $v2 on the idle PVC"
	logs_contain req-2 "already initialized" || die "next Pod restored over the restorer's work"
	pass "WorkspaceRestore with no Pod: a restorer Job restored the idle PVC; the next Pod kept it"
	kubectl -n "$NS" delete pod req-2 --wait=true >/dev/null

	log "requests: fork with restoreFrom"
	fresh_workspace ws-fork
	printf 'apiVersion: wsnap.dev/v1alpha1\nkind: Workspace\nmetadata: {name: ws-fork, namespace: %s}\nspec: {repositoryName: restic-s3, restoreFrom: {workspaceName: ws-req, snapshotID: "%s"}}\n' "$NS" "$v1" | kubectl apply -f - >/dev/null
	apply_pod fork-1 ws-fork restic-s3 ephemeral yes >/dev/null
	wait_for 180 "fork-1 ready" pod_ready fork-1
	[[ $(read_file fork-1 /workspace/state.txt) == v1 ]] || die "fork did not get the parent's snapshot"
	[[ $(ws_field ws-fork .status.lastRestore.sourceWorkspace) == ws-req ]] || die "fork restore not recorded"
	write_file fork-1 /workspace/state.txt fork-only
	kubectl -n "$NS" delete pod fork-1 --wait=true >/dev/null
	[[ $(ws_field ws-fork .status.lastSnapshot.final) == true ]] || die "fork took no final snapshot"
	apply_pod fork-2 ws-fork restic-s3 ephemeral yes >/dev/null
	wait_for 240 "fork-2 ready" pod_ready fork-2
	[[ $(read_file fork-2 /workspace/state.txt) == fork-only ]] || die "restoreFrom applied twice, or the fork's own history is missing"
	pass "fork: Kopia parent to restic child; the child keeps its own history; restoreFrom applied once"
	kubectl -n "$NS" delete pod fork-2 --wait=true >/dev/null

	apply_request wssnap snap-nothing ws-tmp >/dev/null
	wait_for 60 "request with nothing to act on" req_is wssnap snap-nothing Failed
	pass "request for an emptyDir workspace with no Pod failed: $(req_field wssnap snap-nothing .status.message)"
	wait_for 60 "finished request Job Pods removed" no_pods "$NS" 'wsnap.dev/job-workspace=ws-req,wsnap.dev/job in (mover,restorer)'
	kubectl -n "$NS" delete pvc req-a --wait=false >/dev/null
	wait_for 60 "PVC deleted after request Jobs" pvc_gone req-a
	pass "finished Jobs released the PVC promptly for deletion or reprovisioning"
}

scenario_chaos() {
	log "chaos 1+2: controller down while a Pod with unsnapshotted data dies; leader killed mid-rescue"
	fresh_workspace ws-chaos
	apply_pod chaos-1 ws-chaos restic-s3 ephemeral no >/dev/null
	wait_for 180 "chaos-1 ready" pod_ready chaos-1
	write_file chaos-1 /workspace/precious.txt survived-outage
	local pv; pv=$(pv_of chaos-1-workspace)
	wait_for 60 "PV protected" pv_policy_is "$pv" Retain
	kubectl -n wsnap-system scale deploy/wsnap-controller --replicas=0 >/dev/null
	wait_for 90 "controller down" no_pods wsnap-system app.kubernetes.io/name=wsnap-controller
	crash_pod chaos-1
	wait_for 60 "PV released" pv_phase_is "$pv" Released
	sleep 5
	kubectl get pv "$pv" >/dev/null || die "PV deleted while the controller was down"
	pass "controller down: the PVC died with its Pod; PV $pv kept its data (Retain)"
	kubectl -n wsnap-system scale deploy/wsnap-controller --replicas=2 >/dev/null
	wait_for 120 "rescue started" pv_rescue_started "$pv"
	if pv_annotation_is "$pv" rescue running; then
		local leader; leader=$(leader_pod)
		kubectl -n wsnap-system delete pod "$leader" --wait=false >/dev/null
		pass "killed leader $leader while the rescue runs"
	else
		echo "   note: the rescue finished before the leader could be killed"
	fi
	wait_for 240 "rescue finished by the new leader" ws_is ws-chaos .status.lastSnapshot.executor mover
	wait_for 120 "PV reclaimed" pv_gone "$pv"
	SIDECAR_OP_TIMEOUT=20s apply_pod chaos-2 ws-chaos restic-s3 ephemeral yes >/dev/null
	wait_for 180 "chaos-2 ready" pod_ready chaos-2
	[[ $(read_file chaos-2 /workspace/precious.txt) == survived-outage ]] || die "data lost across the outage"
	pass "after the outage and a leader failover, the rescued data was restored"

	log "chaos 3: storage outage"
	kubectl -n storage scale deploy/minio --replicas=0 >/dev/null
	wait_for 90 "minio down" no_pods storage app=minio
	write_file chaos-2 /workspace/precious.txt during-outage
	kubectl -n "$NS" exec chaos-2 -c agent -- touch /workspace/.wsnap/requests/during-outage
	wait_for 180 "failed result" result_has chaos-2 during-outage error
	wait_for 30 "lastError" ws_changed ws-chaos .status.lastError ""
	pass "storage down: the snapshot failed visibly (result file and Workspace lastError)"
	kubectl -n storage scale deploy/minio --replicas=1 >/dev/null
	kubectl -n storage rollout status deploy/minio --timeout=180s >/dev/null
	# A client retries a failed request; the first try may still see stale
	# connections or a credential the restarted store has to re-validate.
	wait_for 240 "recovered" retry_request chaos-2 after-outage
	pass "storage back: the next snapshot succeeded"

	log "chaos 4: sidecar SIGKILL"
	local before; before=$(sidecar_restarts chaos-2)
	# SIGKILL from the container runtime, as an OOM kill would arrive. A
	# "kill -9 1" from inside the container is ignored: Linux protects a PID
	# namespace's init from signals sent within the namespace.
	crash_sidecar chaos-2
	wait_for 120 "sidecar restarted" sidecar_restarted chaos-2 "$before"
	wait_for 60 "pod ready again" pod_ready chaos-2
	[[ $(read_file chaos-2 /workspace/precious.txt) == during-outage ]] || die "workload data changed by the sidecar restart"
	logs_contain chaos-2 "already initialized" || die "restarted sidecar restored over live data"
	pass "sidecar killed: the kubelet restarted it; the workload kept running; no re-restore"
	kubectl -n "$NS" delete pod chaos-2 --wait=true >/dev/null

	log "chaos 5: crash and an immediate replacement (restore vs rescue race)"
	fresh_workspace ws-race
	apply_pod race-1 ws-race restic-s3 ephemeral no >/dev/null
	wait_for 180 "race-1 ready" pod_ready race-1
	write_file race-1 /workspace/last-words.txt written-before-crash
	pv=$(pv_of race-1-workspace)
	wait_for 60 "PV protected" pv_policy_is "$pv" Retain
	crash_pod race-1 &
	apply_pod race-2 ws-race restic-s3 ephemeral yes >/dev/null
	wait
	wait_for 300 "race-2 ready" pod_ready race-2
	[[ $(read_file race-2 /workspace/last-words.txt) == written-before-crash ]] || die "the replacement started from an older snapshot"
	if logs_contain race-2 "being rescued"; then
		pass "the replacement waited for the rescue of the crashed Pod's volume, then restored its data"
	else
		pass "the rescue finished before the replacement asked; it restored the crashed Pod's data"
	fi
	kubectl -n "$NS" delete pod race-2 --wait=true >/dev/null
}

broker_tls() { # on|off
	local args
	if [[ $1 == on ]]; then
		args='["--broker-addr=:8443","--broker-url=https://wsnap-broker.wsnap-system.svc:8443","--agent-image=wsnap-agent:dev","--leader-elect","--tls-cert-file=/tls/tls.crt","--tls-key-file=/tls/tls.key","--agent-ca-configmap=wsnap-ca"]'
		kubectl -n wsnap-system patch deploy wsnap-controller --type strategic -p '{"spec":{"template":{"spec":{
			"volumes":[{"name":"tls","secret":{"secretName":"wsnap-broker-tls"}}],
			"containers":[{"name":"controller","args":'"$args"',
				"volumeMounts":[{"name":"tls","mountPath":"/tls","readOnly":true}]}]}}}}' >/dev/null
	else
		args='["--broker-addr=:8443","--broker-url=https://wsnap-broker.wsnap-system.svc:8443","--agent-image=wsnap-agent:dev","--leader-elect"]'
		kubectl -n wsnap-system patch deploy wsnap-controller --type strategic -p '{"spec":{"template":{"spec":{
			"volumes":[{"name":"tls","$patch":"delete"}],
			"containers":[{"name":"controller","args":'"$args"',
				"volumeMounts":[{"mountPath":"/tls","$patch":"delete"}]}]}}}}' >/dev/null
	fi
	kubectl -n wsnap-system rollout status deploy/wsnap-controller --timeout=180s >/dev/null
	# An old leader can still hold the lease after the rollout and create
	# Jobs with the old broker settings. Wait for a leader with the new flags.
	wait_for 120 "leader with operator certificate $1" leader_operator_cert "$1"
}

leader_operator_cert() { # on|off
	local leader args; leader=$(leader_pod)
	[[ -n $leader ]] || return 1
	args=$(kubectl -n wsnap-system get pod "$leader" -o jsonpath='{.metadata.deletionTimestamp} {.spec.containers[0].args}') || return 1
	[[ $args == " "* ]] || return 1 # terminating
	if [[ $1 == on ]]; then [[ $args == *--tls-cert-file* ]]; else [[ $args != *--tls-cert-file* ]]; fi
}

scenario_tls() {
	log "tls: broker over HTTPS with a private CA"
	local dir; dir=$(mktemp -d)
	openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=wsnap-test-ca" -keyout "$dir/ca.key" -out "$dir/ca.crt" 2>/dev/null
	openssl req -newkey rsa:2048 -nodes -subj "/CN=wsnap-broker" -keyout "$dir/tls.key" -out "$dir/tls.csr" 2>/dev/null
	printf 'subjectAltName=DNS:wsnap-broker.wsnap-system.svc,DNS:wsnap-broker.wsnap-system.svc.cluster.local\n' > "$dir/ext"
	openssl x509 -req -in "$dir/tls.csr" -CA "$dir/ca.crt" -CAkey "$dir/ca.key" -CAcreateserial -days 1 -extfile "$dir/ext" -out "$dir/tls.crt" 2>/dev/null
	kubectl -n wsnap-system create secret tls wsnap-broker-tls --cert "$dir/tls.crt" --key "$dir/tls.key" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
	kubectl -n "$NS" create configmap wsnap-ca --from-file=ca.crt="$dir/ca.crt" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
	broker_tls on
	pass "broker restarted with TLS (certificate signed by a private CA)"

	kubectl -n "$NS" delete pod tls-1 tls-bad tls-plain --ignore-not-found --wait=true >/dev/null
	kubectl -n "$NS" delete pvc tls-a --ignore-not-found --wait=true >/dev/null
	fresh_workspace ws-tls
	apply_pvc tls-a >/dev/null
	WSNAP_TLS=trusted SIDECAR_TLS_ARGS=", --broker-url=https://wsnap-broker.wsnap-system.svc:8443, --ca-file=/var/run/wsnap-ca/ca.crt" \
		wait_for 60 "webhook admission after the TLS rollout" apply_pod tls-1 ws-tls kopia-keep pvc:tls-a yes
	wait_for 180 "tls-1 ready" pod_ready tls-1
	write_file tls-1 /workspace/secret.txt over-tls
	kubectl -n "$NS" delete pod tls-1 --wait=true >/dev/null
	[[ $(ws_field ws-tls .status.lastSnapshot.final) == true ]] || die "no final snapshot over TLS"
	pass "sidecar verified the broker CA: restore, snapshot and final snapshot over HTTPS"

	apply_request wssnap tls-idle ws-tls >/dev/null
	wait_for 120 "mover over TLS" req_is wssnap tls-idle Succeeded
	pass "controller-created mover Job trusted the CA through --agent-ca-configmap"

	fresh_workspace ws-tls-bad
	WSNAP_TLS=untrusted SIDECAR_TLS_ARGS=", --broker-url=https://wsnap-broker.wsnap-system.svc:8443" \
		apply_pod tls-bad ws-tls-bad kopia-keep emptydir yes >/dev/null
	wait_for 60 "untrusted refusal" logs_contain tls-bad "certificate signed by unknown authority"
	pod_ready tls-bad && die "a sidecar that does not trust the CA became ready"
	pass "a sidecar without the CA refuses the broker (x509: unknown authority)"
	kubectl -n "$NS" delete pod tls-bad --wait=true >/dev/null

	SIDECAR_TLS_ARGS=", --broker-url=http://wsnap-broker.wsnap-system.svc:8443" apply_pod tls-plain ws-tls-bad kopia-keep emptydir yes >/dev/null
	wait_for 60 "plain HTTP refusal" logs_contain tls-plain "initialization failed"
	pod_ready tls-plain && die "a plain-HTTP sidecar became ready against a TLS broker"
	pass "plain HTTP to the TLS broker is refused"
	kubectl -n "$NS" delete pod tls-plain --wait=true >/dev/null
	kubectl -n "$NS" delete pvc tls-a >/dev/null

	broker_tls off
	pass "broker back on its own certificate for the other scenarios"
	rm -rf "$dir"
}

job_node() { kubectl -n "$NS" get pods -l "wsnap.dev/job=mover,wsnap.dev/job-workspace=$1" -o jsonpath='{.items[0].spec.nodeName}' 2>/dev/null; }
job_node_known() { [[ -n $(job_node "$1") ]]; }

scenario_multinode() {
	local nodes; nodes=($(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' -o jsonpath='{.items[*].metadata.name}'))
	if ((${#nodes[@]} < 2)); then
		echo "   skipping multinode: needs 2 worker nodes (create the cluster with test/e2e/kind-multinode.yaml)"
		return
	fi
	local a=${nodes[0]} b=${nodes[1]}
	log "multinode: workers $a and $b"
	# A previous failed drain may leave its bare Pod and cordon behind.
	kubectl uncordon "$a" >/dev/null
	kubectl -n "$NS" delete pod drain-1 drain-2 --ignore-not-found --wait=true >/dev/null

	fresh_workspace ws-mn
	apply_pvc mn-a >/dev/null
	NODE_SELECTOR=$a wait_for 60 "webhook admission after TLS rollback" apply_pod mn-1 ws-mn restic-s3 pvc:mn-a yes
	wait_for 180 "mn-1 ready on $a" pod_ready mn-1
	write_file mn-1 /workspace/where.txt "written-on-$a"
	kubectl -n "$NS" delete pod mn-1 --wait=true >/dev/null
	apply_pvc mn-b >/dev/null
	NODE_SELECTOR=$b apply_pod mn-2 ws-mn restic-s3 pvc:mn-b yes >/dev/null
	wait_for 180 "mn-2 ready on $b" pod_ready mn-2
	[[ $(read_file mn-2 /workspace/where.txt) == "written-on-$a" ]] || die "cross-node restore failed"
	pass "workspace written on $a restored on $b through object storage (node-local PVs on both)"
	kubectl -n "$NS" delete pod mn-2 --wait=true >/dev/null
	kubectl -n "$NS" delete pvc mn-a mn-b >/dev/null

	fresh_workspace ws-mn-rescue
	NODE_SELECTOR=$a apply_pod mnr-1 ws-mn-rescue restic-s3 ephemeral no >/dev/null
	wait_for 180 "mnr-1 ready" pod_ready mnr-1
	write_file mnr-1 /workspace/orphan.txt only-on-$a
	crash_pod mnr-1
	wait_for 180 "mover scheduled" job_node_known ws-mn-rescue
	[[ $(job_node ws-mn-rescue) == "$a" ]] || die "mover ran on $(job_node ws-mn-rescue), but the PV lives on $a"
	wait_for 240 "rescue" ws_is ws-mn-rescue .status.lastSnapshot.executor mover
	NODE_SELECTOR=$b apply_pod mnr-2 ws-mn-rescue restic-s3 ephemeral yes >/dev/null
	wait_for 240 "mnr-2 ready on $b" pod_ready mnr-2
	[[ $(read_file mnr-2 /workspace/orphan.txt) == "only-on-$a" ]] || die "rescued data not restored on $b"
	pass "the mover followed the node-local PV to $a; the data came back on $b"
	kubectl -n "$NS" delete pod mnr-2 --wait=true >/dev/null

	fresh_workspace ws-drain
	NODE_SELECTOR=$a apply_pod drain-1 ws-drain restic-s3 ephemeral yes >/dev/null
	wait_for 180 "drain-1 ready" pod_ready drain-1
	write_file drain-1 /workspace/evicted.txt before-drain
	if ! kubectl drain "$a" --pod-selector=wsnap.dev/workspace=ws-drain --delete-emptydir-data --ignore-daemonsets --force --timeout=180s >/dev/null; then
		kubectl uncordon "$a" >/dev/null
		die "could not drain $a"
	fi
	kubectl uncordon "$a" >/dev/null
	wait_for 120 "drain-1 evicted" pod_gone drain-1
	[[ $(ws_field ws-drain .status.lastSnapshot.final) == true ]] || die "eviction did not take a final snapshot"
	NODE_SELECTOR=$b apply_pod drain-2 ws-drain restic-s3 ephemeral yes >/dev/null
	wait_for 240 "drain-2 ready" pod_ready drain-2
	[[ $(read_file drain-2 /workspace/evicted.txt) == before-drain ]] || die "drained workspace not restored on $b"
	pass "draining $a evicted the Pod with a final snapshot; the workspace resumed on $b"
	kubectl -n "$NS" delete pod drain-2 --wait=true >/dev/null
}

labels_only_pod() { # name workspace [extra spec lines]
	kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $1
  namespace: $NS
  labels: {wsnap.dev/workspace: $2, wsnap.dev/repository: restic-s3}
spec:
${3:-}
  containers:
  - name: agent
    image: alpine:3.22
    command: [sh, -c, 'trap "exit 0" TERM; sleep infinity & wait']
    volumeMounts: [{name: workspace, mountPath: /workspace}]
  volumes:
  - name: workspace
    ephemeral: {volumeClaimTemplate: {spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 1Gi}}}}}
EOF
}

# probe_pod asks the API server to admit a labeled Pod without creating it.
probe_pod() {
	kubectl -n "$NS" apply --dry-run=server -f - 2>&1 <<EOF
{"apiVersion": "v1", "kind": "Pod",
 "metadata": {"name": "probe", "namespace": "$NS",
  "labels": {"wsnap.dev/workspace": "ws-probe", "wsnap.dev/repository": "restic-s3"}},
 "spec": {"containers": [{"name": "a", "image": "alpine:3.22", "volumeMounts": [{"name": "workspace", "mountPath": "/w"}]}],
  "volumes": [{"name": "workspace", "emptyDir": {}}]}}
EOF
}
webhook_admits() { probe_pod >/dev/null; }
webhook_refuses() {
	local out; out=$(probe_pod) && return 1
	[[ $out == *"failed calling webhook"* || $out == *"no endpoints"* || $out == *"connection refused"* ]]
}

scenario_webhook() {
	log "webhook: automatic injection, fail closed"
	fresh_workspace ws-inject
	labels_only_pod inj-1 ws-inject >/dev/null
	wait_for 180 "inj-1 ready" pod_ready inj-1
	[[ $(kubectl -n "$NS" get pod inj-1 -o jsonpath='{.spec.initContainers[0].name}') == wsnap ]] || die "no sidecar injected"
	[[ $(kubectl -n "$NS" get pod inj-1 -o jsonpath='{.spec.automountServiceAccountToken}') == false ]] || die "API token not disabled"
	write_file inj-1 /workspace/auto.txt injected
	kubectl -n "$NS" delete pod inj-1 --wait=true >/dev/null
	[[ $(ws_field ws-inject .status.lastSnapshot.final) == true ]] || die "injected sidecar took no final snapshot"
	pass "a Pod with only labels got the sidecar, restored, snapshotted and took a final snapshot"

	fresh_workspace ws-inject-sb
	kubectl apply -f - >/dev/null <<EOF
apiVersion: agents.x-k8s.io/v1beta1
kind: Sandbox
metadata: {name: sb-inj, namespace: $NS}
spec:
  volumeClaimTemplates:
  - metadata: {name: workspace}
    spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 1Gi}}}
  podTemplate:
    metadata:
      labels: {wsnap.dev/workspace: ws-inject-sb, wsnap.dev/repository: restic-s3}
    spec:
      containers:
      - name: agent
        image: alpine:3.22
        command: [sh, -c, 'trap "exit 0" TERM; sleep infinity & wait']
        volumeMounts: [{name: workspace, mountPath: /workspace}]
EOF
	wait_for 180 "sandbox pod" kubectl -n "$NS" get pod sb-inj
	wait_for 180 "sb-inj ready" pod_ready sb-inj
	[[ $(kubectl -n "$NS" get pod sb-inj -o jsonpath='{.spec.initContainers[0].name}') == wsnap ]] || die "Sandbox pod not injected"
	pass "an agent-sandbox Sandbox with only labels in its podTemplate got the sidecar"
	kubectl -n "$NS" delete sandbox sb-inj --wait=true >/dev/null

	local out
	out=$(labels_only_pod bad-1 ws-bad "  automountServiceAccountToken: true" 2>&1 || true)
	[[ $out == *"automountServiceAccountToken is true"* ]] || die "a Pod with an API token was admitted: $out"
	out=$(kubectl apply -f - 2>&1 <<EOF || true
apiVersion: v1
kind: Pod
metadata: {name: bad-2, namespace: $NS, labels: {wsnap.dev/workspace: ws-bad, wsnap.dev/repository: restic-s3}}
spec:
  containers: [{name: agent, image: alpine:3.22, command: [sleep, infinity]}]
EOF
)
	[[ $out == *"has no volume"* ]] || die "a Pod without a workspace volume was admitted: $out"
	pass "unsafe Pods are refused at admission (API token, missing workspace volume)"

	kubectl -n wsnap-system scale deploy/wsnap-controller --replicas=0 >/dev/null
	wait_for 90 "controller down" no_pods wsnap-system app.kubernetes.io/name=wsnap-controller
	# Endpoint removal reaches the API server's webhook client a moment
	# after the Pods are gone. A webhook that fails open never refuses, so
	# this wait still fails then.
	wait_for 60 "admission refuses labeled Pods" webhook_refuses
	out=$(labels_only_pod down-1 ws-down 2>&1 || true)
	[[ $out == *"failed calling webhook"* || $out == *"no endpoints"* || $out == *"connection refused"* ]] || die "a labeled Pod was admitted while the webhook was down: $out"
	kubectl -n "$NS" run plain-pod --image=alpine:3.22 --restart=Never -- sleep 5 >/dev/null || die "an unlabeled Pod was blocked"
	kubectl -n "$NS" delete pod plain-pod --wait=false >/dev/null
	pass "webhook down: labeled Pods are refused (fail closed); unlabeled Pods are unaffected"
	kubectl -n wsnap-system scale deploy/wsnap-controller --replicas=2 >/dev/null
	kubectl -n wsnap-system rollout status deploy/wsnap-controller --timeout=180s >/dev/null
	wait_for 60 "webhook back" webhook_admits
}

# copy_broker_ca puts the broker's CA into gw-client. It passes the CA as
# an argument, not on stdin: "kubectl exec -i" can drop stdin and leave an
# empty file. It succeeds only when a PEM certificate landed.
copy_broker_ca() {
	local ca
	ca=$(kubectl -n wsnap-system get secret wsnap-broker-cert -o jsonpath='{.data.ca\.crt}') && [[ -n $ca ]] || return 1
	kubectl -n "$NS" exec gw-client -- sh -c "echo '$ca' | base64 -d > /tmp/broker-ca.crt && grep -q 'BEGIN CERTIFICATE' /tmp/broker-ca.crt"
}
gw_curl() { kubectl -n "$NS" exec gw-client -- curl -sS --cacert /tmp/broker-ca.crt "$@"; }
gw_status() { kubectl -n "$NS" exec gw-client -- curl -s --cacert /tmp/broker-ca.crt -o /dev/null -w '%{http_code}' "$@"; }

scenario_gateway() {
	log "gateway: REST API for platforms and gateways, authorized by Kubernetes RBAC"
	kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: ServiceAccount
metadata: {name: platform, namespace: $NS}
---
apiVersion: v1
kind: ServiceAccount
metadata: {name: nobody, namespace: $NS}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: wsnap-requester, namespace: $NS}
rules:
- apiGroups: [wsnap.dev]
  resources: [workspacesnapshots, workspacerestores]
  verbs: [create, get]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: platform-requester, namespace: $NS}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: wsnap-requester}
subjects: [{kind: ServiceAccount, name: platform, namespace: $NS}]
EOF
	kubectl -n "$NS" run gw-client --image=curlimages/curl:8.17.0 --restart=Never --command -- sleep infinity >/dev/null 2>&1 || true
	wait_for 120 "gw-client ready" pod_ready gw-client
	fresh_workspace ws-gw
	labels_only_pod gw-1 ws-gw >/dev/null
	wait_for 180 "gw-1 ready" pod_ready gw-1
	write_file gw-1 /workspace/api.txt via-rest
	local tok nob url resp
	tok=$(kubectl -n "$NS" create token platform)
	nob=$(kubectl -n "$NS" create token nobody)
	wait_for 60 "broker CA in gw-client" copy_broker_ca
	url=https://wsnap-broker.wsnap-system.svc:8443/v1/namespaces/$NS/workspaces/ws-gw
	resp=$(gw_curl -X POST -H "Authorization: Bearer $tok" -d '{"wait":true,"timeoutSeconds":45}' "$url/snapshots")
	[[ $resp == *'"phase":"Succeeded"'* && $resp == *'"executor":"sidecar"'* ]] || die "gateway snapshot did not succeed: $resp"
	pass "POST .../snapshots with wait=true: $resp"
	[[ $(gw_status -X POST -H "Authorization: Bearer $nob" "$url/snapshots") == 403 ]] || die "a service account without RBAC created a request"
	[[ $(gw_status -X POST "$url/snapshots") == 401 ]] || die "an anonymous caller was not rejected"
	pass "a service account without RBAC gets 403, an anonymous caller 401 (SubjectAccessReview, no own permission model)"
	kubectl -n "$NS" delete pod gw-1 --wait=true >/dev/null

	log "gateway: data-plane request through the agent-sandbox sandbox-router and sandboxd"
	kubectl kustomize "$AGENT_SANDBOX_DIR/sandbox-router/deploy" |
		sed 's|registry.k8s.io/agent-sandbox/sandbox-router-go:latest|registry.k8s.io/agent-sandbox/sandbox-router-go:v1.0.10|' |
		kubectl apply -f - >/dev/null
	kubectl -n agent-sandbox-system rollout status deploy/sandbox-router --timeout=180s >/dev/null 2>&1 ||
		kubectl -n agent-sandbox-system rollout status deploy/sandbox-router-deployment --timeout=180s >/dev/null
	fresh_workspace ws-gw-sb
	kubectl apply -f - >/dev/null <<EOF
apiVersion: agents.x-k8s.io/v1beta1
kind: Sandbox
metadata: {name: sb-gw, namespace: $NS}
spec:
  volumeClaimTemplates:
  - metadata: {name: workspace}
    spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 1Gi}}}
  podTemplate:
    metadata:
      labels: {wsnap.dev/workspace: ws-gw-sb, wsnap.dev/repository: restic-s3}
    spec:
      containers:
      - name: sandboxd
        image: registry.k8s.io/agent-sandbox/sandboxd:v1.0.10
        volumeMounts: [{name: workspace, mountPath: /workspace}]
EOF
	wait_for 180 "sandbox pod" kubectl -n "$NS" get pod sb-gw
	wait_for 240 "sb-gw ready" pod_ready sb-gw
	local ip router
	ip=$(kubectl -n "$NS" get pod sb-gw -o jsonpath='{.status.podIP}')
	router=http://sandbox-router-svc.agent-sandbox-system.svc:8080
	local hdr=(-H "X-Sandbox-ID: sb-gw" -H "X-Sandbox-Namespace: $NS" -H "X-Sandbox-Port: 8080" -H "X-Sandbox-Pod-IP: $ip")
	gw_curl -X PUT "${hdr[@]}" --data 'written through the router' "$router/v1/files/notes.txt" >/dev/null
	gw_curl -X PUT "${hdr[@]}" --data '' "$router/v1/files/.wsnap/requests/via-router" >/dev/null
	wait_for 60 "result through the router" bash -c "kubectl -n $NS exec gw-client -- curl -sf -H 'X-Sandbox-ID: sb-gw' -H 'X-Sandbox-Namespace: $NS' -H 'X-Sandbox-Port: 8080' -H 'X-Sandbox-Pod-IP: $ip' $router/v1/files/.wsnap/results/via-router.json | grep -q snapshotID"
	resp=$(gw_curl "${hdr[@]}" "$router/v1/files/.wsnap/results/via-router.json")
	pass "client -> sandbox-router -> sandboxd wrote a request file; the wsnap sidecar answered: $resp"
	kubectl -n "$NS" delete sandbox sb-gw --wait=true >/dev/null
	kubectl -n "$NS" delete pod gw-client --wait=false >/dev/null
}

# Each group must stand alone after setup. "requests" uses the emptyDir
# workspace of "emptydir", so both stay in one group, in this order.
group() {
	case $1 in
	controlplane) echo webhook gateway tls multinode ;;
	volumes) echo pvc ephemeral sandbox emptydir singlewriter requests ;;
	failures) echo chaos ;;
	worker) echo worker ;;
	runtimes) echo runtimes ;;
	kata) echo kata ;;
	esac
}

# diagnose prints the cluster state when a run fails, so a CI log is enough
# to tell a product bug from an environment problem.
diagnose() {
	local rc=$?
	((rc == 0)) && return
	set +e
	log "diagnostics (exit $rc)"
	kubectl get nodes -o wide
	kubectl get pods -A -o wide
	kubectl get pv
	kubectl -n "$NS" get pvc,jobs,workspaces,workspacesnapshots,workspacerestores -o wide
	kubectl -n "$NS" get workspaces -o yaml | grep -A12 '^  status:'
	kubectl -n "$NS" get events --sort-by=.lastTimestamp | tail -40
	for p in $(kubectl -n wsnap-system get pods -l app.kubernetes.io/name=wsnap-controller -o name); do
		echo "--- $p"
		kubectl -n wsnap-system logs "$p" --tail=80
	done
	for p in $(kubectl -n "$NS" get pods -o name); do
		echo "--- $p (wsnap)"
		kubectl -n "$NS" logs "$p" -c wsnap --tail=30 2>/dev/null
	done
	local kn
	for kn in $(kubectl get nodes -l wsnap-e2e/kata=true -o name 2>/dev/null); do
		echo "--- Kata journal on ${kn#node/}"
		docker exec "${kn#node/}" journalctl -u containerd --no-pager 2>/dev/null |
			grep -iE 'kata|qemu|vsock|virtiofs|guest|vmm|panic|error' | tail -150
	done
}

worker_pod() { # name workspace claim
	kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $1
  namespace: $NS
  labels: {wsnap.dev/workspace: $2, wsnap.dev/repository: restic-s3}
spec:
  ${RUNTIME_CLASS:+runtimeClassName: $RUNTIME_CLASS}
  containers:
  - name: agent
    image: alpine:3.22
    command: [sh, -c, 'trap "exit 0" TERM; sleep infinity & wait']
    volumeMounts: [{name: workspace, mountPath: /workspace}]
  volumes:
  - name: workspace
    persistentVolumeClaim: {claimName: $3}
EOF
}
worker_job_of() { kubectl -n "$NS" get jobs -l "wsnap.dev/job=worker,wsnap.dev/job-workspace=$1" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null; }
worker_pod_node() { kubectl -n "$NS" get pods -l "wsnap.dev/job=worker,wsnap.dev/job-workspace=$1" -o jsonpath='{.items[0].spec.nodeName}' 2>/dev/null; }
# A worker Job has TTL 0: once it succeeds, Kubernetes deletes it.
job_succeeded_or_gone() { [[ $(kubectl -n "$NS" get job "$1" -o jsonpath='{.status.succeeded}' 2>/dev/null) == 1 ]] || not_found -n "$NS" job "$1"; }

scenario_worker() {
	log "worker: live snapshots from a separate Pod that shares the PVC"
	kubectl -n "$NS" delete pod wk-1 wk-2 wk-rwop --ignore-not-found --wait=true >/dev/null
	kubectl -n "$NS" delete pvc wk-a wk-b wk-rwop --ignore-not-found --wait=true >/dev/null
	fresh_workspace ws-wk
	printf 'apiVersion: wsnap.dev/v1alpha1\nkind: Workspace\nmetadata: {name: ws-wk, namespace: %s}\nspec: {repositoryName: restic-s3, execution: {mode: worker}}\n' "$NS" | kubectl apply -f - >/dev/null
	apply_pvc wk-a >/dev/null
	worker_pod wk-1 ws-wk wk-a >/dev/null
	wait_for 180 "wk-1 ready" pod_ready wk-1
	local inits; inits=$(kubectl -n "$NS" get pod wk-1 -o jsonpath='{.spec.initContainers[*].name}')
	[[ $inits == wsnap-gate ]] || die "worker-mode Pod has init containers '$inits'; want only the gate"
	kubectl -n "$NS" get pod wk-1 -o jsonpath='{.spec.volumes[*].projected}' | grep -q wsnap && die "worker-mode workload got a wsnap token"
	local job; job=$(worker_job_of ws-wk)
	[[ -n $job ]] || die "no worker Job"
	[[ $(worker_pod_node ws-wk) == $(kubectl -n "$NS" get pod wk-1 -o jsonpath='{.spec.nodeName}') ]] || die "worker Pod not on the workload's node"
	pass "workload Pod has only a credential-free gate; worker Job $job runs on its node"

	write_file wk-1 /workspace/live.txt from-the-workload
	apply_request wssnap wk-live ws-wk >/dev/null
	wait_for 90 "live snapshot by the worker" req_is wssnap wk-live Succeeded
	[[ $(req_field wssnap wk-live .status.executor) == worker ]] || die "live request not served by the worker"
	pass "WorkspaceSnapshot on a live workload: the separate worker took $(req_field wssnap wk-live .status.snapshotID)"

	# Write, then stop at once: the final snapshot must wait for the
	# workload and include this write.
	write_file wk-1 /workspace/live.txt last-write
	kubectl -n "$NS" delete pod wk-1 --wait=true >/dev/null
	wait_for 120 "worker Job finished" job_succeeded_or_gone "$job"
	[[ $(ws_field ws-wk .status.lastSnapshot.final) == true && $(ws_field ws-wk .status.lastSnapshot.executor) == worker ]] ||
		die "no final snapshot by the worker: $(ws_field ws-wk .status.lastSnapshot)"
	pass "the workload stopped; the worker took the final snapshot, then finished"

	apply_pvc wk-b >/dev/null
	worker_pod wk-2 ws-wk wk-b >/dev/null
	wait_for 180 "wk-2 ready" pod_ready wk-2
	[[ $(read_file wk-2 /workspace/live.txt) == last-write ]] || die "cold restore by a worker returned $(read_file wk-2 /workspace/live.txt)"
	pass "a new workload on a new PVC got the last write: its worker restored before the gate opened"
	kubectl -n "$NS" delete pod wk-2 --wait=true >/dev/null

	kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: wk-rwop, namespace: $NS}
spec: {accessModes: [ReadWriteOncePod], resources: {requests: {storage: 1Gi}}}
EOF
	local out; out=$(worker_pod wk-rwop ws-wk wk-rwop 2>&1 || true)
	[[ $out == *ReadWriteOncePod* ]] || die "a ReadWriteOncePod claim was admitted in worker mode: $out"
	pass "a ReadWriteOncePod claim is refused at admission: no worker could mount it"
	kubectl -n "$NS" delete pvc wk-a wk-b wk-rwop --wait=false >/dev/null
}

# gVisor: a pinned release, checked against its published SHA-512.
GVISOR_RELEASE=${GVISOR_RELEASE:-20261019}
GVISOR_SHA512_x86_64=7cbc2aeeee809dcccfb40e612504a1b76f2c119790cee59e482f7b35428cccc4f71705e7e322442d07d1f7d29b5d9e72e31319fc3088d6dcbec9fc7c1f0d8881

# install_gvisor adds the runsc runtime to every kind node and registers
# the RuntimeClass "gvisor". Idempotent.
install_gvisor() {
	if kubectl get runtimeclass gvisor >/dev/null 2>&1; then
		return
	fi
	local nodes arch dir want sum
	nodes=$(kind get nodes --name "$CLUSTER")
	arch=$(docker exec "$(echo "$nodes" | head -1)" uname -m)
	[[ $arch == x86_64 ]] || die "the gVisor checksum is pinned for x86_64 only, not $arch"
	dir=$(mktemp -d)
	curl -fsSLo "$dir/gvisor.tar.bz2" "https://storage.googleapis.com/gvisor/releases/release/$GVISOR_RELEASE/$arch/gvisor.tar.bz2"
	want=$GVISOR_SHA512_x86_64
	sum=$( (sha512sum "$dir/gvisor.tar.bz2" 2>/dev/null || shasum -a 512 "$dir/gvisor.tar.bz2") | cut -d' ' -f1)
	[[ $sum == "$want" ]] || die "gVisor $GVISOR_RELEASE checksum mismatch"
	tar -xjf "$dir/gvisor.tar.bz2" -C "$dir"
	for node in $nodes; do
		docker cp "$dir/runsc" "$node:/usr/local/bin/runsc"
		docker cp "$dir/containerd-shim-runsc-v1" "$node:/usr/local/bin/containerd-shim-runsc-v1"
		docker cp "$dir/gvisor-bin" "$node:/usr/local/bin/"
		# containerd 2 (config version 3) and 1.x name the CRI plugin differently.
		docker exec "$node" sh -c '
			set -e
			if grep -q "^version = 3" /etc/containerd/config.toml; then
				plugin="plugins.\"io.containerd.cri.v1.runtime\""
			else
				plugin="plugins.\"io.containerd.grpc.v1.cri\""
			fi
			printf "\n[%s.containerd.runtimes.runsc]\n  runtime_type = \"io.containerd.runsc.v1\"\n" "$plugin" >> /etc/containerd/config.toml
			systemctl restart containerd'
	done
	rm -rf "$dir"
	kubectl apply -f - >/dev/null <<EOF
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata: {name: gvisor}
handler: runsc
EOF
	kubectl wait --for=condition=Ready nodes --all --timeout=180s >/dev/null
}

# in_gvisor checks, from inside a Pod's container, that its kernel is the
# gVisor Sentry, so a test cannot pass on runc by accident.
in_gvisor() { kubectl -n "$NS" exec "$1" -c agent -- dmesg 2>/dev/null | grep -q gVisor; }

# in_kata checks that a Pod runs its own kernel (the Kata guest), not the
# node's: under runc or gVisor uname -r would match the node, or say gVisor.
in_kata() {
	local node pod_kernel node_kernel
	node=$(kubectl -n "$NS" get pod "$1" -o jsonpath='{.spec.nodeName}') || return 1
	pod_kernel=$(kubectl -n "$NS" exec "$1" -c agent -- uname -r 2>/dev/null) || return 1
	node_kernel=$(docker exec "$node" uname -r) || return 1
	[[ -n $pod_kernel && $pod_kernel != "$node_kernel" ]] && ! in_gvisor "$1"
}

runtime_evidence() {
	printf 'runtimeClassName=%q; uname -r: %s; dmesg: %s' "$(kubectl -n "$NS" get pod "$1" -o jsonpath='{.spec.runtimeClassName}')" \
		"$(kubectl -n "$NS" exec "$1" -c agent -- uname -r 2>&1)" \
		"$(kubectl -n "$NS" exec "$1" -c agent -- dmesg 2>&1 | head -3 | tr '\n' '|')"
}

# check_runtime_modes runs wsnap in both execution modes with workload Pods
# of one RuntimeClass. The sidecar shares the workload's sandbox; the
# worker uses the node's default runtime and shares the PVC through the
# host mount. check is a function that proves a Pod runs in that runtime.
check_runtime_modes() { # runtimeClass name-prefix label check
	local rc=$1 p=$2 label=$3 check=$4 job
	kubectl -n "$NS" delete pod "$p-1" "$p-2" "${p}w-1" "${p}w-2" --ignore-not-found --wait=true >/dev/null
	kubectl -n "$NS" delete pvc "$p-a" "$p-b" "${p}w-a" "${p}w-b" --ignore-not-found --wait=true >/dev/null

	fresh_workspace "ws-$p"
	apply_pvc "$p-a" >/dev/null
	RUNTIME_CLASS=$rc apply_pod "$p-1" "ws-$p" kopia-s3 "pvc:$p-a" yes >/dev/null
	wait_for 300 "$p-1 ready" pod_ready "$p-1"
	for _ in 1 2 3 4 5; do "$check" "$p-1" && break; sleep 2; done
	"$check" "$p-1" || die "$p-1 does not run in $label: $(runtime_evidence "$p-1")"
	write_file "$p-1" /workspace/kernel.txt "written-in-$rc"
	kubectl -n "$NS" exec "$p-1" -c agent -- touch "/workspace/.wsnap/requests/in-$rc"
	wait_for 90 "snapshot inside $label" result_has "$p-1" "in-$rc" snapshotID
	kubectl -n "$NS" delete pod "$p-1" --wait=true >/dev/null
	[[ $(ws_field "ws-$p" .status.lastSnapshot.final) == true ]] || die "no final snapshot from the sidecar in $label"
	apply_pvc "$p-b" >/dev/null
	RUNTIME_CLASS=$rc apply_pod "$p-2" "ws-$p" kopia-s3 "pvc:$p-b" yes >/dev/null
	wait_for 300 "$p-2 ready" pod_ready "$p-2"
	[[ $(read_file "$p-2" /workspace/kernel.txt) == "written-in-$rc" ]] || die "restore into a $label Pod lost data"
	pass "sidecar mode in $label: snapshot on request, final snapshot, cold restore into a new $label Pod"
	kubectl -n "$NS" delete pod "$p-2" --wait=true >/dev/null

	fresh_workspace "ws-${p}w"
	printf 'apiVersion: wsnap.dev/v1alpha1\nkind: Workspace\nmetadata: {name: ws-%sw, namespace: %s}\nspec: {repositoryName: restic-s3, execution: {mode: worker}}\n' "$p" "$NS" | kubectl apply -f - >/dev/null
	apply_pvc "${p}w-a" >/dev/null
	RUNTIME_CLASS=$rc worker_pod "${p}w-1" "ws-${p}w" "${p}w-a" >/dev/null
	wait_for 300 "${p}w-1 ready" pod_ready "${p}w-1"
	for _ in 1 2 3 4 5; do "$check" "${p}w-1" && break; sleep 2; done
	"$check" "${p}w-1" || die "${p}w-1 does not run in $label: $(runtime_evidence "${p}w-1")"
	write_file "${p}w-1" /workspace/kernel.txt "written-in-$rc-worker-mode"
	apply_request wssnap "${p}w-live" "ws-${p}w" >/dev/null
	wait_for 120 "worker snapshot of a $label workload" req_is wssnap "${p}w-live" Succeeded
	[[ $(req_field wssnap "${p}w-live" .status.executor) == worker ]] || die "live request not served by the worker"
	write_file "${p}w-1" /workspace/kernel.txt "last-write-in-$rc"
	job=$(worker_job_of "ws-${p}w")
	kubectl -n "$NS" delete pod "${p}w-1" --wait=true >/dev/null
	wait_for 180 "worker Job finished" job_succeeded_or_gone "$job"
	[[ $(ws_field "ws-${p}w" .status.lastSnapshot.final) == true ]] || die "no final snapshot by the worker"
	apply_pvc "${p}w-b" >/dev/null
	RUNTIME_CLASS=$rc worker_pod "${p}w-2" "ws-${p}w" "${p}w-b" >/dev/null
	wait_for 300 "${p}w-2 ready" pod_ready "${p}w-2"
	[[ $(read_file "${p}w-2" /workspace/kernel.txt) == "last-write-in-$rc" ]] ||
		die "worker restore into a $label Pod returned $(read_file "${p}w-2" /workspace/kernel.txt)"
	pass "worker mode beside $label: live snapshot, final snapshot with the last write, restore into a new $label Pod"
	kubectl -n "$NS" delete pod "${p}w-2" --wait=true >/dev/null
	kubectl -n "$NS" delete pvc "$p-a" "$p-b" "${p}w-a" "${p}w-b" --wait=false >/dev/null
}

scenario_runtimes() {
	log "runtimes: gVisor workload Pods"
	install_gvisor
	pass "gVisor $GVISOR_RELEASE installed on every node (RuntimeClass gvisor, handler runsc)"
	check_runtime_modes gvisor gv gVisor in_gvisor
}

# Kata Containers: a pinned release, checked against the SHA-256 that
# GitHub publishes for the asset.
KATA_VERSION=${KATA_VERSION:-4.2.0}
KATA_SHA256=b828904fa3f1e49ddd7dc799c72cb1503cd1e772d354c3987c8d4189b2a623a8

# install_kata puts Kata on one worker node (the release is about 1 GB)
# and registers the RuntimeClass "kata", which schedules Pods to that
# node. It needs /dev/kvm in the node; without it the scenario is skipped
# unless E2E_REQUIRE_KATA=1. Idempotent.
install_kata() {
	if kubectl get runtimeclass kata >/dev/null 2>&1; then
		return 0
	fi
	local node dir sum cfg
	node=$(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' -o jsonpath='{.items[0].metadata.name}')
	if ! docker exec "$node" test -c /dev/kvm; then
		[[ -n ${E2E_REQUIRE_KATA:-} ]] && die "Kata needs /dev/kvm in node $node"
		return 1
	fi
	dir=$(mktemp -d)
	curl -fsSLo "$dir/kata.tar.zst" "https://github.com/kata-containers/kata-containers/releases/download/$KATA_VERSION/kata-static-$KATA_VERSION-amd64.tar.zst"
	sum=$( (sha256sum "$dir/kata.tar.zst" 2>/dev/null || shasum -a 256 "$dir/kata.tar.zst") | cut -d' ' -f1)
	[[ $sum == "$KATA_SHA256" ]] || die "Kata $KATA_VERSION checksum mismatch"
	tar --zstd -xf "$dir/kata.tar.zst" -C "$dir"
	rm -f "$dir/kata.tar.zst"
	tar -C "$dir" -cf - opt/kata | docker exec -i "$node" tar -C / -xf -
	rm -rf "$dir"
	# Kata 4 ships the Rust runtime (runtime-rs) in this tarball; 3.x
	# shipped the Go runtime under /opt/kata/bin.
	local shim=/opt/kata/runtime-rs/bin/containerd-shim-kata-v2
	cfg=/opt/kata/share/defaults/kata-containers/runtime-rs/configuration-qemu-runtime-rs.toml
	if ! docker exec "$node" test -x "$shim"; then
		shim=/opt/kata/bin/containerd-shim-kata-v2
		cfg=/opt/kata/share/defaults/kata-containers/configuration-qemu.toml
	fi
	docker exec "$node" test -x "$shim" || die "no Kata shim in the $KATA_VERSION release"
	docker exec "$node" test -f "$cfg" || die "no Kata QEMU configuration in the $KATA_VERSION release"
	# The VM boots under nested virtualization on CI machines, slower than
	# the 3 s the shim waits for the guest agent by default.
	docker exec "$node" sed -i 's/^reconnect_timeout_ms = .*/reconnect_timeout_ms = 60000/' "$cfg"
	docker exec "$node" grep -q '^reconnect_timeout_ms = 60000' "$cfg" || die "could not raise the Kata agent timeout in $cfg"
	# Under nested virtualization the guest crashed during boot ("kvm run
	# failed Bad address") while it set up the NVDIMM (DAX) that holds its
	# root image. Attach the image as a virtio block device instead.
	docker exec "$node" sed -i 's/^disable_image_nvdimm = false/disable_image_nvdimm = true/' "$cfg"
	docker exec "$node" grep -q '^disable_image_nvdimm = true' "$cfg" || die "could not turn off the Kata image NVDIMM in $cfg"
	if [[ -n ${KATA_DEBUG:-} ]]; then
		# Hypervisor, agent and runtime debug logs, the guest agent at debug
		# level; the logs go to the containerd journal of the node.
		docker exec "$node" sed -i -e 's/^enable_debug = false/enable_debug = true/' \
			-e 's/^kernel_params = "\(.*\)"/kernel_params = "\1 agent.log=debug"/' "$cfg"
	fi
	# With the systemd cgroup driver (kind's default), Kata's Rust runtime
	# creates the sandbox's cgroup through systemd over D-Bus, which a
	# kind node does not run.
	docker exec "$node" sh -c '
		set -e
		if ! command -v dbus-daemon >/dev/null; then
			apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq dbus >/dev/null
		fi
		systemctl start dbus.socket dbus.service
		test -S /run/dbus/system_bus_socket'
	docker exec "$node" sh -c '
		set -e
		ln -sf "$2" /usr/local/bin/containerd-shim-kata-v2
		if grep -q "^version = 3" /etc/containerd/config.toml; then
			plugin="plugins.\"io.containerd.cri.v1.runtime\""
		else
			plugin="plugins.\"io.containerd.grpc.v1.cri\""
		fi
		printf "\n[%s.containerd.runtimes.kata]\n  runtime_type = \"io.containerd.kata.v2\"\n  privileged_without_host_devices = true\n[%s.containerd.runtimes.kata.options]\n  ConfigPath = \"%s\"\n" "$plugin" "$plugin" "$1" >> /etc/containerd/config.toml
		systemctl restart containerd' sh "$cfg" "$shim"
	kubectl label node "$node" wsnap-e2e/kata=true --overwrite >/dev/null
	kubectl apply -f - >/dev/null <<EOF
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata: {name: kata}
handler: kata
scheduling:
  nodeSelector: {wsnap-e2e/kata: "true"}
EOF
	kubectl wait --for=condition=Ready nodes --all --timeout=180s >/dev/null
}

scenario_kata() {
	log "kata: Kata Containers workload Pods (a microVM per Pod, volumes over virtio-fs)"
	if ! install_kata; then
		echo "   skipping kata: no /dev/kvm in the nodes (load kvm, vhost_vsock and vhost_net before creating the cluster)"
		return
	fi
	pass "Kata $KATA_VERSION installed (RuntimeClass kata)"
	check_runtime_modes kata kt "Kata Containers" in_kata
}

main() {
	local steps=() s g
	(($#)) || set -- setup controlplane volumes failures worker runtimes
	for s in "$@"; do
		if [[ -n $(group "$s") ]]; then
			read -ra g <<<"$(group "$s")"
			steps+=("${g[@]}")
		else
			steps+=("$s")
		fi
	done
	trap diagnose EXIT
	for s in "${steps[@]}"; do
		case $s in
		setup) setup ;;
		*) "scenario_$s" ;;
		esac
	done
	log "all requested scenarios passed"
}
main "$@"
