# Engineering standard

This page states how wsnap code is organized and changed, and how CI
enforces it. Each rule has a reason. Where a rule comes from an outside
source, the source is linked.

## 1. One module, three kinds of directory

wsnap is one Go module:

| Directory | Holds |
|---|---|
| `cmd/` | The binaries. They only parse flags and wire packages together. |
| `api/` | The Kubernetes types (CRDs) and the API constants (labels, annotations, Job kinds, executors, rescue phases). |
| `internal/` | Everything else. Other modules cannot import it, so it can change freely. |

This is the layout the Go team recommends for a server project
([Go module layout](https://go.dev/doc/modules/layout)). Add a second
module only when something must be released on its own schedule.

## 2. One job per package

| Package | Job | Must not know about |
|---|---|---|
| `agent` | Local data: restore, snapshot, swap, the sidecar and worker loops | Kubernetes (it talks only to the broker) |
| `engine` | Run a snapshot tool (Kopia, restic, exec) | Kubernetes, credentials providers |
| `credentials` | Issue a credential scoped to one workspace and role | Kubernetes |
| `protocol` | The agent–broker wire types | Everything else |
| `broker` | Authenticate callers, decide roles, issue credentials, record results | The controllers, the webhook |
| `controller` | Reconcile Kubernetes objects: PVs, requests, Jobs, repositories | The broker, the webhook |
| `webhook` | Admission: inject or validate the agent | The broker, the controllers |
| `workspace` | Rules that more than one component applies: identity, volume binding, PV marks, isolation | — |
| `agentpod` | What every agent container looks like | Kubernetes clients |

Dependencies flow one way. The data plane (`agent`, `engine`,
`credentials`, `protocol`) runs beside untrusted code and imports nothing
from Kubernetes. The three control-plane components share rules through
`workspace`, `agentpod`, `credentials` and `protocol`, never by importing
each other. **`internal/archtest` fails CI when a change breaks one of
these rules.**

Split a large file when its responsibilities separate cleanly; list the
files in the package doc (see `broker` and `agent`). Do not split a
package just to reach a line count. The Google Go style guide warns against
both "a single file with many thousands of lines" and "many tiny files"
([Go style best practices: package size](https://google.github.io/styleguide/go/best-practices)).

## 3. One owner for each safety rule

A safety rule written twice drifts apart. Each rule below lives in one
place, and every mode (sidecar, worker, Job) calls it:

| Rule | Owner |
|---|---|
| A writer is admitted only alone, never while an older volume awaits rescue, and its volume is protected before any credential | `broker.admitWriter` |
| `lastSnapshot` moves forward only in writer epochs | `broker.recordInEpochOrder` |
| A released PV is safe to reclaim only after its last writer's final snapshot | `workspace.HasFinalSnapshot` |
| PVC and PV identity (UIDs, `claimRef`) | `workspace.ResolveVolume`, `workspace.ValidateBinding` |
| A Job is served only while it matches its registration | `workspace.JobRefMatches` |
| Workload containers cannot reach agent secrets or the API | `workspace.CheckIsolation`, `workspace.CheckWorkerIsolation` (shared scans) |
| How an agent container looks, how it trusts the broker CA, the worker gate | `agentpod` |
| The worker gate opens only on a fresh marker from a live worker | `agent.WaitWorker`, `agent.heartbeatWorkerReady` |
| A worker marks a snapshot final only after its workload stopped | `agent.watchWorkload` with the broker's `/v1/worker-state` |
| Rescue phases and their transitions | `api/v1alpha1` (`Rescue*`), `controller.PVReconciler` |
| Job kinds and executors | `api/v1alpha1` (`Job*`, `Executor*`) |

A change that adds a mode or a path calls the owner. It does not copy the
check.

## 4. Small interfaces, defined by the caller

Define an interface in the package that uses it, when a real caller needs
it, and return concrete types from implementations
([Go Code Review Comments: interfaces](https://go.dev/wiki/CodeReviewComments#interfaces)).
wsnap uses no dependency-injection framework: `cmd/` builds structs and
passes them in.

## 5. Explicit extension contracts

| To add | Implement | It must pass |
|---|---|---|
| A snapshot engine | `engine.Engine` (or the exec contract in api.md) | `TestRoundTrip` (snapshot, restore, maintenance) and `TestUnreadableFileDoesNotFailTheSnapshot` |
| A storage backend or credential provider | `credentials.Provider`, `workspace.Backend` | `TestRoundTrip` and `TestScopedIsolation` (workspace b's credential cannot open workspace a) |
| An execution mode | broker roles, `agentpod`, a reconciler | `admitWriter`, the isolation checks, and its own e2e group |
| A workspace volume kind | `workspace.ResolveVolume` | the `pvc`, `ephemeral` and `chaos` e2e scenarios, or a new one |

Real clouds run the same contract tests in `cloud-conformance.yml`.

## 6. Readable code

- `gofmt`, `go vet` and staticcheck are clean (CI).
- Names say what a thing is, without stutter (`broker.Server`, not
  `broker.BrokerServer`). Error strings start lowercase and have no final
  period. A wrapped error says what failed, with `%w` at the end
  ([Go style best practices](https://google.github.io/styleguide/go/best-practices)).
- `context.Context` is the first parameter.
- A function does one thing. When a function grows past a screen, look
  for named stages (see `broker.credentials` and `report`). Length alone
  is not the rule; clarity is.
- A comment explains why a safety check exists: the attack, race or
  failure it prevents. Say what the code does only when that is not obvious.
- API values (labels, annotations, phases, kinds) are named constants in
  `api/v1alpha1`, never repeated literals.

## 7. Test the failure path

A change that touches data deletion, credentials, or restore order tests
how it fails, not only the success path:

- stale objects (a cached PV, an old resource version);
- retries and redelivery (a request delivered twice, a lost report);
- replacement objects with the same name and a new UID (Pod, PVC, PV, Job);
- crashes between two writes (a Job created but not registered);
- denied access (another workspace's credential, a Forbidden Secret).

Each bug fix comes with a test that fails without the fix. Check that by
reverting the fix (a mutation check). Test failures say what was wrong,
with inputs, as got/want
([Go Code Review Comments: useful test failures](https://go.dev/wiki/CodeReviewComments#useful-test-failures)).

## 8. Kubernetes controllers

- Reconcilers are idempotent: the same key can arrive many times
  ([Kubebuilder good practices](https://book.kubebuilder.io/reference/good-practices)).
- Decisions that delete data or grant access read live objects (the API
  reader), not the cache.
- Conditions carry a reason and a message that says how to fix the problem
  (see `SecretForbidden`).
- A status update that depends on what was read uses that read's
  resource version, so a concurrent change becomes a conflict and a retry.

## 9. Pull requests

A pull request states:

1. **The invariant**: what must stay true, and what changed.
2. **Upgrade impact**: flags, ports, RBAC, API fields, data.
3. **Evidence**: the tests that cover it, including mutation checks for
   fixes, and the e2e result.

CI requires `gofmt`, `go vet`, staticcheck, unit and race tests,
govulncheck, the architecture test, and every e2e group.
`.github/pull_request_template.md` asks for the three points above.
