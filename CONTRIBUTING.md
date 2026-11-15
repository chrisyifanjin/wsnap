# Contributing to wsnap

wsnap is a `v1alpha1` prototype. Issues that reproduce a recovery or
isolation failure are especially useful. Search existing issues and pull
requests before opening a new one. For a vulnerability, follow
[SECURITY.md](SECURITY.md) and report it privately.

## Get oriented

- [Architecture](docs/architecture.md) explains the controller, broker,
  webhook, agent, and recovery state machines.
- [Engineering standard](docs/engineering.md) defines package boundaries,
  safety-rule owners, and the evidence expected in a PR.
- [Verification](docs/verification.md) describes the unit, failure, and
  Kubernetes end-to-end suites.

The Go code uses one module. `cmd/` wires binaries, `api/` holds Kubernetes
types and shared constants, and `internal/` holds the implementation. Keep
data-plane packages independent of Kubernetes clients; the architecture test
checks this boundary.

## Run checks

Use the Go version in [go.mod](go.mod). On Linux amd64, install the Kopia
and restic versions from the Dockerfile with `hack/install-engines.sh` so
engine tests run against the real CLIs:

```sh
hack/install-engines.sh /tmp/wsnap-engines
PATH="/tmp/wsnap-engines:$PATH" make vet test
go test -race ./internal/...
```

On another platform, install the matching engine versions yourself or let
CI run the real-engine tests. The remaining local checks still run with:

```sh
make vet test
go test -race ./internal/...
```

For Kubernetes changes, use a three-node kind cluster and the prerequisites
in [docs/verification.md](docs/verification.md), then run `make e2e`. CI also
runs staticcheck, govulncheck, and end-to-end groups on Kubernetes 1.30 and
1.37. If you change API types or markers, run `make generate` and include
the generated CRD and deepcopy changes.

## Send a pull request

Keep a PR focused on one behavior or invariant. Explain its upgrade impact
and show the test that would catch a regression. For a bug fix, demonstrate
the failure before the fix when practical. Changes to data deletion,
credentials, volume identity, or restore order need a failure-path test.
Update documentation when behavior or operator steps change. The
[PR template](.github/pull_request_template.md) and
[engineering standard](docs/engineering.md#9-pull-requests) give the evidence
format.

Never include live credentials, repository data, or unredacted Kubernetes
Secrets in issues, logs, test fixtures, or pull requests.
