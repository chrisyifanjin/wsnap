# Security policy

## Report a vulnerability

Do not open a public issue. Use GitHub's private vulnerability reporting:
**Security → Report a vulnerability** on
https://github.com/chrisyifanjin/wsnap. Include the version
(`wsnap-controller --version`), the configuration (engine, backend,
credential provider), and the steps to reproduce.

You get an answer within 7 days. A fix for a confirmed issue is released as
a patch version, with an advisory that credits you unless you ask
otherwise.

## Scope

The threat model is in [docs/security.md](docs/security.md): untrusted code
in the agent container, and namespace users. Reports that a cluster
administrator, or a user who can edit a `SnapshotRepository`, can read
workspace data are expected behavior; docs/security.md lists them as known
limits.

## Supported versions

wsnap is `v1alpha1`. Only the newest release gets fixes.
