#!/bin/sh
# install-engines.sh DIR installs the Kopia and restic CLIs that the agent
# image ships, checked against the published checksums, into DIR. The
# versions come from the Dockerfile, so tests and the image never differ.
# Linux amd64 (CI runners). Run from the repository root.
set -eu
dir=$1
KOPIA=$(sed -n 's/^ARG KOPIA_VERSION=//p' Dockerfile)
RESTIC=$(sed -n 's/^ARG RESTIC_VERSION=//p' Dockerfile)
[ -n "$KOPIA" ] && [ -n "$RESTIC" ] || { echo "engine versions not found in Dockerfile" >&2; exit 1; }
mkdir -p "$dir" && cd "$dir"
k="kopia-${KOPIA}-linux-x64.tar.gz"
curl -fsSLO "https://github.com/kopia/kopia/releases/download/v${KOPIA}/$k"
curl -fsSL "https://github.com/kopia/kopia/releases/download/v${KOPIA}/checksums.txt" | grep " $k\$" | sha256sum -c -
tar -xzf "$k" --strip-components=1 "kopia-${KOPIA}-linux-x64/kopia"
r="restic_${RESTIC}_linux_amd64.bz2"
curl -fsSLO "https://github.com/restic/restic/releases/download/v${RESTIC}/$r"
curl -fsSL "https://github.com/restic/restic/releases/download/v${RESTIC}/SHA256SUMS" | grep " $r\$" | sha256sum -c -
bunzip2 -c "$r" > restic
chmod 0755 restic kopia
rm -f "$k" "$r"
