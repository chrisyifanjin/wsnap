#!/usr/bin/env bash
# Verify that a release-style OCI export contains both supported platforms.
set -euo pipefail

archive=${1:?OCI archive required}
metadata=${2:?Buildx metadata file required}
digest=$(jq -er '."containerimage.digest"' "$metadata")
[[ $digest =~ ^sha256:[0-9a-f]{64}$ ]]
index_digest=$(tar -xOf "$archive" index.json | jq -er '.manifests[0].digest')
[[ $digest == "$index_digest" ]]
tar -xOf "$archive" "blobs/sha256/${digest#sha256:}" |
	jq -e '
	  [.manifests[] | select(.platform.os == "linux" and .platform.architecture == "amd64")] | length == 1
	' >/dev/null
tar -xOf "$archive" "blobs/sha256/${digest#sha256:}" |
	jq -e '
	  [.manifests[] | select(.platform.os == "linux" and .platform.architecture == "arm64")] | length == 1
	' >/dev/null
echo "OCI image $digest includes linux/amd64 and linux/arm64"
