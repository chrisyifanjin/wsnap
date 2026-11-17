#!/usr/bin/env bash
# Publish the already-built assets. A pre-release tag must never appear as a
# stable or "Latest" release. Keep version-specific notes with the tag.
set -euo pipefail
: "${VERSION:?}"

notes_file=".github/release-notes/$VERSION.md"
notes=(--generate-notes)
if [[ -f $notes_file ]]; then
	notes=(--notes-file "$notes_file")
fi

flags=(--verify-tag)
version_without_build=${VERSION%%+*}
if [[ $version_without_build == *-* ]]; then
	flags+=(--prerelease --latest=false)
fi

gh release create "$VERSION" install.yaml config/prometheus/rules.yaml \
	"${notes[@]}" "${flags[@]}"
