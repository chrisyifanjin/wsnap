#!/usr/bin/env bash
# Run before any release image is pushed. A tag may point to an untested
# branch or to a main commit whose push CI was cancelled by a later merge.
set -euo pipefail

: "${GITHUB_REPOSITORY:?}"
commit=$(git rev-parse HEAD)
git fetch --no-tags origin main >/dev/null
if ! git merge-base --is-ancestor "$commit" FETCH_HEAD; then
	echo "release commit $commit is not on main" >&2
	exit 1
fi

successful=$(gh api -X GET "repos/$GITHUB_REPOSITORY/actions/workflows/ci.yml/runs" \
	-f head_sha="$commit" -f branch=main -f event=push -f status=success \
	--jq '.total_count')
if [[ ! $successful =~ ^[1-9][0-9]*$ ]]; then
	echo "release commit $commit has no successful main-branch CI run" >&2
	exit 1
fi

echo "release commit $commit is on main and passed main-branch CI"
