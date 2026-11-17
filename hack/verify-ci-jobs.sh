#!/usr/bin/env bash
# Check each job in this attempt. GitHub's aggregate needs result can report
# success even when a matrix leg was cancelled before a runner acquired it.
set -euo pipefail

: "${GITHUB_REPOSITORY:?}"
: "${GITHUB_RUN_ID:?}"
: "${GITHUB_RUN_ATTEMPT:?}"
: "${EXPECTED_E2E_LEGS:?}"
[[ $EXPECTED_E2E_LEGS =~ ^[1-9][0-9]*$ ]]
expected=$((EXPECTED_E2E_LEGS + 1)) # the Go job plus every E2E leg
endpoint="repos/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID/attempts/$GITHUB_RUN_ATTEMPT/jobs?per_page=100"

# The API can lag job completion briefly. Fail closed if the expected jobs do
# not all appear as completed within one minute.
jobs='[]'
for _ in {1..12}; do
	jobs=$(gh api -X GET "$endpoint" --jq '[.jobs[] | select(.name == "go" or (.name | startswith("e2e ("))) | {name, status, conclusion}]')
	if jq -e --argjson expected "$expected" \
		'length == $expected and all(.[]; .status == "completed")' <<<"$jobs" >/dev/null; then
		break
	fi
	sleep 5
done

if ! jq -e --argjson expected "$expected" \
	'length == $expected and all(.[]; .conclusion == "success")' <<<"$jobs" >/dev/null; then
	echo "CI requires Go and all $EXPECTED_E2E_LEGS Kubernetes E2E legs to succeed:" >&2
	jq -r '.[] | "\(.name): \(.status)/\(.conclusion)"' <<<"$jobs" >&2
	exit 1
fi
echo "Go and all $EXPECTED_E2E_LEGS Kubernetes E2E legs succeeded"
