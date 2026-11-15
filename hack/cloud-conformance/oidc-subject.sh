#!/usr/bin/env bash
# Print the exact main-branch OIDC subject for the repository. GitHub's
# default subject can contain immutable owner and repository IDs; guessing
# it from the repository name can leave cloud federation unusable.
set -euo pipefail

repo=${REPO:?set REPO=owner/name}
if [[ ! $repo =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]]; then
	echo "REPO must be owner/name" >&2
	exit 1
fi

subject=${OIDC_SUBJECT:-}
if [[ -z $subject ]]; then
	if ! command -v gh >/dev/null 2>&1; then
		echo "set OIDC_SUBJECT to the repository's exact main-branch OIDC subject, or install and authenticate gh" >&2
		exit 1
	fi
	prefix=$(gh api "repos/$repo/actions/oidc/customization/sub" \
		--jq 'if .use_default then .sub_claim_prefix else empty end')
	if [[ -z $prefix ]]; then
		echo "repository uses a custom OIDC subject; adapt the cloud trust scripts for its exact main-branch identity" >&2
		exit 1
	fi
	subject="$prefix:ref:refs/heads/main"
fi

# All three cloud trusts compare this exact value. Refuse wildcards and
# subjects for another repository, pull request, tag, or branch.
if [[ ! $subject =~ ^repo:[A-Za-z0-9_.-]+(@[0-9]+)?/[A-Za-z0-9_.-]+(@[0-9]+)?:ref:refs/heads/main$ ]]; then
	echo "OIDC_SUBJECT must be an exact repository subject for refs/heads/main" >&2
	exit 1
fi
identity=${subject#repo:}
identity=${identity%:ref:refs/heads/main}
owner=${identity%%/*}
name=${identity#*/}
if [[ ${owner%%@*}/${name%%@*} != "$repo" ]]; then
	echo "OIDC_SUBJECT must identify $repo" >&2
	exit 1
fi
printf '%s\n' "$subject"
