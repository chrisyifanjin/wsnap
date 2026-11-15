#!/usr/bin/env bash
set -euo pipefail

dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cat > "$tmp/gh" <<'EOF'
#!/usr/bin/env bash
[[ $1 == api && $2 == repos/example/project/actions/oidc/customization/sub && $3 == --jq ]] || exit 1
printf '%s\n' "$FAKE_PREFIX"
EOF
chmod +x "$tmp/gh"

prefix='repo:example@123/project@456'
want="$prefix:ref:refs/heads/main"
got=$(PATH="$tmp:$PATH" FAKE_PREFIX="$prefix" REPO=example/project \
	OIDC_SUBJECT= bash "$dir/oidc-subject.sh")
[[ $got == "$want" ]] || { echo "immutable subject: got $got, want $want" >&2; exit 1; }

got=$(REPO=example/project OIDC_SUBJECT="$want" bash "$dir/oidc-subject.sh")
[[ $got == "$want" ]] || { echo "explicit subject: got $got, want $want" >&2; exit 1; }

legacy='repo:example/project:ref:refs/heads/main'
got=$(REPO=example/project OIDC_SUBJECT="$legacy" bash "$dir/oidc-subject.sh")
[[ $got == "$legacy" ]] || { echo "legacy subject: got $got, want $legacy" >&2; exit 1; }

for unsafe in 'repo:example/project:pull_request' 'repo:example/project:*' \
	'repo:example/project:ref:refs/heads/other' \
	'repo:other/project:ref:refs/heads/main' \
	'repo:example/project:ref:refs/heads/main"'; do
	if REPO=example/project OIDC_SUBJECT="$unsafe" bash "$dir/oidc-subject.sh" >/dev/null 2>&1; then
		echo "accepted unsafe OIDC subject: $unsafe" >&2
		exit 1
	fi
done

if PATH="$tmp:$PATH" FAKE_PREFIX= REPO=example/project \
	OIDC_SUBJECT= bash "$dir/oidc-subject.sh" >/dev/null 2>&1; then
	echo "accepted a custom subject without an explicit override" >&2
	exit 1
fi

echo "cloud OIDC subject checks passed"
