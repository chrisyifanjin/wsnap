#!/bin/sh
# tar-engine.sh is a minimal wsnap exec engine. It stores each snapshot as a
# gzipped tarball in a filesystem backend. It shows the contract only: it
# has no deduplication or encryption, so do not use it for real data.
set -eu

repo=$(printf '%s' "$WSNAP_BACKEND" | sed -n 's/.*"filesystem":{"path":"\([^"]*\)".*/\1/p')
[ -n "$repo" ] || { echo "tar-engine supports the filesystem backend only" >&2; exit 2; }

case "$1" in
init)
	if [ "$WSNAP_READ_ONLY" = true ]; then
		[ -d "$repo/snapshots" ] || exit 3 # repository does not exist
	else
		mkdir -p "$repo/snapshots"
	fi
	;;
snapshot)
	id=$(date -u +%Y%m%dT%H%M%S)-$$
	tmp="$repo/snapshots/.$id.tmp"
	excludes=$(printf '%s\n' "$WSNAP_EXCLUDE" | sed 's|^|--exclude=./|')
	# shellcheck disable=SC2086
	tar -C "$WSNAP_DIR" $excludes -czf "$tmp" .
	mv "$tmp" "$repo/snapshots/$id.tar.gz"
	printf '{"id":"%s","time":"%s"}\n' "$id" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	;;
latest)
	last=$(ls "$repo/snapshots" 2>/dev/null | grep '\.tar\.gz$' | sort | tail -n 1 || true)
	[ -z "$last" ] && exit 0
	id=${last%.tar.gz}
	printf '{"id":"%s","time":"%s"}\n' "$id" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	;;
restore)
	tar -C "$WSNAP_DIR" -xzf "$repo/snapshots/$WSNAP_SNAPSHOT_ID.tar.gz"
	;;
maintain)
	ls "$repo/snapshots" | grep '\.tar\.gz$' | sort |
		awk -v keep="$WSNAP_KEEP_LAST" '{ f[NR] = $0 } END { for (i = 1; i <= NR - keep; i++) print f[i] }' |
		while read -r f; do
			# Keep pinned snapshots (WSNAP_KEEP_IDS, one ID per line).
			printf '%s\n' "$WSNAP_KEEP_IDS" | grep -qxF "${f%.tar.gz}" || rm -f "$repo/snapshots/$f"
		done
	;;
*)
	echo "unknown verb $1" >&2; exit 2
	;;
esac
