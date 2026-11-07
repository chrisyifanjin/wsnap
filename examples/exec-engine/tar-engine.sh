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
	# Keep: the newest WSNAP_KEEP_LAST, the newest of each of the last
	# WSNAP_KEEP_DAILY days that have snapshots (IDs start with the UTC
	# date), and the pinned WSNAP_KEEP_IDS. Delete the rest.
	all=$(ls "$repo/snapshots" | grep '\.tar\.gz$' | sed 's/\.tar\.gz$//' | sort)
	keep=$(
		printf '%s\n' "$all" | tail -n "$WSNAP_KEEP_LAST"
		printf '%s\n' "$all" | awk '{ last[substr($0, 1, 8)] = $0 } END { for (d in last) print d, last[d] }' |
			sort -r | head -n "${WSNAP_KEEP_DAILY:-0}" | cut -d' ' -f2
		printf '%s\n' "${WSNAP_KEEP_IDS:-}"
	)
	printf '%s\n' "$all" | grep -vxF -e "$keep" -e '' | while read -r id; do
		rm -f "$repo/snapshots/$id.tar.gz"
	done
	;;
*)
	echo "unknown verb $1" >&2; exit 2
	;;
esac
