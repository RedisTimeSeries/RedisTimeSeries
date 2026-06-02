#!/usr/bin/env bash
# Print the Redis git ref that redistimeseries is built/tested against.
#
# The value lives in a single source-of-truth file (.install/redis_ref.txt) so it
# does not have to be duplicated across Dockerfiles and CI workflows. Comment
# lines (starting with '#') and blank lines in that file are ignored; the first
# remaining line is treated as the ref.
#
# Usage:
#   sbin/get-redis-ref.sh            # prints the ref, e.g. "unstable"
#
# In a GitHub Actions step:
#   echo "redis-ref=$(sbin/get-redis-ref.sh)" >> "$GITHUB_OUTPUT"

set -euo pipefail

PROGNAME="${BASH_SOURCE[0]}"
HERE="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"
ROOT="$(cd "$HERE/.." &>/dev/null && pwd)"
REF_FILE="$ROOT/.install/redis_ref.txt"

if [[ ! -f "$REF_FILE" ]]; then
	echo "Error: redis ref file not found at $REF_FILE" >&2
	exit 1
fi

# First non-comment, non-blank line, trimmed of surrounding whitespace.
REDIS_REF="$(sed -e 's/#.*//' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' "$REF_FILE" | grep -m1 -v '^$' || true)"

if [[ -z "$REDIS_REF" ]]; then
	echo "Error: no redis ref defined in $REF_FILE" >&2
	exit 1
fi

echo "$REDIS_REF"
