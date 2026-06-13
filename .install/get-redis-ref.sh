#!/usr/bin/env bash
# Print the Redis git ref that redistimeseries is built/tested against.
#
# The ref is read from the `redis_ref` field of the RAMP manifest
# (pack/ramp.yml), so it lives in a single place and is not duplicated across
# Dockerfiles and CI workflows. Set it to "unstable" to track the Redis
# development branch, or to a specific tag/branch (e.g. "8.8") for a release.
# The value may be quoted or unquoted; an inline '#' comment, if any, is stripped.
#
# It lives under .install/ (not sbin/) so it is copied into the Docker build
# context together with the rest of .install/, letting install_redis.sh reuse
# the very same reader instead of re-implementing the parse.
#
# Usage:
#   .install/get-redis-ref.sh        # prints the ref, e.g. "unstable" or "8.8"
#
# In a GitHub Actions step:
#   echo "redis-ref=$(.install/get-redis-ref.sh)" >> "$GITHUB_OUTPUT"

set -euo pipefail

PROGNAME="${BASH_SOURCE[0]}"
HERE="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"
ROOT="$(cd "$HERE/.." &>/dev/null && pwd)"
RAMP_FILE="$ROOT/pack/ramp.yml"

if [[ ! -f "$RAMP_FILE" ]]; then
	echo "Error: RAMP manifest not found at $RAMP_FILE" >&2
	exit 1
fi

# Value of the top-level `redis_ref:` key, with inline comment,
# surrounding whitespace and quotes stripped.
REDIS_REF="$(sed -nE 's/^redis_ref:[[:space:]]*(.*)$/\1/p' "$RAMP_FILE" | head -n1 \
	| sed -E 's/[[:space:]]*#.*$//; s/^[[:space:]]+//; s/[[:space:]]+$//; s/^"(.*)"$/\1/; s/^'\''(.*)'\''$/\1/')"

if [[ -z "$REDIS_REF" ]]; then
	echo "Error: 'redis_ref' is not defined in $RAMP_FILE" >&2
	exit 1
fi

echo "$REDIS_REF"
