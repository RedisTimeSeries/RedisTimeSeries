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

"$HERE/get-ramp-field.sh" redis_ref
