#!/usr/bin/env bash
# Print the CI Docker image tag derived from the RedisTimeSeries module version.
#
# Development builds use 99.99.99 in src/version.h and map to the moving
# "unstable" image tag. Release builds use the literal module version.

set -euo pipefail

PROGNAME="${BASH_SOURCE[0]}"
HERE="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"
ROOT="$(cd "$HERE/.." &>/dev/null && pwd)"
VERSION_FILE="${1:-$ROOT/src/version.h}"

if [[ ! -f "$VERSION_FILE" ]]; then
	echo "Error: version file not found at $VERSION_FILE" >&2
	exit 1
fi

read_define() {
	local name="$1"
	awk -v name="$name" '$1 == "#define" && $2 == name { print $3; exit }' "$VERSION_FILE"
}

MAJOR="$(read_define REDISTIMESERIES_VERSION_MAJOR)"
MINOR="$(read_define REDISTIMESERIES_VERSION_MINOR)"
PATCH="$(read_define REDISTIMESERIES_VERSION_PATCH)"

if [[ -z "$MAJOR" || -z "$MINOR" || -z "$PATCH" ]]; then
	echo "Error: could not parse RedisTimeSeries version from $VERSION_FILE" >&2
	exit 1
fi

VERSION="${MAJOR}.${MINOR}.${PATCH}"
if [[ "$VERSION" == "99.99.99" ]]; then
	echo "unstable"
else
	echo "$VERSION"
fi
