#!/usr/bin/env bash
# Print the CI Docker image tag that redistimeseries is built/tested with.
#
# The tag is read from the `docker_image_version` field of the RAMP manifest
# (pack/ramp.yml), so image producers and consumers share one manually bumped
# value. Bump it when Docker image inputs change.

set -euo pipefail

PROGNAME="${BASH_SOURCE[0]}"
HERE="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"
ROOT="$(cd "$HERE/.." &>/dev/null && pwd)"
RAMP_FILE="$ROOT/pack/ramp.yml"

if [[ ! -f "$RAMP_FILE" ]]; then
	echo "Error: RAMP manifest not found at $RAMP_FILE" >&2
	exit 1
fi

IMAGE_TAG="$(sed -nE 's/^docker_image_version:[[:space:]]*(.*)$/\1/p' "$RAMP_FILE" | head -n1 \
	| sed -E 's/[[:space:]]*#.*$//; s/^[[:space:]]+//; s/[[:space:]]+$//; s/^"(.*)"$/\1/; s/^'\''(.*)'\''$/\1/')"

if [[ -z "$IMAGE_TAG" ]]; then
	echo "Error: 'docker_image_version' is not defined in $RAMP_FILE" >&2
	exit 1
fi

echo "$IMAGE_TAG"
