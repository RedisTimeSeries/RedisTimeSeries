#!/usr/bin/env bash
# Print the CI Docker image tag that redistimeseries is built/tested with.
#
# The tag is read from the `docker_image_version` field of the RAMP manifest
# (pack/ramp.yml), so image producers and consumers share one manually bumped
# value. Bump it when Docker image inputs change.

set -euo pipefail

PROGNAME="${BASH_SOURCE[0]}"
HERE="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"

"$HERE/get-ramp-field.sh" docker_image_version
