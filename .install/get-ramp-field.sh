#!/usr/bin/env bash
# Print a top-level field from pack/ramp.yml, stripping inline comments,
# surrounding whitespace, and optional quotes.

set -euo pipefail

if [[ $# -ne 1 ]]; then
	echo "Usage: $0 <field>" >&2
	exit 1
fi

FIELD="$1"
PROGNAME="${BASH_SOURCE[0]}"
HERE="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"
ROOT="$(cd "$HERE/.." &>/dev/null && pwd)"
RAMP_FILE="$ROOT/pack/ramp.yml"

if [[ ! -f "$RAMP_FILE" ]]; then
	echo "Error: RAMP manifest not found at $RAMP_FILE" >&2
	exit 1
fi

VALUE="$(sed -nE "s/^${FIELD}:[[:space:]]*(.*)$/\\1/p" "$RAMP_FILE" | head -n1 \
	| sed -E 's/[[:space:]]*#.*$//; s/^[[:space:]]+//; s/[[:space:]]+$//; s/^"(.*)"$/\1/; s/^'\''(.*)'\''$/\1/')"

if [[ -z "$VALUE" ]]; then
	echo "Error: '$FIELD' is not defined in $RAMP_FILE" >&2
	exit 1
fi

echo "$VALUE"
