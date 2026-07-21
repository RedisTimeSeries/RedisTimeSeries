#!/usr/bin/env bash
# Install build/test dependencies for redistimeseries.
#
# Flow:
#   1. detect canonical OSNICK (uname + /etc/os-release)
#   2. source lib/pm.sh — exports PM, SUDO, install helpers
#   3. source os/<osnick>.sh — installs OS packages and inlines any quirks
#   4. source lib/setup-python.sh — uv + venv + pip deps (incl. gevent)
#
# Same calling convention as the legacy script:
#   ./install_script.sh [sudo]    # "sudo" wraps installs (Linux); empty
#                                 # for macOS or already-root containers.

set -euo pipefail

MODE="${1:-}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
LIB="$HERE/lib"

# shellcheck source=lib/detect-osnick.sh
. "$LIB/detect-osnick.sh"
# shellcheck source=lib/pm.sh
. "$LIB/pm.sh"

OSNICK="$(detect_osnick)"
if [ -z "$OSNICK" ]; then
    echo "install_script.sh: cannot detect OSNICK (uname=$(uname -s))" >&2
    exit 1
fi

osfile="$HERE/os/$OSNICK.sh"
if [ ! -f "$osfile" ]; then
    echo "install_script.sh: unsupported OSNICK '$OSNICK' (no $osfile)" >&2
    echo "Supported: $(ls "$HERE/os" 2>/dev/null | sed 's/\.sh$//' | xargs)" >&2
    exit 1
fi

echo "==> [redistimeseries] OSNICK=$OSNICK PM=$PM"

# shellcheck disable=SC1090
. "$osfile"

# Allow git operations on the checked-out source even when its uid doesn't
# match the current user (common in CI containers). Scoped to this repo
# (--local), not the host's global git config. Skipped in check-deps mode —
# a check must not mutate anything.
if [ "${CHECK_DEPS:-0}" != 1 ]; then
    git config --global --add safe.directory '*' || true
    if [ -d "$ROOT/.git" ]; then
        git -C "$ROOT" config --local --add safe.directory '*' || true
    fi
fi

# shellcheck source=lib/setup-python.sh
. "$LIB/setup-python.sh"

if [ "${CHECK_DEPS:-0}" = 1 ]; then
    n_ok=$(set -- $DEPS_OK; echo $#)
    n_missing=$(set -- $DEPS_MISSING; echo $#)
    total=$((n_ok + n_missing))
    echo
    echo "==> [redistimeseries] dependency check (OSNICK=$OSNICK, PM=$PM) — nothing was installed"
    # Full satisfied list is reassurance, not action: summarize by count,
    # print it in full only under VERBOSE=1.
    if [ "${VERBOSE:-0}" = 1 ]; then
        echo "installed:"
        for _p in $DEPS_OK; do echo "    $_p"; done
        [ -n "$DEPS_OK" ] || echo "    (none)"
    else
        echo "installed: $n_ok/$total (set VERBOSE=1 to list)"
    fi
    echo "not installed:"
    for _p in $DEPS_MISSING; do echo "    $_p"; done
    [ -n "$DEPS_MISSING" ] || echo "    (none)"
    # Non-zero exit when anything is missing so CI / callers can gate on it.
    [ "$n_missing" -eq 0 ] || exit 1
    exit 0
fi

echo "==> [redistimeseries] install_script.sh: done"
