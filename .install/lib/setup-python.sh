#!/usr/bin/env bash
# Provision uv + a project-local venv + pip dependencies for redistimeseries.
#
# Sourced by install_script.sh after the OS package install. Reads $ROOT
# (set by install_script.sh) and $HERE (path to .install/). Writes
# $ROOT/venv/.
#
# Replaces the legacy `.install/common_installations.sh` (now deleted): all
# pip work lives here so `make bootstrap` is just install_script.sh + done.
#
# uv installs to ~/.local/bin (or ~/.cargo/bin), which is not on PATH in the
# non-login bootstrap subshell — detect it there too, not just via PATH.
_have_uv() { command -v uv >/dev/null 2>&1 || [ -x "$HOME/.local/bin/uv" ] || [ -x "$HOME/.cargo/bin/uv" ]; }

# list / dry-run are read-only dependency reports — they must run in EVERY
# environment, including inside a `docker build`, so handle them BEFORE the
# /.dockerenv skip below (which only short-circuits the real venv+pip work).
if [ "${CHECK_DEPS:-0}" = 1 ]; then
    # uv presence, routed through OPTIONAL_PKGS like any other dep.
    if _have_uv; then _uv=ok; else _uv=missing; fi
    if _is_optional uv; then
        [ "$_uv" = ok ] && DEPS_OPT_OK="$DEPS_OPT_OK uv" || DEPS_OPT_MISSING="$DEPS_OPT_MISSING uv"
    else
        [ "$_uv" = ok ] && DEPS_OK="$DEPS_OK uv" || DEPS_MISSING="$DEPS_MISSING uv"
    fi
    return 0 2>/dev/null || exit 0
fi

if [ "${DRY_RUN:-0}" = 1 ]; then
    # print the uv install command only if uv is missing; never touch venv/pip.
    _have_uv || _dry_line "curl -LsSf https://astral.sh/uv/install.sh | sh   # uv (then: uv venv + uv pip install -r ...)"
    return 0 2>/dev/null || exit 0
fi

# Inside a `docker build`, every Dockerfile.<osnick> creates its own dedicated
# venv at /opt/.venv after this script runs, so the venv we'd build here would
# just bloat the image layer and double install time. Detect the Docker build
# environment via /.dockerenv (created by Docker itself; never present on a
# dev host) and skip the venv + pip work entirely in that case.
if [ -f /.dockerenv ]; then
    echo "==> [redistimeseries] /.dockerenv detected; skipping venv + pip setup (Dockerfile builds its own /opt/.venv)"
    return 0 2>/dev/null || exit 0
fi

# Required by callers — set by install_script.sh. Fail fast if absent rather
# than producing a confusing `uv venv ""` failure later.
: "${ROOT:?setup-python.sh: ROOT not set (must be sourced by install_script.sh)}"
: "${HERE:?setup-python.sh: HERE not set (must be sourced by install_script.sh)}"

if ! command -v uv >/dev/null 2>&1; then
    echo "==> [redistimeseries] installing uv"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
fi

if ! command -v uv >/dev/null 2>&1; then
    echo "setup-python.sh: WARNING: uv installation failed; skipping venv setup" >&2
    return 0 2>/dev/null || exit 0
fi

# A stale or partial venv (e.g. a previous `make bootstrap` aborted halfway,
# or the developer ran `python3 -m venv` against a now-missing python) shows
# up as `$ROOT/venv` existing but `bin/python` not being executable. Wipe
# and recreate so we don't trip the executable check below.
if [ -d "$ROOT/venv" ] && [ ! -x "$ROOT/venv/bin/python" ]; then
    echo "==> [redistimeseries] $ROOT/venv looks broken (no bin/python); recreating"
    rm -rf "$ROOT/venv"
fi

if [ ! -d "$ROOT/venv" ]; then
    uv venv "$ROOT/venv" --python "${SETUP_PYTHON_VERSION:-3.12}"
fi

if [ ! -x "$ROOT/venv/bin/python" ]; then
    echo "setup-python.sh: missing $ROOT/venv/bin/python (uv venv step failed?)" >&2
    exit 1
fi

# All pip work goes through `uv pip --python <venv>` (never --system, never
# under sudo). Sourcing under sudo would otherwise resolve uv against /usr's
# python3 (3.6 on EL8) and break rltest.
uv_pip() {
    uv pip install --python "$ROOT/venv/bin/python" "$@"
}

uv_pip --upgrade pip wheel "setuptools<81"
uv_pip -r "$HERE/build_package_requirements.txt"

# tests/flow/requirements.txt is committed at the repo root; absent only on
# unusual checkouts (e.g. Dockerfile build context that excluded tests/).
if [ -f "$ROOT/tests/flow/requirements.txt" ]; then
    (cd "$ROOT" && uv_pip -r tests/flow/requirements.txt)
fi

# tests/flow/test_short_read.py imports gevent but the requirements file
# keeps it commented out (heavy dep). Install explicitly here.
uv_pip gevent
