#!/usr/bin/env bash
# Alpine Linux. ALPINE_BASE in lib/packages.sh already includes the musl-specific
# extras (musl-dev, linux-headers, gcompat, libstdc++, libgcc, bsd-compat-
# headers, ...) that used to live in a separate quirks file.
#
# The py3-* prebuilt packages avoid building those C extensions against musl
# from source during pip install; openblas-dev / xsimd back math/SIMD test
# deps.

# shellcheck source=../lib/packages.sh
. "$LIB/packages.sh"

alpine_default_install
apk_install py3-cryptography py3-numpy py3-psutil openblas-dev xsimd

# uv's standalone musl CPython is clang-built and bakes clang-only flags
# (--rtlib=compiler-rt) into its sysconfig, so setup-python.sh's `uv venv`
# then fails to compile sdist-only test deps that lack musl wheels (psutil,
# via rltest) with Alpine's gcc. Use Alpine's own python3 (installed above
# with python3-dev) and forbid uv from downloading a managed interpreter.
export SETUP_PYTHON_VERSION="${SETUP_PYTHON_VERSION:-$(command -v python3)}"
export UV_PYTHON_DOWNLOADS=never
echo "==> [redistimeseries] alpine: SETUP_PYTHON_VERSION=$SETUP_PYTHON_VERSION UV_PYTHON_DOWNLOADS=never"
