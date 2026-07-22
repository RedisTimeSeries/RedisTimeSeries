#!/usr/bin/env bash
set -e

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# If REDIS_REF was not provided explicitly, derive it from the manifest's
# compatible_redis_version (pack/ramp.yml) via the shared reader that ships in
# this same directory (.install/get-redis-ref.sh). Both this script and
# pack/ramp.yml are copied into the Docker build context, so the reader works
# here exactly as it does in CI.
if [ -z "${REDIS_REF}" ]; then
    REDIS_REF="$(bash "$HERE/get-redis-ref.sh")"
fi

if [ -z "${REDIS_REF}" ]; then
    echo "Error: REDIS_REF is not set and could not be derived from pack/ramp.yml"
    exit 1
fi

echo "Installing Redis from ref: ${REDIS_REF}"

# SANITIZER can be passed to build Redis with sanitizer support (e.g., SANITIZER=address)
if [ -n "${SANITIZER}" ]; then
    echo "Building Redis with SANITIZER=${SANITIZER}"
fi

git clone https://github.com/redis/redis.git 
cd redis
git fetch origin ${REDIS_REF}
git checkout ${REDIS_REF}
git submodule update --init --recursive
# For Valgrind runs only, build Redis without _FORTIFY_SOURCE. At -O2/-O3 the
# distro compiler lowers correct memmove() calls into __memcpy_chk(), which
# Valgrind's direction-agnostic overlap check misreports as errors (e.g. in
# readSyncBulkPayload). Other builds keep fortification unchanged. See RED-195208.
REDIS_EXTRA_CFLAGS=""
if [ -n "${VALGRIND}" ]; then
    echo "Building Redis without _FORTIFY_SOURCE for Valgrind"
    REDIS_EXTRA_CFLAGS="-U_FORTIFY_SOURCE"
fi
# `install` builds `all` first, so a single invocation keeps the compiler flags
# consistent and avoids a redundant rebuild.
# BUILD_TLS=yes so flow tests can exercise TLS (incl. tls-cluster) topologies;
# TLS ports/certs are opt-in at runtime via redis.conf, so this is a no-op for
# every other topology.
make SANITIZER=${SANITIZER:-} REDIS_CFLAGS="${REDIS_EXTRA_CFLAGS}" BUILD_TLS=yes install -j$(nproc)
cd ..

echo "Redis installed successfully"
redis-server --version
