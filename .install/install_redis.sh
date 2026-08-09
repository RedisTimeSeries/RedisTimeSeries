#!/usr/bin/env bash
set -e

if [ -z "${REDIS_REF}" ]; then
    echo "Error: REDIS_REF environment variable is required"
    exit 1
fi

echo "Installing Redis from ref: ${REDIS_REF}"

# SANITIZER can be passed to build Redis with sanitizer support (e.g., SANITIZER=address).
#
# Redis's Makefile only learned about the SANITIZER flag in 7.0. This branch builds
# Redis 6.2, whose Makefile ignores SANITIZER entirely: `make SANITIZER=address` yields
# a NON-instrumented server, which then aborts on load of the ASan-built module
# ("ASan runtime does not come first in initial library list") and every flow test gets
# connection-refused. So inject the flags 7.0+ sets under SANITIZER via the CFLAGS/LDFLAGS
# vars 6.2 honors, and force MALLOC=libc (ASan and jemalloc can't coexist).
git clone https://github.com/redis/redis.git
cd redis
git fetch origin ${REDIS_REF}
git checkout ${REDIS_REF}
git submodule update --init --recursive
if [ -n "${SANITIZER}" ]; then
    echo "Building Redis with SANITIZER=${SANITIZER} (manual flags for Redis < 7.0)"
    make MALLOC=libc \
         CFLAGS="-fsanitize=${SANITIZER} -fno-omit-frame-pointer -fno-sanitize-recover=all" \
         LDFLAGS="-fsanitize=${SANITIZER}" \
         -j"$(nproc)"
else
    make -j"$(nproc)"
fi
make install
cd ..

echo "Redis installed successfully"
redis-server --version
