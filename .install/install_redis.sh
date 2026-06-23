#!/usr/bin/env bash
set -e

# Redis source is pre-staged at /workspace/redis by the Dockerfile (which
# COPYs redis-src/ from the build context). The CI workflow checks it out
# via actions/checkout against redislabsdev/Redis; for local docker builds
# the developer must clone redis source to ./redis-src/ before building.
if [ ! -d /workspace/redis ]; then
    echo "Error: /workspace/redis not present."
    echo "       CI pre-stages it via actions/checkout into redis-src/."
    echo "       For local builds, clone redis source to ./redis-src/ first."
    exit 1
fi

# SANITIZER can be passed to build Redis with sanitizer support (e.g., SANITIZER=address)
if [ -n "${SANITIZER}" ]; then
    echo "Building Redis with SANITIZER=${SANITIZER}"
fi

cd /workspace/redis
make SANITIZER=${SANITIZER:-} -j$(nproc)
make install
cd ..

# `make install` drops bs_speedb.so / libspeedb.so into /usr/local/lib, but
# that path is not on the default loader search path on many of our base
# images (jammy, alpine, the rocky/alma family, etc.). Without this, the
# Flex leg's redis-server fails with:
#   "Failed to load bigredis driver 'bs_speedb.so': ... No such file or
#    directory" and exits before tests can connect (#80).
if [ -d /etc/ld.so.conf.d ]; then
    echo "/usr/local/lib" > /etc/ld.so.conf.d/usrlocal.conf
fi
if command -v ldconfig >/dev/null 2>&1; then
    ldconfig 2>/dev/null || true
fi
if [ -f /lib/ld-musl-x86_64.so.1 ] || [ -f /lib/ld-musl-aarch64.so.1 ]; then
    arch=$(uname -m)
    musl_path_file="/etc/ld-musl-${arch}.path"
    if [ -f "$musl_path_file" ]; then
        grep -q '^/usr/local/lib$' "$musl_path_file" || \
            echo "/usr/local/lib" >> "$musl_path_file"
    else
        printf '/lib\n/usr/lib\n/usr/local/lib\n' > "$musl_path_file"
    fi
fi

echo "Redis installed successfully"
redis-server --version
