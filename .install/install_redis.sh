#!/usr/bin/env bash
set -e

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# If REDIS_REF was not provided explicitly, fall back to the single
# source-of-truth field (`redis_ref`) in the RAMP manifest pack/ramp.yml, which
# is copied into the Docker build context next to this script's parent dir.
if [ -z "${REDIS_REF}" ]; then
    RAMP_FILE="$HERE/../pack/ramp.yml"
    if [ -f "$RAMP_FILE" ]; then
        REDIS_REF="$(sed -nE 's/^redis_ref:[[:space:]]*(.*)$/\1/p' "$RAMP_FILE" | head -n1 \
            | sed -E 's/[[:space:]]*#.*$//; s/^[[:space:]]+//; s/[[:space:]]+$//; s/^"(.*)"$/\1/; s/^'\''(.*)'\''$/\1/')"
    fi
fi

if [ -z "${REDIS_REF}" ]; then
    echo "Error: REDIS_REF is not set and 'redis_ref' not found in $HERE/../pack/ramp.yml"
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
make SANITIZER=${SANITIZER:-} -j$(nproc)
make install
cd ..

echo "Redis installed successfully"
redis-server --version
