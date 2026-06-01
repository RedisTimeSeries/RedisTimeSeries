#!/usr/bin/env bash
set -e

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# If REDIS_REF was not provided explicitly, fall back to the single
# source-of-truth file shipped alongside this script (.install/redis_ref.txt).
# Comment ('#') and blank lines are ignored; the first remaining line is used.
if [ -z "${REDIS_REF}" ]; then
    REF_FILE="$HERE/redis_ref.txt"
    if [ -f "$REF_FILE" ]; then
        REDIS_REF="$(sed -e 's/#.*//' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' "$REF_FILE" | grep -m1 -v '^$' || true)"
    fi
fi

if [ -z "${REDIS_REF}" ]; then
    echo "Error: REDIS_REF is not set and no ref found in $HERE/redis_ref.txt"
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
