#!/bin/bash
set -e

if [ -z "${REDIS_REF}" ]; then
    echo "Error: REDIS_REF environment variable is required"
    exit 1
fi

echo "Installing Redis from ref: ${REDIS_REF}"

git clone https://github.com/redis/redis.git 
cd redis
git fetch origin ${REDIS_REF}
git checkout ${REDIS_REF}
git submodule update --init --recursive
make -j$(nproc)
make install
cd /

echo "Redis installed successfully"
redis-server --version
