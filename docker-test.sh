#!/bin/bash
# Script to run tests inside Docker container
set -e

echo "🔨 Setting up environment..."
./sbin/system-setup.py

echo ""
echo "🏗️  Building Redis unstable..."
if [ ! -d "redis" ]; then
    git clone --depth 1 --branch unstable https://github.com/redis/redis.git
fi
cd redis
# Clean build to avoid any cached flags
make clean > /dev/null 2>&1 || true
# Build with proper flags for Linux
make -j$(nproc) MALLOC=libc BUILD_TLS=no
cd ..

echo ""
echo "🏗️  Building RedisTimeSeries module..."
make clean || true
make -j$(nproc)

echo ""
echo "🧪 Running tests..."
REDIS_SERVER=$(pwd)/redis/src/redis-server make "$@"

