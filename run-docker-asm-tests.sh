#!/bin/bash
# Script to run only ASM tests in Docker with OSS Cluster
set -e

IMAGE_NAME="redistimeseries-test:local"

echo "🐳 Building Docker test image..."
docker build -f Dockerfile.test -t "$IMAGE_NAME" . > /dev/null

echo "✅ Docker image built successfully"
echo ""
echo "🧪 Running ASM (Active Slot Migration) tests in Docker..."
echo "   Environment: OSS Cluster with 2 shards"
echo ""

docker run --rm \
  -v "$(pwd):/workspace" \
  -w /workspace \
  "$IMAGE_NAME" \
  bash -c "./docker-test.sh flow_tests GEN=0 SLAVES=0 AOF=0 AOF_SLAVES=0 OSS_CLUSTER=1 SHARDS=2 TEST=test_asm"

echo ""
echo "✅ ASM tests completed successfully!"

