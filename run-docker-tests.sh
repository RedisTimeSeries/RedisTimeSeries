#!/bin/bash
# Main script to run all RedisTimeSeries tests in Docker (Linux environment)
set -e

IMAGE_NAME="redistimeseries-test:local"

echo "🐳 Building Docker test image..."
docker build -f Dockerfile.test -t "$IMAGE_NAME" . > /dev/null

echo "✅ Docker image built successfully"
echo ""
echo "🧪 Running full test suite in Docker (Linux)..."
echo "   This includes:"
echo "   - Unit tests"
echo "   - General tests"
echo "   - AOF tests"
echo "   - Replication tests" 
echo "   - OSS Cluster tests (including ASM tests)"
echo ""

docker run --rm \
  -v "$(pwd):/workspace" \
  -w /workspace \
  "$IMAGE_NAME" \
  ./docker-test.sh test

echo ""
echo "✅ All tests completed successfully!"

