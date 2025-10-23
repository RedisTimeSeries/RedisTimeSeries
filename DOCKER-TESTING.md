# Docker Testing for RedisTimeSeries

This directory contains scripts to run RedisTimeSeries tests in a Docker Linux environment. This is useful for:

1. **Running OSS Cluster tests** (including ASM tests) on macOS where native support has limitations
2. **Reproducing nightly CI environment locally**
3. **Consistent testing environment** across different platforms

## Prerequisites

- Docker installed and running
- Basic familiarity with command line

## Quick Start

### Run All Tests (Full Nightly CI Suite)

```bash
./run-docker-tests.sh
```

This runs the complete test suite including:
- Unit tests
- General tests
- AOF persistence tests
- Replication tests
- **OSS Cluster tests** (including ASM tests)

### Run Only ASM Tests

To run just the Active Slot Migration tests in OSS Cluster mode:

```bash
./run-docker-asm-tests.sh
```

## What's Included

- `Dockerfile.test` - Docker image definition with all build dependencies
- `docker-test.sh` - Internal script that runs inside the container
- `run-docker-tests.sh` - Wrapper to run full test suite
- `run-docker-asm-tests.sh` - Wrapper to run only ASM tests

## How It Works

1. **Builds** a Ubuntu 22.04-based Docker image with all dependencies
2. **Mounts** your current directory into the container
3. **Builds** Redis unstable and the RedisTimeSeries module inside the container
4. **Runs** the tests with proper Linux environment (needed for cluster mode)

## Why Docker?

### macOS Limitations

On macOS, Redis modules that use newer cluster APIs face symbol resolution issues:
- The module requires symbols like `RedisModule_ClusterCanAccessKeysInSlot`
- These exist in `redis-server` but macOS's dynamic linker (`dlopen`) can't resolve them
- This prevents OSS cluster tests (including ASM tests) from running natively

### Linux Works Perfectly

In Linux containers:
- Dynamic symbol resolution works as expected
- OSS Cluster mode runs without issues
- ASM (Active Slot Migration) tests pass successfully

## Example Output

```bash
$ ./run-docker-asm-tests.sh
🐳 Building Docker test image...
✅ Docker image built successfully

🧪 Running ASM (Active Slot Migration) tests in Docker...
   Environment: OSS Cluster with 2 shards

📦 Installing dependencies...
🔨 Setting up project...
🏗️  Building Redis unstable...
🏗️  Building RedisTimeSeries module...
🧪 Running tests...

test_asm:test_asm_without_data               [PASS]
test_asm:test_asm_with_data                  [PASS]
test_asm:test_asm_with_data_and_queries...   [PASS]

✅ ASM tests completed successfully!
```

## Troubleshooting

### Docker Not Running
```
Cannot connect to the Docker daemon
```
**Solution:** Start Docker Desktop

### Permission Issues
```
permission denied while trying to connect
```
**Solution:** Make sure Docker is running and your user has Docker permissions

### Slow First Run
The first run downloads the Ubuntu image and builds everything, which takes 5-10 minutes. Subsequent runs are much faster as Docker caches the layers.

## CI/CD Integration

The nightly CI (`.github/workflows/event-nightly.yml`) runs the same tests on Linux platforms. This Docker setup allows you to reproduce those results locally.

