#!/bin/bash
set -e

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
MODULE="$ROOT/bin/macos-arm64v8-release/redistimeseries.so"
RESULTS_DIR="$ROOT/tests/benchmarks/results"
mkdir -p "$RESULTS_DIR"

NUM_KEYS=100
NUM_SAMPLES=50
MEMTIER_THREADS=2
MEMTIER_CLIENTS=10
MEMTIER_DURATION=15
MEMTIER_PIPELINE=1

REDIS_SERVER="${REDIS_SERVER:-redis-server}"

pkill -f redis-server 2>/dev/null || true
sleep 2
rm -f "$ROOT"/nodes-*.conf "$ROOT"/dump.rdb

populate_single() {
    local port=$1
    echo "  Populating $NUM_KEYS keys with $NUM_SAMPLES samples on port $port..."
    for i in $(seq 0 $((NUM_KEYS-1))); do
        redis-cli -p $port TS.ADD "ts:bench:$i" 1000 $i.0 LABELS type bench grp "g$((i%10))" id "$i" > /dev/null 2>&1
    done
    for s in $(seq 1 $((NUM_SAMPLES-1))); do
        for i in $(seq 0 $((NUM_KEYS-1))); do
            redis-cli -p $port TS.ADD "ts:bench:$i" $((1000+s)) $i.$s > /dev/null 2>&1
        done
    done
    echo "  Done populating."
}

populate_cluster() {
    local port=$1
    echo "  Populating $NUM_KEYS keys with $NUM_SAMPLES samples on cluster (port $port)..."
    for i in $(seq 0 $((NUM_KEYS-1))); do
        redis-cli -c -p $port TS.ADD "ts:bench:$i" 1000 $i.0 LABELS type bench grp "g$((i%10))" id "$i" > /dev/null 2>&1
    done
    for s in $(seq 1 $((NUM_SAMPLES-1))); do
        for i in $(seq 0 $((NUM_KEYS-1))); do
            redis-cli -c -p $port TS.ADD "ts:bench:$i" $((1000+s)) $i.$s > /dev/null 2>&1
        done
    done
    echo "  Done populating."
}

run_memtier() {
    local port=$1
    local label=$2
    local cmd=$3
    echo ""
    echo "=== Benchmarking: $label ==="
    memtier_benchmark \
        -s 127.0.0.1 -p $port \
        --threads=$MEMTIER_THREADS \
        --clients=$MEMTIER_CLIENTS \
        --test-time=$MEMTIER_DURATION \
        --pipeline=$MEMTIER_PIPELINE \
        --command="$cmd" \
        --command-key-pattern=R \
        --hide-histogram \
        --print-percentiles=50,99,99.9 \
        2>&1 | tee "$RESULTS_DIR/${label}.txt"
}

echo ""
echo "============================================"
echo "  PHASE 1: SINGLE SHARD BASELINE"
echo "============================================"

echo "Starting single shard Redis on port 6379... ($REDIS_SERVER)"
$REDIS_SERVER --port 6379 --loadmodule "$MODULE" --save "" --appendonly no --daemonize yes --loglevel warning
sleep 1

populate_single 6379

run_memtier 6379 "single_mrange_all" "TS.MRANGE 1000 1050 FILTER type=bench"
run_memtier 6379 "single_mrange_filtered" "TS.MRANGE 1000 1050 FILTER grp=g0"
run_memtier 6379 "single_mget" "TS.MGET FILTER type=bench"
run_memtier 6379 "single_queryindex" "TS.QUERYINDEX type=bench"

redis-cli -p 6379 SHUTDOWN NOSAVE 2>/dev/null || true
sleep 2
rm -f "$ROOT"/nodes-*.conf "$ROOT"/dump.rdb

echo ""
echo "============================================"
echo "  PHASE 2: 3-SHARD OSS CLUSTER"
echo "============================================"

PORTS=(6379 6380 6381)

for port in "${PORTS[@]}"; do
    echo "Starting shard on port $port..."
    $REDIS_SERVER --port $port \
        --loadmodule "$MODULE" \
        --save "" --appendonly no \
        --daemonize yes --loglevel warning \
        --cluster-enabled yes \
        --cluster-config-file "nodes-$port.conf" \
        --cluster-node-timeout 5000
done
sleep 2

echo "Creating cluster..."
echo yes | redis-cli --cluster create 127.0.0.1:6379 127.0.0.1:6380 127.0.0.1:6381 2>&1 | tail -3
sleep 3

echo "Refreshing timeseries cluster topology..."
for port in "${PORTS[@]}"; do
    redis-cli -p $port timeseries.REFRESHCLUSTER 2>/dev/null || true
done
sleep 2

echo "Forcing shard connections..."
for port in "${PORTS[@]}"; do
    redis-cli -p $port timeseries.FORCESHARDSCONNECTION 2>/dev/null || true
done
sleep 3

populate_cluster 6379

echo ""
echo "--- Starting profiling (sample) on shard 6379 during cluster benchmark ---"
SHARD_PID=$(redis-cli -p 6379 INFO server 2>/dev/null | grep process_id | cut -d: -f2 | tr -d '\r')
echo "Shard 6379 PID: $SHARD_PID"

run_memtier 6379 "cluster_mrange_all" "TS.MRANGE 1000 1050 FILTER type=bench"

# Profile during the most important benchmark
echo "  Starting profiler on PID $SHARD_PID ..."
sample $SHARD_PID 10 -f "$RESULTS_DIR/cluster_mrange_profile.txt" &
SAMPLE_PID=$!

run_memtier 6379 "cluster_mrange_filtered" "TS.MRANGE 1000 1050 FILTER grp=g0"

wait $SAMPLE_PID 2>/dev/null || true
echo "  Profile saved to $RESULTS_DIR/cluster_mrange_profile.txt"

run_memtier 6379 "cluster_mget" "TS.MGET FILTER type=bench"

echo "  Starting profiler on PID $SHARD_PID for MGET..."
sample $SHARD_PID 10 -f "$RESULTS_DIR/cluster_mget_profile.txt" &
SAMPLE_PID=$!

run_memtier 6379 "cluster_mget_profiled" "TS.MGET FILTER type=bench"

wait $SAMPLE_PID 2>/dev/null || true

run_memtier 6379 "cluster_queryindex" "TS.QUERYINDEX type=bench"

echo ""
echo "============================================"
echo "  PHASE 3: RESULTS SUMMARY"
echo "============================================"
echo ""
printf "%-30s %12s %12s %12s\n" "Benchmark" "Ops/sec" "p50 (ms)" "p99 (ms)"
printf "%-30s %12s %12s %12s\n" "------------------------------" "------------" "------------" "------------"

for f in "$RESULTS_DIR"/single_*.txt "$RESULTS_DIR"/cluster_*.txt; do
    [[ "$f" == *profile* ]] && continue
    [[ ! -f "$f" ]] && continue
    label=$(basename "$f" .txt)
    ops=$(grep "Totals" "$f" 2>/dev/null | awk '{print $2}' | head -1)
    p50=$(grep "Totals" "$f" 2>/dev/null | awk '{print $5}' | head -1)
    p99=$(grep "Totals" "$f" 2>/dev/null | awk '{print $6}' | head -1)
    printf "%-30s %12s %12s %12s\n" "$label" "${ops:-N/A}" "${p50:-N/A}" "${p99:-N/A}"
done

echo ""
echo "Profile files:"
ls -la "$RESULTS_DIR"/*profile* 2>/dev/null || echo "  No profile files found"

echo ""
echo "Cleaning up..."
for port in "${PORTS[@]}"; do
    redis-cli -p $port SHUTDOWN NOSAVE 2>/dev/null || true
done
rm -f nodes-*.conf dump.rdb
echo "Done."
