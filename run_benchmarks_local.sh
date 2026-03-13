#!/bin/bash
set -e

SSH_KEY="/Users/tom.gabsow/.ssh/aws"
SSH_OPTS="-o StrictHostKeyChecking=no -i $SSH_KEY"
ENTERPRISE_HOST="108.131.94.202"
MEMTIER_HOST="18.201.228.52"
API_CREDS="test@redis.com:Admin1234!"
CLUSTER_API="https://localhost:9443"
TS_MODULE_UID="3a2abd3a55eece7d95a861243cb67f9c"
CLI="/opt/redislabs/bin/redis-cli"
LOCAL_RESULTS_DIR="/Users/tom.gabsow/Projects/RedisTimeSeries/benchmark_results"
mkdir -p "$LOCAL_RESULTS_DIR"

SHARD_COUNTS=(2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)

remote_enterprise() {
    ssh $SSH_OPTS ubuntu@$ENTERPRISE_HOST "$@"
}

remote_memtier() {
    ssh $SSH_OPTS ubuntu@$MEMTIER_HOST "$@"
}

create_db() {
    local shards=$1
    local db_name="ts-bench-${shards}shards"
    local memory=$((1024 * 1024 * 1024 * 10))

    echo "$(date): Creating database $db_name with $shards shards..."

    local response
    response=$(remote_enterprise "curl -s -k -u '$API_CREDS' \
        -H 'Content-Type: application/json' \
        -X POST '$CLUSTER_API/v1/bdbs' \
        -d '{
            \"name\": \"$db_name\",
            \"memory_size\": $memory,
            \"type\": \"redis\",
            \"sharding\": true,
            \"shards_count\": $shards,
            \"oss_cluster\": true,
            \"oss_cluster_api_preferred_ip_type\": \"external\",
            \"proxy_policy\": \"all-master-shards\",
            \"shards_placement\": \"dense\",
            \"oss_sharding\": true,
            \"module_list\": [{\"module_name\": \"timeseries\", \"module_args\": \"\", \"module_id\": \"$TS_MODULE_UID\"}]
        }'")

    local db_id
    db_id=$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin).get('uid',''))" 2>/dev/null)
    if [ -z "$db_id" ]; then
        echo "ERROR creating DB: $response"
        return 1
    fi
    echo "DB ID: $db_id"

    for i in $(seq 1 120); do
        local status
        status=$(remote_enterprise "curl -s -k -u '$API_CREDS' '$CLUSTER_API/v1/bdbs/$db_id'" | \
            python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null)
        if [ "$status" = "active" ]; then
            echo "DB is active"
            break
        fi
        sleep 5
    done

    local port
    port=$(remote_enterprise "curl -s -k -u '$API_CREDS' '$CLUSTER_API/v1/bdbs/$db_id'" | \
        python3 -c "import sys,json; d=json.load(sys.stdin); eps=d.get('endpoints',[]); print(eps[0]['port'] if eps else '')" 2>/dev/null)

    echo "DB_ID=$db_id PORT=$port"
    DB_ID=$db_id
    DB_PORT=$port
}

load_data() {
    local port=$1
    echo "$(date): Loading data to port $port..."

    remote_enterprise "$CLI -h $ENTERPRISE_HOST -p $port -c TS.CREATE ts:match:key RETENTION 0 \
        LABELS user_id 8938291 amount_type 1 game_provider_id 100 game_id rp_679 \
        metric_id casino-transaction-bet-amount" 2>/dev/null

    echo "Adding 201 data points..."
    remote_enterprise "for ts in \$(seq 1765889400000 1000 1765889600000); do
        $CLI -h $ENTERPRISE_HOST -p $port -c TS.ADD ts:match:key \$ts 42.5 > /dev/null 2>&1
    done"

    echo "Creating 100 non-matching keys..."
    remote_enterprise "for i in \$(seq 1 100); do
        $CLI -h $ENTERPRISE_HOST -p $port -c TS.CREATE \"ts:other:key:\$i\" RETENTION 0 \
            LABELS user_id \$((8938291 + i)) amount_type 1 game_provider_id 100 game_id rp_679 \
            metric_id casino-transaction-bet-amount > /dev/null 2>&1
        $CLI -h $ENTERPRISE_HOST -p $port -c TS.ADD \"ts:other:key:\$i\" 1765889500000 10.0 > /dev/null 2>&1
    done"

    echo "$(date): Data loaded. Verifying..."
    remote_enterprise "$CLI -h $ENTERPRISE_HOST -p $port -c TS.MREVRANGE 1765889400000 1765889600000 \
        FILTER_BY_VALUE 1e-8 999999999999999 AGGREGATION sum 5184000000000000000 \
        FILTER user_id=8938291 amount_type=1 'game_provider_id!=(568)' game_id=rp_679 \
        metric_id=casino-transaction-bet-amount GROUPBY user_id REDUCE SUM"
}

run_benchmark() {
    local port=$1
    local shards=$2
    local output_file="${LOCAL_RESULTS_DIR}/mrevrange_${shards}shards.txt"

    echo "$(date): Running benchmark for $shards shards on port $port (300 seconds)..."

    remote_memtier "/opt/redislabs/bin/memtier_benchmark \
        -s $ENTERPRISE_HOST \
        -p $port \
        --test-time=300 \
        --clients=10 \
        --threads=4 \
        --cluster-mode \
        --command='TS.MREVRANGE 1765889400000 1765889600000 FILTER_BY_VALUE 1e-8 999999999999999 AGGREGATION sum 5184000000000000000 FILTER user_id=8938291 amount_type=1 game_provider_id!=(568) game_id=rp_679 metric_id=casino-transaction-bet-amount GROUPBY user_id REDUCE SUM'" \
        2>&1 | tee "$output_file"

    echo "$(date): Benchmark complete for $shards shards"
}

delete_db() {
    local db_id=$1
    echo "$(date): Deleting database $db_id..."
    remote_enterprise "curl -s -k -u '$API_CREDS' -X DELETE '$CLUSTER_API/v1/bdbs/$db_id'" > /dev/null

    for i in $(seq 1 60); do
        local status
        status=$(remote_enterprise "curl -s -k -u '$API_CREDS' '$CLUSTER_API/v1/bdbs/$db_id' -w '%{http_code}' -o /dev/null 2>/dev/null")
        if [ "$status" = "404" ]; then
            echo "DB deleted"
            break
        fi
        sleep 5
    done
}

echo "============================================"
echo "TS.MREVRANGE Benchmark Suite v99.99"
echo "Date: $(date)"
echo "Enterprise: $ENTERPRISE_HOST"
echo "Memtier from: $MEMTIER_HOST"
echo "============================================"
echo ""

for shards in "${SHARD_COUNTS[@]}"; do
    echo "============================================"
    echo "Testing $shards shards"
    echo "============================================"

    create_db "$shards"

    if [ -z "$DB_PORT" ] || [ -z "$DB_ID" ]; then
        echo "ERROR: Could not get DB info, skipping $shards shards"
        continue
    fi

    load_data "$DB_PORT"
    run_benchmark "$DB_PORT" "$shards"
    delete_db "$DB_ID"

    echo "$(date): Completed $shards shards"
    echo ""
done

echo "============================================"
echo "$(date): ALL BENCHMARKS COMPLETE"
echo "============================================"
echo ""
echo "SUMMARY OF RESULTS:"
echo "==================="
echo "Shards | Ops/sec | Avg Lat (ms) | p50 (ms) | p99 (ms) | p99.9 (ms)"
echo "-------|---------|-------------|----------|----------|----------"

for shards in "${SHARD_COUNTS[@]}"; do
    f="${LOCAL_RESULTS_DIR}/mrevrange_${shards}shards.txt"
    if [ -f "$f" ]; then
        ops=$(grep -E "Ts.mrevranges" "$f" | awk '{print $2}')
        avg=$(grep -E "Ts.mrevranges" "$f" | awk '{print $6}')
        p50=$(grep -E "Ts.mrevranges" "$f" | awk '{print $7}')
        p99=$(grep -E "Ts.mrevranges" "$f" | awk '{print $8}')
        p999=$(grep -E "Ts.mrevranges" "$f" | awk '{print $9}')
        echo "$shards | $ops | $avg | $p50 | $p99 | $p999"
    fi
done
