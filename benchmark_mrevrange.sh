#!/bin/bash
set -e

CLUSTER_HOST="108.131.94.202"
CLUSTER_API="https://localhost:9443"
API_CREDS="test@redis.com:Admin1234!"
TS_MODULE_UID="3a2abd3a55eece7d95a861243cb67f9c"
MEMTIER_HOST="18.201.228.52"
BENCHMARK_HOST="$CLUSTER_HOST"

SHARD_COUNTS=(2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)

RESULTS_DIR="/home/ubuntu/benchmark_results"
mkdir -p "$RESULTS_DIR"

create_db() {
    local shards=$1
    local db_name="ts-bench-${shards}shards"
    local memory=$((1024 * 1024 * 1024 * 10))  # 10GB

    echo "Creating database '$db_name' with $shards shards..."

    local response
    response=$(curl -s -k -u "$API_CREDS" \
        -H "Content-Type: application/json" \
        -X POST "$CLUSTER_API/v1/bdbs" \
        -d '{
            "name": "'"$db_name"'",
            "memory_size": '"$memory"',
            "type": "redis",
            "sharding": true,
            "shards_count": '"$shards"',
            "oss_cluster": true,
            "oss_cluster_api_preferred_ip_type": "external",
            "proxy_policy": "all-master-shards",
            "shards_placement": "dense",
            "module_list": [{"module_name": "timeseries", "module_args": "", "module_id": "'"$TS_MODULE_UID"'"}]
        }')

    echo "API response: $response"

    local db_id
    db_id=$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin).get('uid',''))" 2>/dev/null)
    if [ -z "$db_id" ]; then
        echo "ERROR: Failed to create database"
        return 1
    fi
    echo "Database created with ID: $db_id"

    echo "Waiting for database to be active..."
    for i in $(seq 1 60); do
        local status
        status=$(curl -s -k -u "$API_CREDS" "$CLUSTER_API/v1/bdbs/$db_id" | \
            python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null)
        if [ "$status" = "active" ]; then
            echo "Database is active."
            break
        fi
        echo "  Status: $status (attempt $i/60)"
        sleep 5
    done

    local port
    port=$(curl -s -k -u "$API_CREDS" "$CLUSTER_API/v1/bdbs/$db_id" | \
        python3 -c "import sys,json; d=json.load(sys.stdin); eps=d.get('endpoints',[]); print(eps[0]['port'] if eps else d.get('port',''))" 2>/dev/null)

    echo "DB_ID=$db_id PORT=$port"
    echo "$db_id $port" > /tmp/current_db_info
}

load_data() {
    local port=$1
    local host="$CLUSTER_HOST"

    echo "Loading test data to port $port..."

    redis-cli -h "$host" -p "$port" -c TS.CREATE ts:match:key RETENTION 0 \
        LABELS user_id 8938291 amount_type 1 game_provider_id 100 game_id rp_679 \
        metric_id casino-transaction-bet-amount 2>/dev/null || true

    echo "Adding data points to matching key..."
    local pipeline=""
    for ts in $(seq 1765889400000 1000 1765889600000); do
        pipeline="${pipeline}TS.ADD ts:match:key $ts 42.5\n"
    done
    echo -e "$pipeline" | redis-cli -h "$host" -p "$port" -c --pipe 2>/dev/null

    echo "Creating non-matching keys for realistic filter workload..."
    for i in $(seq 1 100); do
        redis-cli -h "$host" -p "$port" -c TS.CREATE "ts:other:key:$i" RETENTION 0 \
            LABELS user_id $((8938291 + i)) amount_type 1 game_provider_id 100 game_id rp_679 \
            metric_id casino-transaction-bet-amount 2>/dev/null || true
    done

    echo "Verifying data with TS.MREVRANGE..."
    redis-cli -h "$host" -p "$port" -c TS.MREVRANGE 1765889400000 1765889600000 \
        FILTER_BY_VALUE 1e-8 999999999999999 \
        AGGREGATION sum 5184000000000000000 \
        FILTER user_id=8938291 amount_type=1 "game_provider_id!=(568)" game_id=rp_679 \
        metric_id=casino-transaction-bet-amount \
        GROUPBY user_id REDUCE SUM

    echo "Data loading complete."
}

run_benchmark() {
    local port=$1
    local shards=$2
    local output_file="${RESULTS_DIR}/mrevrange_${shards}shards.txt"

    echo "Running benchmark for $shards shards on port $port for 300 seconds..."
    echo "Output: $output_file"

    ssh -o StrictHostKeyChecking=no ubuntu@"$MEMTIER_HOST" \
        "/opt/redislabs/bin/memtier_benchmark \
        -s $CLUSTER_HOST \
        -p $port \
        --test-time=300 \
        --clients=10 \
        --threads=4 \
        --cluster-mode \
        --command='TS.MREVRANGE 1765889400000 1765889600000 FILTER_BY_VALUE 1e-8 999999999999999 AGGREGATION sum 5184000000000000000 FILTER user_id=8938291 amount_type=1 game_provider_id!=(568) game_id=rp_679 metric_id=casino-transaction-bet-amount GROUPBY user_id REDUCE SUM'" \
        2>&1 | tee "$output_file"

    echo "Benchmark complete for $shards shards."
}

delete_db() {
    local db_id=$1
    echo "Deleting database $db_id..."
    curl -s -k -u "$API_CREDS" -X DELETE "$CLUSTER_API/v1/bdbs/$db_id"
    sleep 10
    echo "Database deleted."
}

run_single_test() {
    local shards=$1
    echo "============================================"
    echo "Testing with $shards shards"
    echo "============================================"

    create_db "$shards"
    local db_info
    db_info=$(cat /tmp/current_db_info)
    local db_id=$(echo "$db_info" | awk '{print $1}')
    local port=$(echo "$db_info" | awk '{print $2}')

    if [ -z "$port" ] || [ -z "$db_id" ]; then
        echo "ERROR: Could not get DB info"
        return 1
    fi

    load_data "$port"
    run_benchmark "$port" "$shards"
    delete_db "$db_id"

    echo "Test for $shards shards completed."
    echo ""
}

echo "Starting TS.MREVRANGE benchmark suite"
echo "Date: $(date)"
echo "Cluster: $CLUSTER_HOST"
echo "Benchmark host: $MEMTIER_HOST"
echo ""

for shards in "${SHARD_COUNTS[@]}"; do
    run_single_test "$shards"
done

echo "============================================"
echo "All tests completed!"
echo "Results are in $RESULTS_DIR"
echo "============================================"
