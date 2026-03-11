#!/bin/bash
set -e

API_CREDS="test@redis.com:Admin1234!"
CLUSTER_API="https://localhost:9443"
TS_MODULE_UID="3a2abd3a55eece7d95a861243cb67f9c"
CLUSTER_HOST="108.131.94.202"
MEMTIER_HOST="18.201.228.52"
CLI="/opt/redislabs/bin/redis-cli"
RESULTS_DIR="/home/ubuntu/benchmark_results"
mkdir -p "$RESULTS_DIR"

SHARD_COUNTS=(3 4 5 6 7 8 9 10 11 12 13 14 15 16)

create_db() {
    local shards=$1
    local db_name="ts-bench-${shards}shards"
    local memory=$((1024 * 1024 * 1024 * 10))

    echo "$(date): Creating database $db_name with $shards shards..."

    local response
    response=$(curl -s -k -u "$API_CREDS" \
        -H "Content-Type: application/json" \
        -X POST "$CLUSTER_API/v1/bdbs" \
        -d "{
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
        }")

    local db_id
    db_id=$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin).get('uid',''))" 2>/dev/null)
    if [ -z "$db_id" ]; then
        echo "ERROR creating DB: $response"
        return 1
    fi
    echo "DB ID: $db_id"

    for i in $(seq 1 120); do
        local status
        status=$(curl -s -k -u "$API_CREDS" "$CLUSTER_API/v1/bdbs/$db_id" | \
            python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null)
        if [ "$status" = "active" ]; then
            echo "DB is active"
            break
        fi
        sleep 5
    done

    local port
    port=$(curl -s -k -u "$API_CREDS" "$CLUSTER_API/v1/bdbs/$db_id" | \
        python3 -c "import sys,json; d=json.load(sys.stdin); eps=d.get('endpoints',[]); print(eps[0]['port'] if eps else '')" 2>/dev/null)

    echo "DB_ID=$db_id PORT=$port"
    echo "$db_id $port" > /tmp/current_db_info
}

load_data() {
    local port=$1
    echo "$(date): Loading data to port $port..."

    $CLI -h $CLUSTER_HOST -p $port -c TS.CREATE ts:match:key RETENTION 0 \
        LABELS user_id 8938291 amount_type 1 game_provider_id 100 game_id rp_679 \
        metric_id casino-transaction-bet-amount 2>/dev/null

    for ts in $(seq 1765889400000 1000 1765889600000); do
        $CLI -h $CLUSTER_HOST -p $port -c TS.ADD ts:match:key $ts 42.5 > /dev/null 2>&1
    done

    for i in $(seq 1 100); do
        $CLI -h $CLUSTER_HOST -p $port -c TS.CREATE "ts:other:key:$i" RETENTION 0 \
            LABELS user_id $((8938291 + i)) amount_type 1 game_provider_id 100 game_id rp_679 \
            metric_id casino-transaction-bet-amount > /dev/null 2>&1
        $CLI -h $CLUSTER_HOST -p $port -c TS.ADD "ts:other:key:$i" 1765889500000 10.0 > /dev/null 2>&1
    done

    echo "$(date): Data loaded. Verifying..."
    $CLI -h $CLUSTER_HOST -p $port -c TS.MREVRANGE 1765889400000 1765889600000 \
        FILTER_BY_VALUE 1e-8 999999999999999 AGGREGATION sum 5184000000000000000 \
        FILTER user_id=8938291 amount_type=1 "game_provider_id!=(568)" game_id=rp_679 \
        metric_id=casino-transaction-bet-amount GROUPBY user_id REDUCE SUM
}

run_benchmark() {
    local port=$1
    local shards=$2
    local output_file="${RESULTS_DIR}/mrevrange_${shards}shards.txt"

    echo "$(date): Running benchmark for $shards shards on port $port..."

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

    echo "$(date): Benchmark complete for $shards shards"
}

delete_db() {
    local db_id=$1
    echo "$(date): Deleting database $db_id..."
    curl -s -k -u "$API_CREDS" -X DELETE "$CLUSTER_API/v1/bdbs/$db_id" > /dev/null

    for i in $(seq 1 60); do
        local status
        status=$(curl -s -k -u "$API_CREDS" "$CLUSTER_API/v1/bdbs/$db_id" -w "%{http_code}" -o /dev/null 2>/dev/null)
        if [ "$status" = "404" ]; then
            echo "DB deleted"
            break
        fi
        sleep 5
    done
}

echo "============================================"
echo "Starting TS.MREVRANGE benchmark suite"
echo "Date: $(date)"
echo "============================================"

echo ""
echo "2 shards results already collected:"
echo "Ops/sec: 17930.85, Avg Lat: 2.22977ms, p50: 2.175ms, p99: 3.231ms, p99.9: 3.759ms"
echo ""

for shards in "${SHARD_COUNTS[@]}"; do
    echo "============================================"
    echo "Testing $shards shards"
    echo "============================================"

    create_db "$shards"
    db_info=$(cat /tmp/current_db_info)
    db_id=$(echo "$db_info" | awk '{print $1}')
    port=$(echo "$db_info" | awk '{print $2}')

    if [ -z "$port" ] || [ -z "$db_id" ]; then
        echo "ERROR: Could not get DB info, skipping $shards shards"
        continue
    fi

    load_data "$port"
    run_benchmark "$port" "$shards"
    delete_db "$db_id"

    echo "$(date): Completed $shards shards"
    echo ""
done

echo "============================================"
echo "$(date): ALL BENCHMARKS COMPLETE"
echo "============================================"

echo ""
echo "SUMMARY OF RESULTS:"
echo "==================="

echo "Shards | Ops/sec | Avg Lat | p50 | p99 | p99.9"
echo "2 | 17930.85 | 2.22977 | 2.175 | 3.231 | 3.759"

for shards in "${SHARD_COUNTS[@]}"; do
    f="${RESULTS_DIR}/mrevrange_${shards}shards.txt"
    if [ -f "$f" ]; then
        line=$(grep "Ts.mrevranges\|Totals" "$f" | head -1)
        if [ -n "$line" ]; then
            echo "$shards | $line"
        fi
    fi
done
