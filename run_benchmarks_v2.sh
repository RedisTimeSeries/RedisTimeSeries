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
LOCAL_RESULTS_DIR="/Users/tom.gabsow/Projects/RedisTimeSeries/benchmark_results_v2"
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

    DB_ID=$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin).get('uid',''))" 2>/dev/null)
    if [ -z "$DB_ID" ]; then
        echo "ERROR creating DB: $response"
        return 1
    fi
    echo "DB ID: $DB_ID"

    for i in $(seq 1 120); do
        local status
        status=$(remote_enterprise "curl -s -k -u '$API_CREDS' '$CLUSTER_API/v1/bdbs/$DB_ID'" | \
            python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null)
        if [ "$status" = "active" ]; then
            echo "DB is active"
            break
        fi
        sleep 5
    done

    DB_PORT=$(remote_enterprise "curl -s -k -u '$API_CREDS' '$CLUSTER_API/v1/bdbs/$DB_ID'" | \
        python3 -c "import sys,json; d=json.load(sys.stdin); eps=d.get('endpoints',[]); print(eps[0]['port'] if eps else '')" 2>/dev/null)

    echo "DB_ID=$DB_ID PORT=$DB_PORT"
}

load_data() {
    local port=$1
    echo "$(date): Loading data to port $port..."

    remote_enterprise "$CLI -h $ENTERPRISE_HOST -p $port -c TS.CREATE ts:match:key RETENTION 0 \
        LABELS user_id 8938291 amount_type 1 game_provider_id 100 game_id rp_679 \
        metric_id casino-transaction-bet-amount" 2>/dev/null

    remote_enterprise "for ts in \$(seq 1765889400000 1000 1765889600000); do
        $CLI -h $ENTERPRISE_HOST -p $port -c TS.ADD ts:match:key \$ts 42.5 > /dev/null 2>&1
    done"

    remote_enterprise "for i in \$(seq 1 100); do
        $CLI -h $ENTERPRISE_HOST -p $port -c TS.CREATE \"ts:other:key:\$i\" RETENTION 0 \
            LABELS user_id \$((8938291 + i)) amount_type 1 game_provider_id 100 game_id rp_679 \
            metric_id casino-transaction-bet-amount > /dev/null 2>&1
        $CLI -h $ENTERPRISE_HOST -p $port -c TS.ADD \"ts:other:key:\$i\" 1765889500000 10.0 > /dev/null 2>&1
    done"

    echo "$(date): Data loaded."
}

collect_metrics() {
    local db_id=$1
    local shards=$2
    local metrics_file="${LOCAL_RESULTS_DIR}/metrics_${shards}shards.json"

    remote_enterprise "curl -s -k -u '$API_CREDS' '$CLUSTER_API/v1/bdbs/$db_id/stats/last?interval=5min'" > "${metrics_file}.bdb" 2>/dev/null

    remote_enterprise "curl -s -k -u '$API_CREDS' '$CLUSTER_API/v1/bdbs/$db_id'" > "${metrics_file}.config" 2>/dev/null

    local shard_list
    shard_list=$(remote_enterprise "curl -s -k -u '$API_CREDS' '$CLUSTER_API/v1/bdbs/$db_id'" | \
        python3 -c "import sys,json; d=json.load(sys.stdin); print(' '.join(str(s) for s in d.get('shard_list',[])))" 2>/dev/null)

    echo "$shard_list" > "${metrics_file}.shardlist"

    for shard_id in $shard_list; do
        remote_enterprise "curl -s -k -u '$API_CREDS' '$CLUSTER_API/v1/shards/$shard_id/stats/last?interval=5min'" > "${metrics_file}.shard_${shard_id}" 2>/dev/null
    done

    remote_enterprise "curl -s -k -u '$API_CREDS' '$CLUSTER_API/v1/bdbs/$db_id/stats/last?interval=5min'" | \
        python3 -c "
import sys,json
try:
    d=json.load(sys.stdin)
    for interval in d.get('intervals',[d] if isinstance(d,dict) else []):
        lat = interval.get('avg_latency', interval.get('avg_read_latency', 'N/A'))
        ops = interval.get('total_req', interval.get('instantaneous_ops_per_sec', 'N/A'))
        read_ops = interval.get('read_req', 'N/A')
        print(f'bdb_avg_latency: {lat}')
        print(f'total_requests: {ops}')
        print(f'read_requests: {read_ops}')
except:
    print('Error parsing metrics')
" 2>/dev/null > "${metrics_file}.summary"

    echo "Metrics collected for $shards shards"
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
        2>&1 | tee "$output_file" &
    BENCH_PID=$!

    sleep 60
    echo "$(date): Collecting mid-run metrics..."
    collect_metrics "$DB_ID" "$shards"

    sleep 60
    echo "$(date): Collecting second metrics sample..."
    collect_metrics "$DB_ID" "${shards}_mid"

    wait $BENCH_PID
    echo "$(date): Benchmark complete for $shards shards"

    echo "$(date): Collecting final metrics..."
    collect_metrics "$DB_ID" "${shards}_final"
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
echo "TS.MREVRANGE Benchmark Suite v99.99 (with metrics)"
echo "Date: $(date)"
echo "Enterprise: $ENTERPRISE_HOST"
echo "Memtier from: $MEMTIER_HOST"
echo "============================================"
echo ""

CSV_FILE="${LOCAL_RESULTS_DIR}/results_99.99.csv"
echo "db_id,run_time [hh:mm:ss],shards [count],command,workers [count],desired_clients [count],actual_clients [count],target_ops [ops/sec],Total,bdb_avg_latency,bdb_avg_read_latency,total requests,memtier_ops_sec,memtier_avg_lat,memtier_p50,memtier_p99,memtier_p999" > "$CSV_FILE"

for shards in "${SHARD_COUNTS[@]}"; do
    echo "============================================"
    echo "Testing $shards shards"
    echo "============================================"

    START_TIME=$(date +"%H:%M:%S")

    create_db "$shards"

    if [ -z "$DB_PORT" ] || [ -z "$DB_ID" ]; then
        echo "ERROR: Could not get DB info, skipping $shards shards"
        continue
    fi

    load_data "$DB_PORT"
    run_benchmark "$DB_PORT" "$shards"

    END_TIME=$(date +"%H:%M:%S")

    memtier_file="${LOCAL_RESULTS_DIR}/mrevrange_${shards}shards.txt"
    if [ -f "$memtier_file" ]; then
        memtier_ops=$(grep "Ts.mrevranges" "$memtier_file" | awk '{print $2}')
        memtier_avg=$(grep "Ts.mrevranges" "$memtier_file" | awk '{print $5}')
        memtier_p50=$(grep "Ts.mrevranges" "$memtier_file" | awk '{print $6}')
        memtier_p99=$(grep "Ts.mrevranges" "$memtier_file" | awk '{print $7}')
        memtier_p999=$(grep "Ts.mrevranges" "$memtier_file" | awk '{print $8}')
    fi

    metrics_summary="${LOCAL_RESULTS_DIR}/metrics_${shards}shards.json.summary"
    bdb_lat="N/A"
    bdb_ops="N/A"
    if [ -f "$metrics_summary" ]; then
        bdb_lat=$(grep "bdb_avg_latency" "$metrics_summary" | head -1 | awk '{print $2}')
        bdb_ops=$(grep "total_requests" "$metrics_summary" | head -1 | awk '{print $2}')
    fi

    echo "DB${DB_ID},${START_TIME} - ${END_TIME},${shards},sum,default,40,40,max,40,${bdb_lat},${bdb_lat},${bdb_ops},${memtier_ops},${memtier_avg},${memtier_p50},${memtier_p99},${memtier_p999}" >> "$CSV_FILE"

    delete_db "$DB_ID"

    echo "$(date): Completed $shards shards"
    echo ""
done

echo "============================================"
echo "$(date): ALL BENCHMARKS COMPLETE"
echo "============================================"
echo ""
echo "CSV results saved to: $CSV_FILE"
cat "$CSV_FILE"
