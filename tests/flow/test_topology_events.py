import time
from includes import *
from utils import (
    dump_infocluster,
    fill_ts_data,
    wait_for_valid_cluster,
    compare_clusters,
    ClusterNode,
    SlotRange,
    NUMBER_OF_SLOTS,
    failover_node,
    add_slaves_to_cluster,
)
from test_asm import validate_queries_during_migrations


def test_asm():
    env = Env(shardsCount=3, decodeResponses=True, skipRefreshCluster=True)
    skip_if_needed(env)

    def post_migration(env):
        wait_for_valid_cluster(env)
        wait_for_valid_ts_infocluster(env)

    fill_some_data(env)
    validate_queries_during_migrations(env, post_migration, COMMAND, validate_result)

def test_failover():
    env = Env(shardsCount=3, decodeResponses=True, skipRefreshCluster=True)
    skip_if_needed(env)


    with add_slaves_to_cluster(env):
        fill_some_data(env)

        replica_port = env.envRunner.shards[0].getMasterPort() + 1
        replica_conn = redis.Redis(port=replica_port, decode_responses=True)
        failover_node(replica_conn)


# Helpers:

NUMBER_OF_KEYS = 1000 if not (VALGRIND or SANITIZER) else 100
SAMPLES_PER_KEY = 150
COMMAND = "TS.MRANGE - + FILTER label1=17 GROUPBY label1 REDUCE count"


def skip_if_needed(env):
    if not env.isCluster():
        env.skip()

    # macos-15-intel is the slowest hosted runner and can't reliably serve the multi-shard
    # query within LibMR's 5s max-idle during migration churn (MOD-14615 residual).
    if RUNNER_LABEL == "macos-15-intel":
        env.skip()

def fill_some_data(env):
    fill_ts_data(env, NUMBER_OF_KEYS, SAMPLES_PER_KEY, label1=17, label2=19)


def validate_result(result):
    ((filtered_by, withlabels, samples),) = result
    assert filtered_by == "label1=17"
    assert withlabels == []  # No WITHLABLES
    assert len(samples) == SAMPLES_PER_KEY
    assert all(int(sample[1]) == NUMBER_OF_KEYS for sample in samples)


def ts_cluster_from_conn(conn):
    """Parse timeseries.INFOCLUSTER as seen by conn into a dict of ClusterNode.id -> ClusterNode,
    or None if its slots don't fully and uniquely cover the keyspace."""
    conn.execute_command("debug", "MARK-INTERNAL-CLIENT")
    reply = conn.execute_command("timeseries.INFOCLUSTER")
    nodes = {}
    total = 0
    min_start = NUMBER_OF_SLOTS
    max_end = -1
    for node in reply[4]:
        fields = {}
        slot_ranges = []
        for key, val in zip(node[::2], node[1::2]):
            if key == "minHslot":
                slot_ranges.append([val])
            elif key == "maxHslot":
                slot_ranges[-1].append(val)
            else:
                fields[key] = val
        slots = {SlotRange(lo, hi) for lo, hi in slot_ranges}
        nodes[fields["id"]] = ClusterNode(
            id=fields["id"],
            ip=fields["ip"],
            port=int(fields["port"]),
            flags={"master"},  # timeseries.INFOCLUSTER only exposes master nodes
            slots=slots,
        )
        for sr in slots:
            total += sr.end - sr.start + 1
            min_start = min(min_start, sr.start)
            max_end = max(max_end, sr.end)

    if min_start != 0 or max_end != NUMBER_OF_SLOTS - 1 or total != NUMBER_OF_SLOTS:
        return None
    return nodes


def wait_for_valid_ts_infocluster(env):
    """Wait until every node's timeseries.INFOCLUSTER reports full coverage and all nodes agree."""
    timeout = 60 if (VALGRIND or SANITIZER) else 5
    deadline = time.time() + timeout
    while True:
        clusters = [ts_cluster_from_conn(env.getConnection(i)) for i in range(env.shardsCount)]
        if all(c is not None for c in clusters) and all(compare_clusters(clusters[0], c) for c in clusters[1:]):
            return
        assert time.time() < deadline, "timeseries.INFOCLUSTER did not reach a valid, agreed state in time"
        time.sleep(0.2)
