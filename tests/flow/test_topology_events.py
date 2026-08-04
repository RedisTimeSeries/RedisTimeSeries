import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from includes import *
from utils import (
    fill_ts_data,
    wait_for_valid_cluster,
    compare_clusters,
    ClusterNode,
    SlotRange,
    NUMBER_OF_SLOTS,
    failover_node,
    added_slaves_to_cluster,
    get_timeout,
    remove_slotless_node,
)
from test_asm import validate_queries_during_migrations


def test_add_and_remove_node():
    env = Env(shardsCount=2, decodeResponses=True, skipRefreshCluster=True)
    skip_if_needed(env)

    def node_summary(view):
        # {node_id: (ip, port, slots)} for a {node_id: ClusterNode} view - the node identity we compare.
        return {node_id: (node.ip, node.port, node.slots) for node_id, node in view.items()}

    def validate_views_agree(cluster, infocluster):
        # The redis (CLUSTER NODES) and timeseries (INFOCLUSTER) views must expose the same
        # nodes at the same addresses owning the same slots.
        assert node_summary(cluster) == node_summary(infocluster)

    def validate_before_addition(cluster, infocluster):
        validate_views_agree(cluster, infocluster)
        assert len(cluster) == 2  # the two masters the env starts with

    def validate_after_addition(cluster, infocluster, before):
        validate_views_agree(cluster, infocluster)
        # Exactly one node joined; its address is irrelevant, we only require that it owns no slots.
        (new_node_id,) = set(cluster) - set(before)
        assert cluster[new_node_id].slots == set()
        # The original masters are untouched (address + slots).
        assert node_summary({id: node for id, node in cluster.items() if id != new_node_id}) == node_summary(before)

    def validate_after_removal(cluster, infocluster, before):
        validate_views_agree(cluster, infocluster)
        # The topology is restored to exactly what it was before the addition (address + slots).
        assert node_summary(cluster) == node_summary(before)

    before = wait_for_valid_cluster(env)
    validate_before_addition(before, wait_for_valid_ts_infocluster(env))

    # Add a new node
    env.addShardToClusterIfExists()
    env.shardsCount = env.envRunner.shardsCount

    after_addition = wait_for_valid_cluster(env)
    validate_after_addition(after_addition, wait_for_valid_ts_infocluster(env), before)

    (new_node_id,) = set(after_addition) - set(before)
    remove_slotless_node(env, new_node_id)

    validate_after_removal(wait_for_valid_cluster(env), wait_for_valid_ts_infocluster(env), before)


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
    if env.useTLS:
        env.skip()

    def post_failover(env):
        wait_for_valid_cluster(env)
        wait_for_valid_ts_infocluster(env)

    with added_slaves_to_cluster(env):
        fill_some_data(env)
        validate_queries_during_failovers(env, post_failover, COMMAND, validate_result)


def test_clusterset_after_topology_event_keeps_current_topology():
    env = Env(shardsCount=3, decodeResponses=True, skipRefreshCluster=True)
    skip_if_needed(env)

    wait_for_valid_cluster(env)
    wait_for_valid_ts_infocluster(env)

    conn = env.getConnection(0)
    expected = ts_cluster_from_conn(conn)

    # CLUSTERSET is obsolete after topology events take over, so it should be
    # ignored without freeing the topology that was installed by the event.
    assert conn.execute_command("timeseries.CLUSTERSET") == "OK"
    actual = ts_cluster_from_conn(conn)
    assert actual is not None, "CLUSTERSET discarded the topology installed by the event"
    assert compare_clusters(expected, actual)


# Helpers:

NUMBER_OF_KEYS = 1000 if not (VALGRIND or SANITIZER) else 100
SAMPLES_PER_KEY = 150
COMMAND = "TS.MRANGE - + FILTER label1=17 GROUPBY label1 REDUCE count"


def skip_if_needed(env):
    if env.env != "oss-cluster":
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


def validate_queries_during_failovers(env, post_failover, command, validate_result):
    TOPOLOGY_CHANGED_ERROR = "A multi-shard command failed because the cluster topology has changed"
    # Clients of multi-shard commands are blocked. If a node that serves such a command is demoted
    # while the client is still blocked we expect the following error:
    UNBLOCKED_ERROR = "UNBLOCKED force unblock from blocking operation, instance state changed (master -> replica?)"
    # During a failover there is a brief period when the cluster state is set to fail
    # with cluster-allow-reads-when-down off it then rejects reads until it recovers (sub-second)
    # during which time we expect the following error:
    CLUSTERDOWN_ERROR = "CLUSTERDOWN The cluster is down"
    # A multi-shard fan-out can race slot-ownership propagation during a failover (the owning node's
    # id changes) and momentarily see a slot as unavailable, so we also expect this:
    SLOT_RANGES_ERROR = "Query requires unavailable slots"

    master_conns = {}

    def random_master_conn():
        masters = [
            node
            for node in map(ClusterNode.from_str, env.getConnection(0).execute_command("CLUSTER", "NODES").splitlines())
            if "master" in node.flags
        ]
        node = random.choice(masters)
        return master_conns.setdefault(
            (node.ip, node.port), redis.Redis(host=node.ip, port=node.port, decode_responses=True)
        )

    def strict_validation(env):
        validate_result(random_master_conn().execute_command(command))

    def tolerable_validation(env):
        try:
            result = random_master_conn().execute_command(command)
        except redis.exceptions.ResponseError as x:
            assert str(x) in (TOPOLOGY_CHANGED_ERROR, UNBLOCKED_ERROR, CLUSTERDOWN_ERROR, SLOT_RANGES_ERROR), str(x)
            return
        validate_result(result)

    def validate_after_failover(env):
        post_failover(env)
        strict_validation(env)

    strict_validation(env)

    done = threading.Event()

    def validate_command_in_a_loop():
        while not done.is_set():
            tolerable_validation(env)

    def failover_back_and_forth():
        failover_all_slaves(env, validate_after_failover)
        failover_all_slaves(env, validate_after_failover)

    with ThreadPoolExecutor() as executor:
        # Just one round-robin and back, unfortunately. We can't loop this since redis rate-limits
        # failover votes to once per 2*cluster-node-timeout per demoted primary, so a second cycle
        # would have to wait out that cooldown period before it could failover the same nodes again.
        # This will make the test too long.
        futures = map(executor.submit, [validate_command_in_a_loop, failover_back_and_forth])
        for future in as_completed(futures):
            done.set()
            future.result()

    strict_validation(env)


def failover_all_slaves(env, validator=None):
    nodes = {
        node.id: node
        for node in map(ClusterNode.from_str, env.getConnection(0).execute_command("CLUSTER", "NODES").splitlines())
    }
    for replica in [node for node in nodes.values() if "slave" in node.flags]:
        master = nodes[replica.master]
        failover_node(redis.Redis(host=replica.ip, port=replica.port, decode_responses=True))
        if validator is not None:
            print(f"\n----- master {master.ip}:{master.port} failed over to replica {replica.ip}:{replica.port} -----")
            validator(env)


def ts_cluster_from_conn(conn):
    """Parse timeseries.INFOCLUSTER as seen by conn into a dict of ClusterNode.id -> ClusterNode,
    or None if its slots don't fully and uniquely cover the keyspace."""
    conn.execute_command("debug", "MARK-INTERNAL-CLIENT")
    reply = conn.execute_command("timeseries.INFOCLUSTER")
    if reply == "no cluster mode":
        return None
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
    """Wait until every node's timeseries.INFOCLUSTER reports full coverage and all nodes agree.

    Returns the agreed topology as a {node_id: ClusterNode} dict (the first polled node's view).
    """
    timeout = get_timeout()
    deadline = time.time() + timeout
    while True:
        clusters = [ts_cluster_from_conn(env.getConnection(i)) for i in range(env.shardsCount)]
        if all(c is not None for c in clusters) and all(compare_clusters(clusters[0], c) for c in clusters[1:]):
            return clusters[0]
        assert time.time() < deadline, "timeseries.INFOCLUSTER did not reach a valid, agreed state in time"
        time.sleep(0.2)
