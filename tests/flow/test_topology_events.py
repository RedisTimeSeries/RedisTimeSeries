import time
import random
import functools
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
    dump_node_cluster_nodes,
    dump_node_infocluster,
    run_until_first_finishes,
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

    # Connections to the original nodes only, snapshotted before any node is added: we don't risk
    # racing with the added node so choose from OGs. env.getConnection carries the env's TLS config.
    original_conns = [env.getConnection(i) for i in range(env.shardsCount)]
    fill_some_data(env)

    def tolerable_data_validation():
        conn = random.choice(original_conns)
        try:
            result = conn.execute_command(COMMAND)
        except redis.exceptions.ResponseError as x:
            assert str(x) == TOPOLOGY_CHANGED_ERROR, str(x)
            return
        validate_result(result)

    def validate_data_in_a_loop(done):
        while not done.is_set():
            tolerable_data_validation()

    def add_remove_cycles(done):
        cycles = 10
        for _ in range(cycles):
            if done.is_set():
                return
            # Add a new node
            env.addShardToClusterIfExists()
            env.shardsCount = env.envRunner.shardsCount

            after_addition = wait_for_valid_cluster(env)
            validate_after_addition(after_addition, wait_for_valid_ts_infocluster(env), before)

            # Remove it, restoring the initial cluster
            (new_node_id,) = set(after_addition) - set(before)
            remove_slotless_node(env, new_node_id)

            validate_after_removal(wait_for_valid_cluster(env), wait_for_valid_ts_infocluster(env), before)

    run_until_first_finishes(validate_data_in_a_loop, add_remove_cycles)


def test_take_node_down_and_up():
    env = Env(shardsCount=3, decodeResponses=True, skipRefreshCluster=True)
    skip_if_needed(env)

    wait_for_valid_cluster(env)
    wait_for_valid_ts_infocluster(env)

    # An untouchable node: it is never stopped and we use it for the data validations
    untouchable = env.envRunner.shards[0]
    fill_some_data(env)

    def tolerable_data_validation():
        try:
            result = untouchable.getConnection().execute_command(COMMAND)
        except redis.exceptions.ResponseError as x:
            assert str(x) in (TOPOLOGY_CHANGED_ERROR, SLOT_RANGES_ERROR, CLUSTERDOWN_ERROR, SHARD_TIMEOUT_ERROR), str(x)
            return
        validate_result(result)

    def validate_data_in_a_loop(done):
        while not done.is_set():
            tolerable_data_validation()

    def node_down_up_cycles(done):
        cycles = 10
        round_robin_on = [shard for shard in env.envRunner.shards if shard is not untouchable]
        for cycle in range(cycles):
            if done.is_set():
                return
            victim = round_robin_on[cycle % len(round_robin_on)]
            time.sleep(1)  # let the node some time to work on subtasks
            victim.stopEnv()
            wait_for_down_state_view_agreement(env, victim)
            victim.startEnv()
            wait_for_valid_cluster(env)
            wait_for_valid_ts_infocluster(env)

    run_until_first_finishes(validate_data_in_a_loop, node_down_up_cycles)


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
    if env.useTLS:  # The added slaves do support TLS (for now; will be resolved by MOD-17386)
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

# Some expected errors:

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
# While a peer master is down its slots go unreachable, so the fan-out can time out waiting for that shard:
# This error is currently swallowed when a node comes down and up again, but should be removed as part of MOD-17548.
SHARD_TIMEOUT_ERROR = "A multi-keys command failed because at least one shard did not reply within the given timeframe."


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


def random_master_node(env):
    masters = [
        node
        for node in map(ClusterNode.from_str, env.getConnection(0).execute_command("CLUSTER", "NODES").splitlines())
        if "master" in node.flags
    ]
    return random.choice(masters)


def validate_queries_during_failovers(env, post_failover, command, validate_result):
    @functools.cache
    def connection_of(ip, port):
        return redis.Redis(host=ip, port=port, decode_responses=True)

    def strict_validation(env):
        node = random_master_node(env)
        validate_result(connection_of(node.ip, node.port).execute_command(command))

    def tolerable_validation(env):
        node = random_master_node(env)
        try:
            result = connection_of(node.ip, node.port).execute_command(command)
        except redis.exceptions.ResponseError as x:
            assert str(x) in (TOPOLOGY_CHANGED_ERROR, UNBLOCKED_ERROR, CLUSTERDOWN_ERROR, SLOT_RANGES_ERROR), str(x)
            return
        validate_result(result)

    def validate_after_failover(env):
        post_failover(env)
        strict_validation(env)

    strict_validation(env)

    def validate_command_in_a_loop(done):
        while not done.is_set():
            tolerable_validation(env)

    def failover_back_and_forth(done):
        # Just one round-robin and back, unfortunately. We can't loop this since redis rate-limits
        # failover votes to once per 2*cluster-node-timeout per demoted primary, so a second cycle
        # would have to wait out that cooldown period before it could failover the same nodes again.
        # This will make the test too long.
        failover_all_slaves(env, validate_after_failover)
        failover_all_slaves(env, validate_after_failover)

    run_until_first_finishes(validate_command_in_a_loop, failover_back_and_forth)

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


def redis_master_addrs(conn):
    """{node_id: (ip, port)} for master nodes as seen in CLUSTER NODES (a fail-flagged master still counts)."""
    return {
        node.id: (node.ip, node.port)
        for node in map(ClusterNode.from_str, conn.execute_command("CLUSTER", "NODES").splitlines())
        if "master" in node.flags
    }


def ts_master_addrs(conn):
    """{node_id: (ip, port)} for master nodes in timeseries.INFOCLUSTER, ignoring slots (unlike
    ts_cluster_from_conn there is no full-coverage gate, so this works while a master is down)."""
    conn.execute_command("debug", "MARK-INTERNAL-CLIENT")
    reply = conn.execute_command("timeseries.INFOCLUSTER")
    if reply == "no cluster mode":
        return None
    addrs = {}
    for node in reply[4]:
        fields = {key: val for key, val in zip(node[::2], node[1::2]) if key not in ("minHslot", "maxHslot")}
        addrs[fields["id"]] = (fields["ip"], int(fields["port"]))
    return addrs


def wait_for_down_state_view_agreement(env, victim):
    # While the victim shard is down, wait until the surviving nodes' redis and timeseries views agree
    # on the same set of nodes (ignoring slots, which are in flux while a node is down).
    up_shards = [shard for shard in env.envRunner.shards if shard is not victim]
    deadline = time.time() + get_timeout()
    while True:
        conns = [shard.getConnection() for shard in up_shards]
        redis_views = [redis_master_addrs(c) for c in conns]
        ts_views = [ts_master_addrs(c) for c in conns]
        if (
            all(v == redis_views[0] for v in redis_views[1:])
            and all(v == ts_views[0] for v in ts_views[1:])
            and redis_views[0] == ts_views[0]
        ):
            return
        if time.time() >= deadline:
            for conn in conns:
                dump_node_cluster_nodes(conn)
                dump_node_infocluster(conn)
            assert False, "redis and timeseries master views did not agree while a node was down"
        time.sleep(0.2)


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
    # Wait until every node's timeseries.INFOCLUSTER reports full coverage and all nodes agree.
    # Returns the agreed topology as a {node_id: ClusterNode} dict (the first polled node's view).
    timeout = get_timeout()
    deadline = time.time() + timeout
    while True:
        try:
            clusters = [ts_cluster_from_conn(env.getConnection(i)) for i in range(env.shardsCount)]
        except redis.exceptions.ClusterDownError:
            # A just-rejoined master's cluster state is transiently 'fail', so INFOCLUSTER is rejected;
            # keep polling until it turns healthy.
            clusters = None
        if clusters is not None and all(clusters) and all(compare_clusters(clusters[0], c) for c in clusters[1:]):
            return clusters[0]
        assert time.time() < deadline, "timeseries.INFOCLUSTER did not reach a valid, agreed state in time"
        time.sleep(0.2)
