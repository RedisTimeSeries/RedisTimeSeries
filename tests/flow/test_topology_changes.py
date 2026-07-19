import time
import threading
import redis
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import Env, Refresh_Cluster
from includes import *
from test_asm import (
    MIGRATION_CYCLES,
    SlotRange,
    ClusterNode,
    fill_some_data,
    migrate_slots_back_and_forth,
    import_slots,
)


# MOD-16382: a subsequent long-form timeseries.CLUSTERSET that moves a peer to a
# new ip / port must update the stored topology. Driven on a non-cluster shard
# (the enterprise CLUSTERSET path); INFOCLUSTER exposes per-node id/ip/port and
# asserts exactly one slot-range per node.


def _infocluster_nodes(conn):
    res = conn.execute_command('timeseries.INFOCLUSTER')
    # ['MyId', <id>, 'MyRunId', <runid>, [ <node flat-array>, ... ]]
    return [{a[i]: a[i + 1] for i in range(0, len(a), 2)} for a in res[4]]


def _peer(nodes):
    # the shard owning the upper half (SLOTRANGE 8192..16383)
    return [n for n in nodes if int(n['minHslot']) == 8192][0]


def _args(my_port, peer_ip, peer_port):
    return [
        'HASHFUNC', 'CRC16', 'NUMSLOTS', '16384', 'MYID', '1', 'RANGES', '2',
        'SHARD', '1', 'SLOTRANGE', '0', '8191',
        'ADDR', '@127.0.0.1:%d' % my_port, 'MASTER',
        'SHARD', '2', 'SLOTRANGE', '8192', '16383',
        'ADDR', '@%s:%d' % (peer_ip, peer_port), 'MASTER',
    ]


def _setup(env):
    conn = env.getConnection()
    try:
        conn.execute_command('debug', 'MARK-INTERNAL-CLIENT')
    except Exception:
        pass  # older redis: CLUSTERSET is not an internal command there
    return conn, conn.connection_pool.connection_kwargs['port']


def _clusterset_args(shards, numslots=16384, myid='1'):
    """Build a long-form timeseries.CLUSTERSET from a list of (slot_start, slot_end, 'ip:port').
    Shard 1 is 'me'. INFOCLUSTER requires each node to own exactly one contiguous range."""
    args = ['HASHFUNC', 'CRC16', 'NUMSLOTS', str(numslots), 'MYID', myid,
            'RANGES', str(len(shards))]
    for i, (start, end, addr) in enumerate(shards, start=1):
        args += ['SHARD', str(i), 'SLOTRANGE', str(start), str(end),
                 'ADDR', '@%s' % addr, 'MASTER']
    return args


def _node_by_min(nodes, min_hslot):
    return [n for n in nodes if int(n['minHslot']) == min_hslot][0]


def test_clusterset_port_change_applied():
    """A CLUSTERSET that moves the peer to a new port updates the stored topology."""
    env = Env(decodeResponses=True)
    if env.is_cluster():
        env.skip()
    env.skipOnSlave()
    conn, my_port = _setup(env)

    env.assertEqual(conn.execute_command('timeseries.CLUSTERSET',
                    *_args(my_port, '127.0.0.1', my_port + 1000)), 'OK')
    env.assertEqual(int(_peer(_infocluster_nodes(conn))['port']), my_port + 1000)

    env.assertEqual(conn.execute_command('timeseries.CLUSTERSET',
                    *_args(my_port, '127.0.0.1', my_port + 2000)), 'OK')
    env.assertEqual(int(_peer(_infocluster_nodes(conn))['port']), my_port + 2000)


def test_clusterset_ip_change_applied():
    """A CLUSTERSET that moves the peer to a new ip updates the stored topology."""
    env = Env(decodeResponses=True)
    if env.is_cluster():
        env.skip()
    env.skipOnSlave()
    conn, my_port = _setup(env)

    env.assertEqual(conn.execute_command('timeseries.CLUSTERSET',
                    *_args(my_port, '127.0.0.1', my_port + 1000)), 'OK')
    env.assertEqual(_peer(_infocluster_nodes(conn))['ip'], '127.0.0.1')

    env.assertEqual(conn.execute_command('timeseries.CLUSTERSET',
                    *_args(my_port, '127.0.0.2', my_port + 1000)), 'OK')
    env.assertEqual(_peer(_infocluster_nodes(conn))['ip'], '127.0.0.2')


def test_clusterset_slot_range_change_applied():
    """Moving a slot boundary via CLUSTERSET updates the stored slot ranges (SLOT change)."""
    env = Env(decodeResponses=True)
    if env.is_cluster():
        env.skip()
    env.skipOnSlave()
    conn, my_port = _setup(env)
    peer = '127.0.0.1:%d' % (my_port + 1000)

    env.assertEqual(conn.execute_command('timeseries.CLUSTERSET', *_clusterset_args([
        (0, 8191, '127.0.0.1:%d' % my_port), (8192, 16383, peer)])), 'OK')
    nodes = _infocluster_nodes(conn)
    env.assertEqual(sorted(int(n['minHslot']) for n in nodes), [0, 8192])

    # Slide the boundary from 8192 to 10000.
    env.assertEqual(conn.execute_command('timeseries.CLUSTERSET', *_clusterset_args([
        (0, 9999, '127.0.0.1:%d' % my_port), (10000, 16383, peer)])), 'OK')
    nodes = _infocluster_nodes(conn)
    env.assertEqual(sorted(int(n['minHslot']) for n in nodes), [0, 10000])
    env.assertEqual(int(_node_by_min(nodes, 0)['maxHslot']), 9999)
    env.assertEqual(int(_node_by_min(nodes, 10000)['maxHslot']), 16383)


def test_clusterset_node_added_applied():
    """A CLUSTERSET that grows 2 -> 3 shards adds the new node (NODE/MEMBERS change)."""
    env = Env(decodeResponses=True)
    if env.is_cluster():
        env.skip()
    env.skipOnSlave()
    conn, my_port = _setup(env)

    env.assertEqual(conn.execute_command('timeseries.CLUSTERSET', *_clusterset_args([
        (0, 8191, '127.0.0.1:%d' % my_port),
        (8192, 16383, '127.0.0.1:%d' % (my_port + 1000))])), 'OK')
    env.assertEqual(len(_infocluster_nodes(conn)), 2)

    env.assertEqual(conn.execute_command('timeseries.CLUSTERSET', *_clusterset_args([
        (0, 5460, '127.0.0.1:%d' % my_port),
        (5461, 10922, '127.0.0.1:%d' % (my_port + 1000)),
        (10923, 16383, '127.0.0.1:%d' % (my_port + 2000))])), 'OK')
    nodes = _infocluster_nodes(conn)
    env.assertEqual(len(nodes), 3)
    env.assertEqual(sorted(int(n['minHslot']) for n in nodes), [0, 5461, 10923])


def test_clusterset_node_removed_applied():
    """A CLUSTERSET that shrinks 3 -> 2 shards drops the removed node (NODE/MEMBERS change)."""
    env = Env(decodeResponses=True)
    if env.is_cluster():
        env.skip()
    env.skipOnSlave()
    conn, my_port = _setup(env)

    env.assertEqual(conn.execute_command('timeseries.CLUSTERSET', *_clusterset_args([
        (0, 5460, '127.0.0.1:%d' % my_port),
        (5461, 10922, '127.0.0.1:%d' % (my_port + 1000)),
        (10923, 16383, '127.0.0.1:%d' % (my_port + 2000))])), 'OK')
    env.assertEqual(len(_infocluster_nodes(conn)), 3)

    env.assertEqual(conn.execute_command('timeseries.CLUSTERSET', *_clusterset_args([
        (0, 8191, '127.0.0.1:%d' % my_port),
        (8192, 16383, '127.0.0.1:%d' % (my_port + 1000))])), 'OK')
    nodes = _infocluster_nodes(conn)
    env.assertEqual(len(nodes), 2)
    env.assertEqual(sorted(int(n['minHslot']) for n in nodes), [0, 8192])


def test_clusterset_ip_and_port_change_together():
    """A CLUSTERSET that moves the peer to a new ip AND port applies both at once."""
    env = Env(decodeResponses=True)
    if env.is_cluster():
        env.skip()
    env.skipOnSlave()
    conn, my_port = _setup(env)

    env.assertEqual(conn.execute_command('timeseries.CLUSTERSET', *_clusterset_args([
        (0, 8191, '127.0.0.1:%d' % my_port),
        (8192, 16383, '127.0.0.1:%d' % (my_port + 1000))])), 'OK')
    peer = _node_by_min(_infocluster_nodes(conn), 8192)
    env.assertEqual(peer['ip'], '127.0.0.1')
    env.assertEqual(int(peer['port']), my_port + 1000)

    env.assertEqual(conn.execute_command('timeseries.CLUSTERSET', *_clusterset_args([
        (0, 8191, '127.0.0.1:%d' % my_port),
        (8192, 16383, '127.0.0.2:%d' % (my_port + 2000))])), 'OK')
    peer = _node_by_min(_infocluster_nodes(conn), 8192)
    env.assertEqual(peer['ip'], '127.0.0.2')
    env.assertEqual(int(peer['port']), my_port + 2000)


def test_clusterset_repeated_identical_is_stable():
    """Re-sending the same CLUSTERSET leaves the topology unchanged (idempotent apply)."""
    env = Env(decodeResponses=True)
    if env.is_cluster():
        env.skip()
    env.skipOnSlave()
    conn, my_port = _setup(env)

    args = _clusterset_args([
        (0, 8191, '127.0.0.1:%d' % my_port),
        (8192, 16383, '127.0.0.1:%d' % (my_port + 1000))])
    for _ in range(5):
        env.assertEqual(conn.execute_command('timeseries.CLUSTERSET', *args), 'OK')
    nodes = _infocluster_nodes(conn)
    env.assertEqual(len(nodes), 2)
    env.assertEqual(sorted(int(n['minHslot']) for n in nodes), [0, 8192])
    env.assertEqual(int(_node_by_min(nodes, 0)['maxHslot']), 8191)
    env.assertEqual(int(_node_by_min(nodes, 8192)['maxHslot']), 16383)


# ---------------------------------------------------------------------------
# MOD-16382 / MOD-16951: cluster topology-change auto-refresh behavior.
#
# These oss-cluster tests all drive the same multi-shard probe query while the
# topology changes underneath it, so they share the setup/validation/convergence
# helpers below rather than each re-deriving them.
# ---------------------------------------------------------------------------

# The multi-shard probe query and its expected result shape.
_MRANGE_COUNT_CMD = "TS.MRANGE - + FILTER label1=17 GROUPBY label1 REDUCE count"

# Transient errors that are expected (and retried) while an auto-refreshed
# topology converges: a slot caught mid-migration, or a fan-out aborted because
# the topology changed under it. Same strings as in libmr_commands.c.
SLOT_RANGES_ERROR = "Query requires unavailable slots"
TOPOLOGY_CHANGED_ERROR = "A multi-shard command failed because the cluster topology has changed"


def _data_sizes():
    # Smaller dataset under valgrind/sanitizer so the suite stays within time budget.
    return (1000 if not (VALGRIND or SANITIZER) else 100), 150


def _mrange_count_validator(number_of_keys, samples_per_key):
    """Return a validator for `_MRANGE_COUNT_CMD` asserting a complete, correct result."""
    def validate(result):
        ((filtered_by, withlabels, samples),) = result
        assert filtered_by == "label1=17"
        assert withlabels == []  # no WITHLABELS
        assert len(samples) == samples_per_key
        assert all(int(sample[1]) == number_of_keys for sample in samples)
    return validate


def _oss_cluster_topology_env(**env_kwargs):
    """A 2-shard oss-cluster env for the topology tests, or skip when not applicable."""
    env = Env(shardsCount=2, decodeResponses=True, noLog=False, **env_kwargs)
    if env.env != "oss-cluster":
        env.skip()
    if RUNNER_LABEL == "macos-15-intel":
        # slowest hosted runner; it can't reliably serve the fan-out within libmr's
        # 5s max-idle during migration churn (MOD-14615 residual, not a product bug).
        env.skip()
    return env


def _fill_and_validate_static(env):
    """Fill the standard dataset and confirm the query is correct on the static cluster.
    Returns (conn, validate)."""
    number_of_keys, samples_per_key = _data_sizes()
    fill_some_data(env, number_of_keys, samples_per_key, label1=17, label2=19)
    conn = env.getConnection(0)
    validate = _mrange_count_validator(number_of_keys, samples_per_key)
    validate(conn.execute_command(_MRANGE_COUNT_CMD))
    return conn, validate


def _my_slot_range(conn) -> SlotRange:
    for line in conn.execute_command("cluster", "nodes").splitlines():
        node = ClusterNode.from_str(line)
        if "myself" in node.flags:
            (slot_range,) = node.slots
            return slot_range
    raise ValueError("No node with 'myself' flag found")


def _middle_third(slot_range: SlotRange) -> SlotRange:
    third = (slot_range.end - slot_range.start) // 3
    return SlotRange(slot_range.start + third, slot_range.end - third)


def converge_query(conn, command, validate_result, deadline_secs):
    """Poll `command`, tolerating the transient topology errors, until it validates
    or times out."""
    deadline = time.time() + deadline_secs
    last_error = None
    while time.time() < deadline:
        try:
            result = conn.execute_command(command)
        except redis.exceptions.ResponseError as x:
            last_error = str(x)
            assert last_error in (SLOT_RANGES_ERROR, TOPOLOGY_CHANGED_ERROR), last_error
            time.sleep(0.1)
            continue
        validate_result(result)
        return
    raise AssertionError(
        f"query did not converge to a complete result within {deadline_secs}s "
        f"(auto-refresh did not repair the cluster view); last transient: {last_error}"
    )


def _reshard_and_converge(env, conn, validate, refresh):
    """Persistently move shard-0's middle third to shard-1 and then back, converging
    the query after each move. `refresh` (or None for auto-refresh) is invoked after
    each import to repair the view when the topology-change event is disabled."""
    first_conn, second_conn = env.getConnection(0), env.getConnection(1)
    deadline_secs = 60 if (VALGRIND or SANITIZER) else 15
    moved = _middle_third(_my_slot_range(first_conn))
    for src_conn, dst_conn in ((first_conn, second_conn), (second_conn, first_conn)):
        import_slots(src_conn, dst_conn, moved)
        if refresh is not None:
            refresh(env)
        converge_query(conn, _MRANGE_COUNT_CMD, validate, deadline_secs)


def test_asm_with_data_and_queries_during_migrations_auto_notified():
    # Auto-notified (MOD-16382) counterpart of the manual migration test in test_asm.py.
    # With the topology-change event, the module auto-refreshes its cluster view
    # mid-migration, so a multi-shard query keeps converging to complete results --
    # tolerating, in addition to the transient "unavailable slots" error, a transient
    # "topology has changed" abort (the auto-refresh reacting to the reshard), which the
    # client simply retries past. The manual test expects ONLY the slot-ranges error.
    env = _oss_cluster_topology_env()
    conn, validate = _fill_and_validate_static(env)

    done = threading.Event()

    def validate_command_in_a_loop():
        while not done.is_set():
            try:
                result = conn.execute_command(_MRANGE_COUNT_CMD)
            except redis.exceptions.ResponseError as x:
                # Both are expected transients while the auto-refreshed topology converges.
                assert str(x) in (SLOT_RANGES_ERROR, TOPOLOGY_CHANGED_ERROR), str(x)
                continue
            validate(result)

    def migrate_slots():
        for _ in range(MIGRATION_CYCLES):
            if done.is_set():
                break
            migrate_slots_back_and_forth(env)

    with ThreadPoolExecutor() as executor:
        futures = map(executor.submit, [validate_command_in_a_loop, migrate_slots])
        try:
            for future in as_completed(futures):
                done.set()
                future.result()
        except TimeoutError as e:
            # Under sanitizer the migration may get stuck in 'init-rdbchannel'
            # (known, MOD-15307); treat as a pass and bail out.
            if SANITIZER and "state is init-rdbchannel" in str(e):
                print(f"Ignoring known sanitizer migration timeout: {e}")
                done.set()
                return
            done.set()
            raise

    validate(conn.execute_command(_MRANGE_COUNT_CMD))


def test_asm_auto_refresh_converges_after_persistent_reshard():
    # MOD-16382: after a *lasting* slot move, the module's cluster view is repaired by the
    # topology-change event alone -- with NO manual TIMESERIES.REFRESHCLUSTER -- so a
    # multi-shard query converges to complete, correct results.
    env = _oss_cluster_topology_env()
    conn, validate = _fill_and_validate_static(env)
    _reshard_and_converge(env, conn, validate, refresh=None)


def test_manual_refreshcluster_after_persistent_reshard():
    # Backwards-compat (MOD-16382): with auto-refresh disabled, a persistent reshard leaves
    # the view stale until an explicit TIMESERIES.REFRESHCLUSTER repairs it (the mirror of
    # the auto test above).
    env = _oss_cluster_topology_env(moduleArgs="ts-topology-events no")
    conn, validate = _fill_and_validate_static(env)
    _reshard_and_converge(env, conn, validate, refresh=Refresh_Cluster)


# ---------------------------------------------------------------------------
# MOD-16951: a first multi-shard query on a "cold" coordinator must return
# promptly rather than hang until the client read timeout. The cluster is
# configured only via ClusterTopologyChange notifications (skipRefreshCluster) --
# the MOD-16382 auto-refresh path. (The production MOD-16951 bug was inter-shard
# TLS opened against a peer's plaintext port; it only reproduces on a dual-TLS
# cluster, which RLTest can't model, so this exercises the non-TLS cold-fan-out.)
# ---------------------------------------------------------------------------


def test_first_multishard_query_after_topology_setup():
    """A first (cold-coordinator) multi-shard command must return promptly with
    the correct result rather than hanging until the client read timeout.

    The client uses an explicit socket timeout so the silent max-idle hang
    ("execution max idle reached" with no reply) surfaces as a TimeoutError."""
    env = Env(shardsCount=3, decodeResponses=True, skipRefreshCluster=True)
    if env.env != 'oss-cluster':
        env.skip()

    number_of_keys = 30
    with env.getClusterConnectionIfNeeded() as rc:
        for i in range(number_of_keys):
            rc.execute_command('TS.CREATE', f'ts:{{key{i}}}', 'LABELS', 'kind', 'reg')

    # Well above libmr's ~5s max-idle: a healthy fan-out replies in milliseconds,
    # so this only trips when the command actually hangs.
    socket_timeout = 30 if (VALGRIND or SANITIZER) else 8

    for shard in range(env.shardsCount):
        base = env.getConnection(shard)
        kwargs = dict(base.connection_pool.connection_kwargs)
        kwargs['socket_timeout'] = socket_timeout
        conn = redis.Redis(**kwargs)
        try:
            res = conn.execute_command('TS.QUERYINDEX', 'kind=reg')
        except redis.exceptions.TimeoutError:
            env.assertTrue(
                False,
                message="coordinator shard %d hung on the first cross-shard "
                        "TS.QUERYINDEX (MOD-16951 max-idle hang)" % shard)
            continue
        finally:
            conn.close()
        env.assertEqual(
            len(res), number_of_keys,
            message="coordinator shard %d returned %d/%d series -- multi-shard "
                    "fan-out did not complete" % (shard, len(res), number_of_keys))
