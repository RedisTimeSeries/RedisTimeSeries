import time

from utils import Env
from includes import *


def _my_run_id(conn):
    res = conn.execute_command('timeseries.INFOCLUSTER')
    # reply layout: ['MyId', <id>, 'MyRunId', <run id>, ...]
    return res[3]


def _cluster_set_args(my_port, second_shard_port):
    # Long-form CLUSTERSET, exactly what the DMC sends to enterprise shards.
    # The second shard is never connected to: connections are lazy and this
    # test never runs a multi-shard command.
    return [
        'HASHFUNC', 'CRC16', 'NUMSLOTS', '16384', 'MYID', '1', 'RANGES', '2',
        'SHARD', '1', 'SLOTRANGE', '0', '8191',
        'ADDR', '@127.0.0.1:%d' % my_port, 'MASTER',
        'SHARD', '2', 'SLOTRANGE', '8192', '16383',
        'ADDR', '@127.0.0.1:%d' % second_shard_port, 'MASTER',
    ]


def test_identical_clusterset_skips_rebuild():
    """
    MOD-16399: a CLUSTERSET carrying the exact topology the cluster was built
    from must not rebuild it. A rebuild regenerates the cluster run id and
    aborts every in-flight multi-shard command with "cluster topology has
    changed", so no-op re-broadcasts (which the DMC issues on node events,
    shard reconnects and delivery retries) failed such commands spuriously.
    """
    env = Env(decodeResponses=True)
    if env.is_cluster():
        env.skip()  # oss-cluster owns its topology; CLUSTERSET is the enterprise path
    env.skipOnSlave()

    conn = env.getConnection()
    try:
        conn.execute_command('debug', 'MARK-INTERNAL-CLIENT')
    except Exception:
        pass  # older redis: INFOCLUSTER is not an internal command there

    my_port = conn.connection_pool.connection_kwargs['port']

    env.assertEqual(conn.execute_command(
        'timeseries.CLUSTERSET', *_cluster_set_args(my_port, my_port + 1)), 'OK')
    run_id = _my_run_id(conn)

    # An identical re-send must keep the cluster (same run id).
    env.assertEqual(conn.execute_command(
        'timeseries.CLUSTERSET', *_cluster_set_args(my_port, my_port + 1)), 'OK')
    env.assertEqual(_my_run_id(conn), run_id)

    # A genuinely different topology must still rebuild (new run id).
    env.assertEqual(conn.execute_command(
        'timeseries.CLUSTERSET', *_cluster_set_args(my_port, my_port + 2)), 'OK')
    env.assertNotEqual(_my_run_id(conn), run_id)


def _clusterset_args_ranges(my_port, second_shard_port, s1_end, s2_start):
    # Long-form CLUSTERSET with caller-chosen slot boundaries, to craft
    # malformed (gap / overlap) topologies.
    return [
        'HASHFUNC', 'CRC16', 'NUMSLOTS', '16384', 'MYID', '1', 'RANGES', '2',
        'SHARD', '1', 'SLOTRANGE', '0', str(s1_end),
        'ADDR', '@127.0.0.1:%d' % my_port, 'MASTER',
        'SHARD', '2', 'SLOTRANGE', str(s2_start), '16383',
        'ADDR', '@127.0.0.1:%d' % second_shard_port, 'MASTER',
    ]


def test_malformed_clusterset_is_rejected():
    """
    MOD-16382/16399 topology integrity: a CLUSTERSET whose slots are not fully
    covered (a gap) or are double-claimed (an overlap) must be rejected -- the
    current topology stays intact (run id unchanged), rather than installing a
    broken slot map.
    """
    env = Env(decodeResponses=True)
    if env.is_cluster():
        env.skip()  # oss-cluster owns its topology; CLUSTERSET is the enterprise path
    env.skipOnSlave()

    conn = env.getConnection()
    try:
        conn.execute_command('debug', 'MARK-INTERNAL-CLIENT')
    except Exception:
        pass

    my_port = conn.connection_pool.connection_kwargs['port']

    # Valid baseline (full, non-overlapping coverage).
    env.assertEqual(conn.execute_command(
        'timeseries.CLUSTERSET', *_cluster_set_args(my_port, my_port + 1)), 'OK')
    run_id = _my_run_id(conn)

    # Gap: shard 1 stops at 8000, shard 2 starts at 8192 -> 8001..8191 uncovered.
    try:
        conn.execute_command(
            'timeseries.CLUSTERSET', *_clusterset_args_ranges(my_port, my_port + 1, 8000, 8192))
        gap_rejected = False
    except Exception:
        gap_rejected = True
    env.assertTrue(gap_rejected)
    env.assertEqual(_my_run_id(conn), run_id)  # topology unchanged

    # Overlap: slot 8192 claimed by both shards.
    try:
        conn.execute_command(
            'timeseries.CLUSTERSET', *_clusterset_args_ranges(my_port, my_port + 1, 8192, 8192))
        overlap_rejected = False
    except Exception:
        overlap_rejected = True
    env.assertTrue(overlap_rejected)
    env.assertEqual(_my_run_id(conn), run_id)  # topology still unchanged
