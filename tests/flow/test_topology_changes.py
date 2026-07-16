from utils import Env
from includes import *


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
