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
