import time

import redis

from includes import Env, VALGRIND, SANITIZER


# MOD-16951 regression test.
#
# Since MOD-16382 RedisTimeSeries configures LibMR's cluster view automatically
# from Redis' ClusterTopologyChange notifications, instead of relying on an
# explicit REFRESHCLUSTER/CLUSTERSET followed by FORCESHARDSCONNECTION.
#
# The regression: the topology-notification path (MR_UpdateClusterTopologyIfNeeded)
# installed the cluster topology but never established the inter-shard (libmr)
# connections. Those connections were only created lazily on the first
# multi-shard command. On a "cold" coordinator that first fan-out then depends on
# the whole connect -> AUTH -> HELLO -> resend handshake (for every peer *and* the
# self loopback) completing within libmr's ~5s max-idle window; the map requests
# were logged as dropped ("message was not sent because status is not connected"),
# and if the handshake did not finish in time the reduce step never received the
# shard responses and the execution aborted with "execution max idle reached" --
# so TS.MRANGE / TS.QUERYINDEX / TS.MGET / TS.MREVRANGE hung and returned nothing
# to the client.
#
# The fix makes the topology-notification path eagerly connect to the shards
# right after installing a topology (MR_UpdateClusterTopologyIfNeeded ->
# MR_ClusterConnectToShards), so the connections are warm before any command
# arrives instead of racing the first command's deadline.
#
# NOTE: the behavioral fix lives in the LibMR submodule (deps/LibMR
# src/cluster.c). This test guards the RedisTimeSeries side and turns red until
# the accompanying deps/LibMR bump lands.


_CONNECT_TIMEOUT = 60 if (VALGRIND or SANITIZER) else 10


def _node_connection_statuses(conn):
    """Return the per-node libmr connection statuses reported by INFOCLUSTER."""
    # On Redis 8.0+ INFOCLUSTER is an internal command; promote the client.
    try:
        conn.execute_command('debug', 'MARK-INTERNAL-CLIENT')
    except Exception:
        pass  # older Redis without the subcommand
    res = conn.execute_command('timeseries.INFOCLUSTER')
    nodes = res[4]
    return [n[17] for n in nodes]


def _wait_for_all_connected(conn, timeout_sec):
    """Poll INFOCLUSTER until every node reports 'connected', or time out.

    Returns the last observed statuses list."""
    deadline = time.time() + timeout_sec
    statuses = []
    while time.time() < deadline:
        statuses = _node_connection_statuses(conn)
        # The local node reports 'connected'; a peer must have an established
        # connection. 'uninitialized'/'disconnected' mean the inter-shard
        # connection was never (re)established off the notification path.
        if statuses and all(s == b'connected' for s in statuses):
            return statuses
        time.sleep(0.1)
    return statuses


def test_topology_notifications_establish_connections():
    """The inter-shard connections must be established off the topology-change
    notification path, without any prior multi-shard command (MOD-16951).

    We skip the manual REFRESHCLUSTER so the cluster is configured *only* via
    the ClusterTopologyChange notifications -- exactly the code path that
    regressed. This is the deterministic check: before the fix the peer nodes
    stay 'uninitialized'; after the fix they converge to 'connected' on their
    own.
    """
    env = Env(shardsCount=3, decodeResponses=False, skipRefreshCluster=True)
    if env.env != 'oss-cluster':
        env.skip()

    conn = env.getConnection(1)

    statuses = _wait_for_all_connected(conn, _CONNECT_TIMEOUT)
    env.assertTrue(len(statuses) >= 3, message="INFOCLUSTER did not report the cluster nodes")
    for s in statuses:
        env.assertEqual(
            s, b'connected',
            message="inter-shard connection not established off the topology-change "
                    "notification (got statuses %r)" % (statuses,))


def test_first_multishard_query_after_topology_setup():
    """A first (cold-coordinator) multi-shard command must return promptly with
    the correct result rather than hanging until the client read timeout
    (MOD-16951).

    The client uses an explicit socket timeout so that the silent max-idle hang
    ("execution max idle reached" with no reply) surfaces as a TimeoutError and
    fails the test instead of blocking.
    """
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
