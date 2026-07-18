import redis

from includes import Env, VALGRIND, SANITIZER


# Cold-coordinator multi-shard fan-out guard (MOD-16951 area).
#
# Since MOD-16382 RedisTimeSeries configures LibMR's cluster view automatically
# from Redis' ClusterTopologyChange notifications, instead of relying on an
# explicit REFRESHCLUSTER/CLUSTERSET followed by FORCESHARDSCONNECTION. This test
# skips the manual REFRESHCLUSTER (skipRefreshCluster) so the cluster is
# configured *only* via those notifications, then asserts that a first
# multi-shard command on a "cold" coordinator returns promptly with the correct
# result instead of hanging until the client read timeout.
#
# NOTE: the actual MOD-16951 production bug was an inter-shard TLS handshake sent
# to a peer's *plaintext* port (checkTLS said TLS because tls-port was set while
# tls-cluster was off, but the native cluster view hands back the plaintext
# port), which deadlocks the peer's RESP parser so timeseries.HELLO never
# completes and the fan-out reduce hits the ~5s max-idle. That is fixed in the
# LibMR submodule (checkTLS now gates inter-shard TLS on tls-cluster=yes / an
# explicit long-form CLUSTERSET). It only reproduces on a dual-TLS cluster
# (tls-cluster no + tls-port + plaintext bus), which RLTest cannot model, so this
# RLTest guard exercises the non-TLS cold-fan-out path rather than that bug
# directly.


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
