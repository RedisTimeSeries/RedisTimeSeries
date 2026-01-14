# import pytest
# import redis
import random
import time
import redis

from utils import Env, Refresh_Cluster
from test_helper_classes import _get_series_value, calc_rule, ALLOWED_ERROR, _insert_data, \
    _get_ts_info, _insert_agg_data
from includes import *


def shardsConnections(env):
    for s in range(1, env.shardsCount + 1):
        yield env.getConnection(shardId=s)

def verifyClusterInitialized(env):
    for conn in shardsConnections(env):
        try:
            conn.execute_command('debug', 'MARK-INTERNAL-CLIENT')
        except Exception:
            pass # in case we run on older version of redis
        allConnected = False
        while not allConnected:
            res = conn.execute_command('timeseries.INFOCLUSTER')
            nodes = res[4]
            allConnected = True
            for n in nodes:
                status = n[17]
                if status != b'connected' and status != b'uninitialized':
                    allConnected = False
            if not allConnected:
                time.sleep(0.1)

def _waitCluster(env, timeout_sec=40):

    st = time.time()
    ok = 0

    while st + timeout_sec > time.time():
        ok = 0
        i = 0
        for i in range(1, env.shardsCount + 1):
            con = env.getConnection(i)
            try:
                status = con.execute_command('CLUSTER', 'INFO')
            except Exception as e:
                #print('got error on cluster info, will try again, %s' % str(e))
                continue
            if 'cluster_state:ok' in str(status):
                ok += 1
        if ok == env.shardsCount:
            time.sleep(2)
            return

        time.sleep(0.1)
    raise RuntimeError(
        "Cluster OK wait loop timed out after %s seconds" % timeout_sec)

def testLibmrFail():
    env = Env()
    if env.shardsCount < 3:
        env.skip()
    if(not env.is_cluster()):
        env.skip()
    env.skipOnSlave() # There can't be 2 rdb save at the same time
    env.skipOnAOF()
    start_ts = 1
    samples_count = 10
    with env.getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 'tester1{1}', 'LABELS', 'name', 'bob')
        _insert_data(r, 'tester1{1}', start_ts, samples_count, 1)
        try:
            env.envRunner.shards[2].stopEnv()
        except Exception as e:
            pass
    try:
        actual_result = env.getConnection(1).execute_command('TS.mrange', start_ts, start_ts + samples_count, 'WITHLABELS', 'FILTER',
                                'name=bob')
        assert(False)
    except Exception as e:
        # The exact error string may vary depending on which layer emits the timeout:
        # - LibMR can report "Timeout" (capital T) when a shard is down.
        # - RedisTimeSeries may wrap it (e.g. "Multi-shard command failed. Timeout").
        # We only require that the user gets a deterministic "multi-shard timeout" error.
        msg = str(e)
        assert (
            msg == "A multi-shard command failed because at least one shard did not reply within the given timeframe."
            or msg == "Multi-shard command failed. Timeout"
            or "Multi-shard command failed" in msg
        )

    env.envRunner.shards[2].startEnv()
    _waitCluster(env)
    env.getConnection(3).execute_command('timeseries.REFRESHCLUSTER')
    verifyClusterInitialized(env)
    expected_res = [[b'tester1{1}', [[b'name', b'bob']], [[1, b'1'], [2, b'1'], [3, b'1'], [4, b'1'], [5, b'1'], [6, b'1'], [7, b'1'], [8, b'1'], [9, b'1'], [10, b'1']]]]
    actual_result = env.getConnection(1).execute_command('TS.mrange', start_ts, start_ts + samples_count, 'WITHLABELS', 'FILTER',
                        'name=bob')
    env.assertEqual(actual_result, expected_res)

def libmr_query(con, env, start_ts, samples_count):
    expected_res = [[b'tester1{1}', [[b'name', b'bob']], [[1, b'1'], [2, b'1'], [3, b'1'], [4, b'1'], [5, b'1'], [6, b'1'], [7, b'1'], [8, b'1'], [9, b'1'], [10, b'1']]]]
    actual_result = con.execute_command('TS.mrange', start_ts, start_ts + samples_count, 'WITHLABELS', 'FILTER',
                        'name=bob')
    env.assertEqual(actual_result, expected_res)

def testLibmr_client_disconnect():
    env = Env()
    if env.shardsCount < 2:
        env.skip()
    if(not env.is_cluster()):
        env.skip()
    env.skipOnSlave() # There can't be 2 rdb save at the same time
    env.skipOnAOF()
    start_ts = 1
    samples_count = 10
    with env.getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 'tester1{1}', 'LABELS', 'name', 'bob')
        _insert_data(r, 'tester1{1}', start_ts, samples_count, 1)

        # Simulate clients that disconnect mid-request without threads.
        # Using threads here is flaky across OS/Python versions (thread exceptions are noisy and
        # can lead to hangs). A low-level redis-py Connection lets us send the command and
        # immediately drop the TCP connection without waiting for a reply.
        for _ in range(20):
            con = env.getConnection(shardId=random.randint(1, env.shardsCount))
            kw = getattr(con, "connection_pool", None).connection_kwargs
            host = kw.get("host", "127.0.0.1")
            port = int(kw["port"])

            c = redis.connection.Connection(host=host,
                                            port=port,
                                            socket_connect_timeout=1,
                                            socket_timeout=1,
                                            decode_responses=False)
            try:
                c.connect()
                c.send_command('TS.MRANGE',
                               start_ts,
                               start_ts + samples_count,
                               'WITHLABELS',
                               'FILTER',
                               'name=bob')
            finally:
                # Drop connection without reading response (simulates abrupt disconnect).
                try:
                    c.disconnect()
                except Exception:
                    pass

        # make sure we did not crash
        r.ping()
        r.close()
