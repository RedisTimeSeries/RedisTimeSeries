# import pytest
# import redis
import random
from threading import Thread
import time

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
        assert str(e) == "A multi-shard command failed because at least one shard did not reply within the given timeframe."

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

        threads = []
        cons = []
        for i in range(0,20):
            # expect a new connection to arrive
            cons.append(env.getConnection(random.randint(0, env.shardsCount - 1)))
            threads.append(Thread(target=libmr_query, args=(cons[i], env, start_ts, samples_count)))

        for i in range(len(threads)):
            threads[i].start()
            cons[i].close()

        # wait for processes to join
        [th.join() for th in threads]

        # make sure we did not crash
        r.ping()
        r.close()


# Must match src/libmr_commands.c (check_and_reply_on_error) after MOD-14439 / PR #1930.
TOPOLOGY_CHANGED_USER_ERR = (
    "A multi-shard command failed because the cluster topology has changed"
)


def test_libmr_topology_change_during_mrange():
    """
    MOD-14439: LibMR aborts initiator executions when timeseries.REFRESHCLUSTER is called.
    Uses DEBUG SLEEP on a remote shard to guarantee the MRANGE execution is in-flight (waiting
    for that shard's reply) when we fire REFRESHCLUSTER on the initiator shard.
    """
    env = Env()
    if env.shardsCount < 3:
        env.skip()
    if not env.is_cluster():
        env.skip()
    env.skipOnSlave()
    env.skipOnAOF()

    start_ts = 1
    samples_count = 10
    with env.getClusterConnectionIfNeeded() as r:
        assert r.execute_command("TS.CREATE", "tester1{1}", "LABELS", "name", "bob")
        _insert_data(r, "tester1{1}", start_ts, samples_count, 1)

    initiator_shard = 1
    remote_shard = 2

    mrange_error = [None]

    def do_mrange():
        try:
            conn = env.getConnection(initiator_shard)
            conn.execute_command(
                "TS.mrange", start_ts, start_ts + samples_count,
                "WITHLABELS", "FILTER", "name=bob",
            )
        except Exception as e:
            mrange_error[0] = str(e)

    def do_sleep_remote():
        try:
            conn = env.getConnection(remote_shard)
            conn.execute_command("DEBUG", "SLEEP", "2")
        except Exception:
            pass

    # 1. Block remote shard so it can't process internal commands
    t_sleep = Thread(target=do_sleep_remote)
    t_sleep.start()
    time.sleep(0.3)

    # 2. Fire MRANGE — it will block waiting for the sleeping shard
    t_mrange = Thread(target=do_mrange)
    t_mrange.start()
    time.sleep(0.3)

    # 3. While MRANGE is stuck waiting, fire REFRESHCLUSTER on the initiator
    try:
        env.getConnection(initiator_shard).execute_command("timeseries.REFRESHCLUSTER")
    except Exception:
        pass

    t_mrange.join(timeout=30)
    t_sleep.join(timeout=30)

    env.assertEqual(
        mrange_error[0],
        TOPOLOGY_CHANGED_USER_ERR,
        message=(
            "Expected MRANGE to get %r but got %r"
            % (TOPOLOGY_CHANGED_USER_ERR, mrange_error[0])
        ),
    )

    _waitCluster(env)
    Refresh_Cluster(env)
    verifyClusterInitialized(env)
    expected_res = [
        [
            b"tester1{1}",
            [[b"name", b"bob"]],
            [
                [1, b"1"], [2, b"1"], [3, b"1"], [4, b"1"], [5, b"1"],
                [6, b"1"], [7, b"1"], [8, b"1"], [9, b"1"], [10, b"1"],
            ],
        ]
    ]
    actual_result = env.getConnection(initiator_shard).execute_command(
        "TS.mrange", start_ts, start_ts + samples_count,
        "WITHLABELS", "FILTER", "name=bob",
    )
    env.assertEqual(actual_result, expected_res)
