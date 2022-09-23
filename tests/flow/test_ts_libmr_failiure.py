import pytest
import redis
import random
from threading import Thread
from time import sleep

from utils import Env, Refresh_Cluster
from test_helper_classes import _get_series_value, calc_rule, ALLOWED_ERROR, _insert_data, \
    _get_ts_info, _insert_agg_data
from includes import *

def testLibmrFail():
    env = Env()
    if env.shardsCount < 3:
        env.skip()
    if(not env.isCluster):
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

    Refresh_Cluster(env)
    try:
        actual_result = env.getConnection(1).execute_command('TS.mrange', start_ts, start_ts + samples_count, 'WITHLABELS', 'FILTER',
                                'name=bob')
        assert(False)
    except Exception as e:
        env.assertResponseError(e, "multi shard cmd failed")
    
    env.envRunner.shards[2].startEnv()
    Refresh_Cluster(env)
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
    if(not env.isCluster):
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

        # make sure we did not crashed
        r.ping()
        r.close()