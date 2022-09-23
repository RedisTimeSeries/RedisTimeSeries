import pytest
import redis
from utils import Env
from test_helper_classes import _insert_data
from test_ts_range import build_expected_aligned_data
from includes import *
import random
from datetime import datetime

def test_ts_slowlog():
    # Issue a long running query, this should replace an existing entry in the slowlog.
    env = Env()
    if env.shardsCount < 3:
        env.skip()
    if(not env.isCluster):
        env.skip()
    slowlog_threshold = []
    for shard in range(0, env.shardsCount):
        # Lower hz value to make it more likely that mrange triggers key expiration
        slowlog_threshold = env.getConnection(shard).execute_command('config get slowlog-log-slower-than')
        assert env.getConnection(shard).execute_command('config set slowlog-log-slower-than 10') == b'OK'

    samples_count = 1000
    seed = datetime.now()
    random.seed(seed)
    with Env(decodeResponses=True).getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 'tester1{1}', 'LABELS', 'name', 'bob')
        assert r.execute_command('TS.CREATE', 'tester2{3}', 'LABELS', 'name', 'fabi')
        assert r.execute_command('TS.CREATE', 'tester3{2}', 'LABELS', 'name', 'alice')
        assert r.execute_command('TS.CREATE', 'tester4{2}', 'LABELS', 'name', 'bob')
        assert r.execute_command('TS.CREATE', 'tester5{3}', 'LABELS', 'name', 'bob')
        for i in range (1, samples_count + 1):
            assert r.execute_command('ts.add', 'tester1{1}', i, random.randint(0, 1))
            assert r.execute_command('ts.add', 'tester2{3}', i, random.randint(0, 1))
            assert r.execute_command('ts.add', 'tester3{2}', i, random.randint(0, 1))
            assert r.execute_command('ts.add', 'tester4{2}', i, random.randint(0, 1))
            assert r.execute_command('ts.add', 'tester5{3}', i, random.randint(0, 1))

        env.getConnection(1).execute_command('TS.mrange', '-', '+', 'AGGREGATION', 'MAX', 50, 'FILTER', 'name=(bob,fabi,alice)')

        # revert the config changes
        for shard in range(0, env.shardsCount):
            # Lower hz value to make it more likely that mrange triggers key expiration
            assert env.getConnection(shard).execute_command('config set slowlog-log-slower-than {}'.format(int(slowlog_threshold[1]))) == b'OK'

        # get redis slowlog
        slowlog = r.slowlog_get()
        env.assertGreater(len(slowlog), 0)
        slowlog_commands = []
        if isinstance(slowlog, dict):
            for serverlog in [serverlog for serverlog in slowlog.values()]:
                for log in serverlog:
                    slowlog_commands.append(log["command"])
        else:
            slowlog_commands = [log["command"] for log in slowlog]

        # validate the command added to redis slowlog
        env.assertContains(b"TS.mrange - + AGGREGATION MAX 50 FILTER name=(bob,fabi,alice)", slowlog_commands)
