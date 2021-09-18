import time

import pytest
import redis
from utils import Env
from includes import *


def test_incrby(env):
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'tester')

        start_incr_time = int(time.time() * 1000)
        for i in range(20):
            r.execute_command('ts.incrby', 'tester', '5')
            time.sleep(0.001)

        start_decr_time = int(time.time() * 1000)
        for i in range(20):
            r.execute_command('ts.decrby', 'tester', '1.5')
            time.sleep(0.001)

        now = int(time.time() * 1000)
        result = r.execute_command('TS.RANGE', 'tester', 0, now)
        env.assertEqual(result[-1][1], '70')
        assert result[-1][0] <= now
        assert result[0][0] >= start_incr_time
        assert len(result) <= 40


def test_incrby_with_timestamp(env):
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'tester')

        for i in range(20):
            env.expect('ts.incrby', 'tester', '5', 'TIMESTAMP', i, conn=r).equal(i)
        result = r.execute_command('TS.RANGE', 'tester', 0, 20)
        env.assertEqual(len(result), 20)
        env.assertEqual(result[19][1], '100')

        query_res = r.execute_command('ts.incrby', 'tester', '5', 'TIMESTAMP', '*') / 1000
        cur_time = int(time.time())
        assert query_res >= cur_time
        assert query_res <= cur_time + 1

        env.expect('ts.incrby', 'tester', '5', 'TIMESTAMP', '10', conn=r).error()


def test_incrby_with_update_latest(env):
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'tester')
        for i in range(1, 21):
            env.expect('ts.incrby', 'tester', '5', 'TIMESTAMP', i, conn=r).equal(i)

        result = r.execute_command('TS.RANGE', 'tester', 0, 20)
        env.assertEqual(len(result), 20)
        env.assertEqual(result[19], [20, '100'])

        env.expect('ts.incrby', 'tester', '5', 'TIMESTAMP', 20, conn=r).equal(i)
        result = r.execute_command('TS.RANGE', 'tester', 0, 20)
        env.assertEqual(len(result), 20)
        env.assertEqual(result[19], [20, '105'])

        env.expect('ts.decrby', 'tester', '10', 'TIMESTAMP', 20, conn=r).equal(i)
        result = r.execute_command('TS.RANGE', 'tester', 0, 20)
        env.assertEqual(len(result), 20)
        env.assertEqual(result[19], [20, '95'])
