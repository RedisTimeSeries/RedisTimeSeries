import math
import time

# import pytest
# import redis
# from utils import Env
from includes import *


def test_incrby():
    with Env().getClusterConnectionIfNeeded() as r:
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
        assert result[-1][1] == b'70'
        assert result[-1][0] <= now
        assert result[0][0] >= start_incr_time
        assert len(result) <= 40


def test_incrby_with_timestamp():
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'tester')

        for i in range(20):
            assert r.execute_command('ts.incrby', 'tester', '5', 'TIMESTAMP', i) == i
        result = r.execute_command('TS.RANGE', 'tester', 0, 20)
        assert len(result) == 20
        assert result[19][1] == b'100'

        query_res = r.execute_command('ts.incrby', 'tester', '5', 'TIMESTAMP', '*')
        query_res = math.floor(query_res / 1000)  # To seconds
        assert time.time() >= query_res

        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('ts.incrby', 'tester', '5', 'TIMESTAMP', '10')


def test_incrby_with_update_latest():
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'tester')
        for i in range(1, 21):
            assert r.execute_command('ts.incrby', 'tester', '5', 'TIMESTAMP', i) == i

        result = r.execute_command('TS.RANGE', 'tester', 0, 20)
        assert len(result) == 20
        assert result[19] == [20, b'100']

        assert r.execute_command('ts.incrby', 'tester', '5', 'TIMESTAMP', 20) == i
        result = r.execute_command('TS.RANGE', 'tester', 0, 20)
        assert len(result) == 20
        assert result[19] == [20, b'105']

        assert r.execute_command('ts.decrby', 'tester', '10', 'TIMESTAMP', 20) == i
        result = r.execute_command('TS.RANGE', 'tester', 0, 20)
        assert len(result) == 20
        assert result[19] == [20, b'95']
