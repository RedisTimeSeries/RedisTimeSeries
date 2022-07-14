import pytest
import redis
import time
from utils import Env, set_hertz
from includes import *


def test_latest_flag_get():
    env = Env(decodeResponses=True)
    key1 = 't1{1}'
    key2 = 't2{1}'
    with env.getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', key1)
        assert r.execute_command('TS.CREATE', key2)
        assert r.execute_command('TS.CREATERULE', key1, key2, 'AGGREGATION', 'SUM', 10)
        assert r.execute_command('TS.add', key1, 1, 1)
        assert r.execute_command('TS.add', key1, 2, 3)
        assert r.execute_command('TS.add', key1, 11, 7)
        assert r.execute_command('TS.add', key1, 13, 1)
        res = r.execute_command('TS.range', key1, 0, 20)
        assert res == [[1, '1'], [2, '3'], [11, '7'], [13, '1']] or res == [[1, b'1'], [2, b'3'], [11, b'7'], [13, b'1']]
        res = r.execute_command('TS.get', key2)
        assert res == [0, '4'] or res == [0, b'4']
        res = r.execute_command('TS.get', key2, "LATEST")
        assert res == [10, '8'] or res == [10, b'8']

        # make sure LATEST haven't changed anything in the keys
        res = r.execute_command('TS.range', key2, 0, 10)
        assert res == [[0, '4']] or res == [[0, b'4']]
        res = r.execute_command('TS.range', key1, 0, 20)
        assert res == [[1, '1'], [2, '3'], [11, '7'], [13, '1']] or res == [[1, b'1'], [2, b'3'], [11, b'7'], [13, b'1']]