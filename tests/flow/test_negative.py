import pytest
import redis
from RLTest import Env
from includes import *


def test_errors(env):
    with env.getConnection() as r:
        # test wrong arity
        env.expect('TS.CREATE', conn=r).error()
        env.expect('TS.ALTER', conn=r).error()
        env.expect('TS.ADD', conn=r).error()
        env.expect('TS.MADD', conn=r).error()
        env.expect('TS.INCRBY', conn=r).error()
        env.expect('TS.DECRBY', conn=r).error()
        env.expect('TS.CREATERULE', conn=r).error()
        env.expect('TS.DELETERULE', conn=r).error()
        env.expect('TS.QUERYINDEX', conn=r).error()
        env.expect('TS.GET', conn=r).error()
        env.expect('TS.MGET', conn=r).error()
        env.expect('TS.RANGE', conn=r).error()
        env.expect('TS.MRANGE', conn=r).error()
        env.expect('TS.INFO', conn=r).error()

    with Env().getClusterConnectionIfNeeded() as r:
        # different type key
        r.execute_command('SET', 'foo', 'bar')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.GET', 'foo', '*', '5')  # too many args
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.GET', 'foo')  # wrong type
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.GET', 'bar')  # does not exist
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.INFO', 'foo')  # wrong type
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.INFO', 'bar')  # does not exist
        env.expect('TS.RANGE', 'foo', '0', '-1', conn=r).error()
        env.expect('TS.ALTER', 'foo', conn=r).error()
        env.expect('TS.INCRBY', 'foo', '1', 'timestamp', '5', conn=r).error()
        env.expect('TS.DECRBY', 'foo', '1', 'timestamp', '5', conn=r).error()
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.ADD', 'values', 'timestamp', '5')  # string
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.ADD', 'values', '*', 'value')  # string
        with pytest.raises(redis.ResponseError) as excinfo:
            labels = ["abc"] * 51
            env.expect('TS.MGET', 'SELECTED_LABELS', *labels, 'FILTER', 'metric=cpu', conn=r).noError()
        with pytest.raises(redis.ResponseError) as excinfo:
            labels = ["abc"] * 51
            env.expect('TS.MRANGE', 'SELECTED_LABELS', *labels, 'FILTER', 'metric=cpu', conn=r).noError()
        env.expect('TS.MRANGE', '-', '+', 'ALIGN', 'FILTER', 'metric=cpu', conn=r).error()
        env.expect('TS.MRANGE', '-', '+', 'ALIGN', '2dd2', 'FILTER', 'metric=cpu', conn=r).error()
        env.expect('TS.MRANGE', '-', '+', 'ALIGN', 'start2', 'FILTER', 'metric=cpu', conn=r).error()
        env.expect('TS.MRANGE', '-', '+', 'ALIGN', 'end2', 'FILTER', 'metric=cpu', conn=r).error()
        env.expect('TS.CREATE', 'tester', conn=r).noError()
        env.expect('TS.RANGE', 'tester', '-', '+', 'ALIGN', conn=r).error()
        env.expect('TS.RANGE', 'tester', '-', '+', 'ALIGN', 'start', conn=r).error()
        env.expect('TS.RANGE', 'tester', '-', '+', 'ALIGN', 'start', 'AGGREGATION', 'max', 60000, conn=r).error()
        env.expect('TS.RANGE', 'tester', '-', '1627460206991', 'ALIGN', 'start', 'AGGREGATION', 'max', 60000, conn=r).error()
        env.expect('TS.RANGE', 'tester', '-', '+', 'ALIGN', 'end', 'AGGREGATION', 'max', 60000, conn=r).error()
        env.expect('TS.RANGE', 'tester', '1627460206991', '+', 'ALIGN', 'end', 'AGGREGATION', 'max', 60000, conn=r).error()
