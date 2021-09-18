# import pytest
import redis
# from RLTest import Env
from utils import Env
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

    with env.getClusterConnectionIfNeeded() as r:
        # different type key
        r.execute_command('SET', 'foo', 'bar')
        env.expect('TS.GET', 'foo', '*', '5', conn=r).error()  # too many args
        env.expect('TS.GET', 'foo', conn=r).error()  # wrong type
        env.expect('TS.GET', 'bar', conn=r).error()  # does not exist
        env.expect('TS.INFO', 'foo', conn=r).error()  # wrong type
        env.expect('TS.INFO', 'bar', conn=r).error()  # does not exist
        env.expect('TS.RANGE', 'foo', '0', '-1', conn=r).error()
        env.expect('TS.ALTER', 'foo', conn=r).error()
        env.expect('TS.INCRBY', 'foo', '1', 'timestamp', '5', conn=r).error()
        env.expect('TS.DECRBY', 'foo', '1', 'timestamp', '5', conn=r).error()
        env.expect('TS.ADD', 'values', 'timestamp', '5', conn=r).error()  # string
        env.expect('TS.ADD', 'values', '*', 'value', conn=r).error()  # string

        labels = ["abc"] * 51
        env.expect('TS.MGET', 'SELECTED_LABELS', *labels, 'FILTER', 'metric=cpu', conn=r).error()
        env.expect('TS.MRANGE', 'SELECTED_LABELS', *labels, 'FILTER', 'metric=cpu', conn=r).error()
        
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
