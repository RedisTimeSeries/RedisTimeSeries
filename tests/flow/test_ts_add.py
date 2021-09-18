import time

# import pytest
import redis
from RLTest import Env
from test_helper_classes import _get_ts_info, TSInfo
from includes import *


def test_issue_504(env):
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'tester')
        for i in range(100, 3000):
            env.expect('ts.add', 'tester', i, i * 1.1, conn=r).equal(i)
        env.expect('ts.add', 'tester', 99, 1, conn=r).equal(99)
        env.expect('ts.add', 'tester', 98, 1, conn=r).equal(98)


def test_issue_588(env):
    def result(res):
        return float(res[0][1])
        
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'test1', "DUPLICATE_POLICY", "min")
        r.execute_command('ts.add', 'test1', 1, -0.05)
        env.expect('TS.RANGE', 'test1', "-", "+", conn=r).apply(result).equal(-0.05)
        r.execute_command('ts.add', 'test1', 1, -0.06)
        env.expect('TS.RANGE', 'test1', "-", "+", conn=r).apply(result).equal(-0.06)

        r.execute_command('ts.create', 'test2', "DUPLICATE_POLICY", "max")
        r.execute_command('ts.add', 'test2', 1, -0.06)
        env.expect('TS.RANGE', 'test2', "-", "+", conn=r).apply(result).equal(-0.06)
        r.execute_command('ts.add', 'test2', 1, -0.05)
        env.expect('TS.RANGE', 'test2', "-", "+", conn=r).apply(result).equal(-0.05)


def test_automatic_timestamp(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'tester', conn=r).noError()
        response_timestamp = r.execute_command('TS.ADD', 'tester', '*', 1)
        curr_time = int(time.time() * 1000)
        result = r.execute_command('TS.RANGE', 'tester', 0, curr_time)
        # test time difference is not more than 5 milliseconds
        env.assertTrue(result[0][0] - curr_time <= 5)
        env.assertTrue(response_timestamp - curr_time <= 5)


def test_add_create_key(env):
    with env.getClusterConnectionIfNeeded() as r:
        ts = time.time()
        env.expect('TS.ADD', 'tester1', str(int(ts)), str(ts), 'RETENTION', '666',
                   'LABELS', 'name', 'blabla', conn=r).equal(int(ts))
        info = _get_ts_info(r, 'tester1')
        env.assertEqual(info.total_samples, 1)
        env.assertEqual(info.retention_msecs, 666)
        env.assertEqual(info.labels, {'name': 'blabla'})

        env.expect('TS.ADD', 'tester2', str(int(ts)), str(ts), 'LABELS', 'name',
                   'blabla2', 'location', 'earth', conn=r).noError()
        info = _get_ts_info(r, 'tester2')
        env.assertEqual(info.total_samples, 1)
        env.assertEqual(info.labels,  {'location': 'earth', 'name': 'blabla2'})


def test_ts_add_encoding(env):
    for ENCODING in ['compressed','uncompressed']:
        env.flush()
        with env.getClusterConnectionIfNeeded() as r:
            r.execute_command('ts.add', 't1', '*', '5.0', 'ENCODING', ENCODING)
            env.assertEqual(TSInfo(r.execute_command('TS.INFO', 't1')).chunk_type, ENCODING)
            # backwards compatible check
            r.execute_command('ts.add', 't1_bc', '*', '5.0', ENCODING)
            env.assertEqual(TSInfo(r.execute_command('TS.INFO', 't1_bc')).chunk_type, ENCODING)


def test_valid_labels(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'tester', 'LABELS', 'name', '', conn=r).raiseError()
        env.expect('TS.ADD', 'tester2', '*', 1, 'LABELS', 'name', 'myName', 'location', '', conn=r).raiseError()
        env.expect('TS.ADD', 'tester2', '*', 1, 'LABELS', 'name', 'myName', 'location', 'list)', conn=r).raiseError()
        env.expect('TS.ADD', 'tester2', '*', 1, 'LABELS', 'name', 'myName', 'location', 'li(st', conn=r).raiseError()
        env.expect('TS.ADD', 'tester2', '*', 1, 'LABELS', 'name', 'myName', 'location', 'lis,t', conn=r).raiseError()


def test_valid_timestamp(env):
    with env.getConnection() as r:
        env.expect('TS.ADD', 'timestamp', '12434fd', '34', conn=r).raiseError()
        env.expect('TS.ADD', 'timestamp', '-34', '22', conn=r).raiseError()
        env.expect('TS.ADD', 'timestamp', '*235', '45', conn=r).raiseError()


def test_gorilla(env):
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'monkey')
        r.execute_command('ts.add', 'monkey', '0', '1')
        r.execute_command('ts.add', 'monkey', '1', '1')
        r.execute_command('ts.add', 'monkey', '2', '1')
        r.execute_command('ts.add', 'monkey', '50', '1')
        r.execute_command('ts.add', 'monkey', '51', '1')
        r.execute_command('ts.add', 'monkey', '500', '1')
        r.execute_command('ts.add', 'monkey', '501', '1')
        r.execute_command('ts.add', 'monkey', '3000', '1')
        r.execute_command('ts.add', 'monkey', '3001', '1')
        r.execute_command('ts.add', 'monkey', '10000', '1')
        r.execute_command('ts.add', 'monkey', '10001', '1')
        r.execute_command('ts.add', 'monkey', '100000', '1')
        r.execute_command('ts.add', 'monkey', '100001', '1')
        r.execute_command('ts.add', 'monkey', '100002', '1')
        r.execute_command('ts.add', 'monkey', '100004', '1')
        r.execute_command('ts.add', 'monkey', '1000000', '1')
        r.execute_command('ts.add', 'monkey', '1000001', '1')
        r.execute_command('ts.add', 'monkey', '10000011000001', '1')
        r.execute_command('ts.add', 'monkey', '10000011000002', '1')
        expected_result = [[0, '1'], [1, '1'], [2, '1'], [50, '1'], [51, '1'],
                           [500, '1'], [501, '1'], [3000, '1'], [3001, '1'],
                           [10000, '1'], [10001, '1'], [100000, '1'], [100001, '1'],
                           [100002, '1'], [100004, '1'], [1000000, '1'], [1000001, '1'],
                           [10000011000001, '1'], [10000011000002, '1']]
        env.expect('TS.range', 'monkey', 0, '+', conn=r).equal(expected_result)


def test_ts_add_negative(env):
    with env.getClusterConnectionIfNeeded() as r:
        time.sleep(0.1)
        env.expect('TS.CREATE', 'tester', 'ENCODING', conn=r).raiseError()
        env.expect('TS.CREATE', 'tester', 'ENCODING', 'bad-encoding', conn=r).raiseError()
