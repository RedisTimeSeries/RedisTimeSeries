import time

import pytest
import redis
from RLTest import Env
from test_helper_classes import _get_ts_info, TSInfo
from includes import *
import random 
import struct


def test_issue_504():
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'tester')
        for i in range(100, 3000):
            assert r.execute_command('ts.add', 'tester', i, i * 1.1) == i
        assert r.execute_command('ts.add', 'tester', 99, 1) == 99
        assert r.execute_command('ts.add', 'tester', 98, 1) == 98


def test_issue_588():
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'test1', "DUPLICATE_POLICY", "min")
        r.execute_command('ts.add', 'test1', 1, -0.05)
        assert float(r.execute_command('TS.RANGE', 'test1', "-", "+")[0][1]) == -0.05
        r.execute_command('ts.add', 'test1', 1, -0.06)
        assert float(r.execute_command('TS.RANGE', 'test1', "-", "+")[0][1]) == -0.06

        r.execute_command('ts.create', 'test2', "DUPLICATE_POLICY", "max")
        r.execute_command('ts.add', 'test2', 1, -0.06)
        assert float(r.execute_command('TS.RANGE', 'test2', "-", "+")[0][1]) == -0.06
        r.execute_command('ts.add', 'test2', 1, -0.05)
        assert float(r.execute_command('TS.RANGE', 'test2', "-", "+")[0][1]) == -0.05


def test_automatic_timestamp():
    with Env().getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 'tester')
        response_timestamp = r.execute_command('TS.ADD', 'tester', '*', 1)
        curr_time = int(time.time() * 1000)
        result = r.execute_command('TS.RANGE', 'tester', 0, curr_time)
        # test time difference is not more than 5 milliseconds
        assert result[0][0] - curr_time <= 5
        assert response_timestamp - curr_time <= 5


def test_add_create_key():
    with Env().getClusterConnectionIfNeeded() as r:
        ts = time.time()
        assert r.execute_command('TS.ADD', 'tester1', str(int(ts)), str(ts), 'RETENTION', '666', 'LABELS', 'name',
                                 'blabla') == int(ts)
        info = _get_ts_info(r, 'tester1')
        assert info.total_samples == 1
        assert info.retention_msecs == 666
        assert info.labels == {b'name': b'blabla'}

        assert r.execute_command('TS.ADD', 'tester2', str(int(ts)), str(ts), 'LABELS', 'name', 'blabla2', 'location',
                                 'earth')
        info = _get_ts_info(r, 'tester2')
        assert info.total_samples == 1
        assert info.labels == {b'location': b'earth', b'name': b'blabla2'}

def test_ts_add_encoding():
    for ENCODING in ['compressed','uncompressed']:
        e = Env()
        e.flush()
        with e.getClusterConnectionIfNeeded() as r:
            r.execute_command('ts.add', 't1', '*', '5.0', 'ENCODING', ENCODING)
            e.assertEqual(TSInfo(r.execute_command('TS.INFO', 't1')).chunk_type, ENCODING.encode())
            # backwards compatible check
            r.execute_command('ts.add', 't1_bc', '*', '5.0', ENCODING)
            e.assertEqual(TSInfo(r.execute_command('TS.INFO', 't1_bc')).chunk_type, ENCODING.encode())


def test_different_chunk_size():
    Env().skipOnCluster()
    with Env().getConnection() as r:
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command('TS.add', 'tester', "1636545188", "123", 'LABELS', 'id', 'abc1231232', 'CHUNK_SIZE', '0')
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command('TS.add', 'tester', "1636545188", "123", 'LABELS', 'id', 'abc1231232', 'CHUNK_SIZE', '-1000')
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command('TS.add', 'tester', "1636545188", "123", 'LABELS', 'id', 'abc1231232', 'CHUNK_SIZE', '100')
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command('TS.add', 'tester', "1636545188", "123", 'LABELS', 'id', 'abc1231232', 'CHUNK_SIZE', '127')
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command('TS.add', 'tester', "1636545188", "123", 'LABELS', 'id', 'abc1231232', 'CHUNK_SIZE', '40000000')

        r.execute_command('TS.add', 'tester3', "1636545188", "123", 'LABELS', 'id', 'abc1231232', 'CHUNK_SIZE', '128')

        r.execute_command('TS.add', 'tester2', "1636545188", "123", 'LABELS', 'id', 'abc1231232', 'CHUNK_SIZE', '40000')


def test_valid_labels():
    with Env().getClusterConnectionIfNeeded() as r:
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command('TS.CREATE', 'tester', 'LABELS', 'name', '')
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command('TS.ADD', 'tester2', '*', 1, 'LABELS', 'name', 'myName', 'location', '')
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command('TS.ADD', 'tester2', '*', 1, 'LABELS', 'name', 'myName', 'location', 'list)')
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command('TS.ADD', 'tester2', '*', 1, 'LABELS', 'name', 'myName', 'location', 'li(st')
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command('TS.ADD', 'tester2', '*', 1, 'LABELS', 'name', 'myName', 'location', 'lis,t')

def test_valid_timestamp():
    with Env().getConnection() as r:
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command('TS.ADD', 'timestamp', '12434fd', '34')
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command('TS.ADD', 'timestamp', '-34', '22')
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command('TS.ADD', 'timestamp', '*235', '45')

def test_gorilla():
    with Env().getClusterConnectionIfNeeded() as r:
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
        expected_result = [[0, b'1'], [1, b'1'], [2, b'1'], [50, b'1'], [51, b'1'],
                           [500, b'1'], [501, b'1'], [3000, b'1'], [3001, b'1'],
                           [10000, b'1'], [10001, b'1'], [100000, b'1'], [100001, b'1'],
                           [100002, b'1'], [100004, b'1'], [1000000, b'1'], [1000001, b'1'],
                           [10000011000001, b'1'], [10000011000002, b'1']]
        assert expected_result == r.execute_command('TS.range', 'monkey', 0, '+')


def test_ts_add_negative():
    with Env().getClusterConnectionIfNeeded() as r:
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command('TS.CREATE', 'tester', 'ENCODING')
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command('TS.CREATE', 'tester', 'ENCODING', 'bad-encoding')

def test_ts_upsert():
    if VALGRIND == 1:
        Env().skip()
    random.seed(5)
    with Env().getClusterConnectionIfNeeded() as r:
        DOUBLE_MAX = 1.7976931348623158E+308     # 64 bit floating point max value
        MAX_INT64 = pow(2,63) - 1
        key = 't1{1}'
        key2 = 't2{1}'
        r.execute_command('ts.create', key, 'CHUNK_SIZE', 128, 'DUPLICATE_POLICY', 'LAST')
        r.execute_command('ts.create', key2, 'CHUNK_SIZE', 128)
        j = 1
        while True:
            info = TSInfo(r.execute_command("ts.info", key, 'DEBUG'))
            if(len(info.chunks) > 1):
                break
            for i in range(1,10):
                r.execute_command('ts.add', key, MAX_INT64 - j, DOUBLE_MAX - j)
                r.execute_command('ts.add', key2, MAX_INT64 - j, DOUBLE_MAX - j)
                j += 1

        info = TSInfo(r.execute_command("ts.info", key, 'DEBUG'))
        n_samples_2_chunk = info.total_samples
        j = 1
        while True:
            info = TSInfo(r.execute_command("ts.info", key, 'DEBUG'))
            if(len(info.chunks) > 2):
                break
            for i in range(1,10):
                r.execute_command('ts.add', key, pow(j, 2), pow(j, 2))
                j += 1

        res = r.execute_command('ts.range', key, '-', pow((j-1), 2))
        expected = [[pow(d, 2), str(int(pow(d, 2))).encode('ascii')] for d in range(1, j)]
        assert res == expected
        info = TSInfo(r.execute_command("ts.info", key, 'DEBUG'))
        assert len(info.chunks) == 3
        total_samples = info.total_samples
        assert len(res) + n_samples_2_chunk == total_samples
        expected2 = expected.copy()
        random.shuffle(expected2)
        for i in range(0, len(expected2)):
            r.execute_command('ts.add', key2, expected2[i][0], expected2[i][1])
        res = r.execute_command('ts.range', key2, '-', pow((j-1), 2))
        assert res == expected

        for k in range (0, pow(j, 2) + 1):
            r.execute_command('ts.add', key, k, pow(k,3))

        res = r.execute_command('ts.range', key, '-', '+')
        info = TSInfo(r.execute_command("ts.info", key, 'DEBUG'))

def test_ts_upsert_downsampled():
    with Env().getClusterConnectionIfNeeded() as r:
        t1 = 't1{a}'
        t2 = 't2{a}'
        r.execute_command('TS.CREATE', t1)
        r.execute_command('TS.CREATE', t2)
        r.execute_command('TS.CREATERULE', t1, t2, 'AGGREGATION', 'max', 10)
        r.execute_command('ts.add', t1, 1, 2)
        r.execute_command('ts.add', t1, 3, 4)
        r.execute_command('ts.add', t2, 10, 6)
        r.execute_command('ts.add', t1, 11, 7)
        res = r.execute_command('TS.range', t2, '-', '+')
        assert res == [[0, b'4'], [10, b'6']]

        #override the downsampled key
        r.execute_command('ts.add', t1, 21, 9)
        res = r.execute_command('TS.range', t2, '-', '+')
        assert res == [[0, b'4'], [10, b'7']]


        t3 = 't3{a}'
        t4 = 't4{a}'
        r.execute_command('TS.CREATE', t3)
        r.execute_command('TS.CREATE', t4)
        r.execute_command('ts.add', t4, 10, 6)
        r.execute_command('ts.add', t4, 20, 9)
        r.execute_command('ts.add', t4, 23, 9)
        r.execute_command('TS.CREATERULE', t3, t4, 'AGGREGATION', 'max', 10)

        #override the downsampled key
        r.execute_command('ts.add', t3, 10, 22)
        r.execute_command('ts.add', t3, 25, 29)
        res = r.execute_command('TS.range', t4, '-', '+')
        assert res == [[10, b'22'], [20, b'9'], [23, b'9']]
        r.execute_command('ts.add', t3, 31, 3)
        res = r.execute_command('TS.range', t4, '-', '+')
        assert res == [[10, b'22'], [20, b'29'], [23, b'9']]

# End Chunk space when add the value, than try adding value which will fit in the old chunk
def test_ts_upsert_bug():
    with Env().getClusterConnectionIfNeeded() as r:
        t1 = 't1{1}'
        r.execute_command('ts.create', t1, 'CHUNK_SIZE', 64, 'DUPLICATE_POLICY', 'LAST')
        j = 1
        mantisa = list('1111111111111111111111111111111111111111100000000000')
        mantisa_len = len(mantisa)
        exp = list('00000000000')
        exp_len = len(exp)
        c = 0
        while True:
            val = int('0' + ''.join(exp) + ''.join(mantisa), 2)
            v = struct.unpack('d', struct.pack('Q', val))[0]
            r.execute_command('ts.add', t1, j, v)
            info = TSInfo(r.execute_command("ts.info", t1, 'DEBUG'))
            if(len(info.chunks) > 1):
                break
            if(j == 13):
                j += 100
            elif (j > 13):
                j += 500
            else:
                j += 2
            exp[exp_len - c - 1] = '1'
            mantisa[mantisa_len - exp_len + c] = '1'
            c += 1

        info = TSInfo(r.execute_command("ts.info", t1, 'DEBUG'))
        first_chunk_last_ts = info.chunks[0][3]
        res = r.execute_command("ts.del", t1, first_chunk_last_ts + 500, '+')
        info = TSInfo(r.execute_command("ts.info", t1, 'DEBUG'))
        first_chunk_last_ts = info.chunks[0][3]
        res = r.execute_command("ts.range", t1, first_chunk_last_ts, first_chunk_last_ts)
        first_chunk_last_val = res[0][1]
        r.execute_command('ts.add', t1, first_chunk_last_ts + 100, first_chunk_last_val)
        res2 = r.execute_command("ts.range", t1, first_chunk_last_ts, first_chunk_last_ts + 100)
        info2 = TSInfo(r.execute_command("ts.info", t1, 'DEBUG'))
        assert res2 == [res[0], [first_chunk_last_ts + 100, first_chunk_last_val]]
  