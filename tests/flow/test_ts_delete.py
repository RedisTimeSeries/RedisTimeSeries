import pytest
import redis
from RLTest import Env


def test_ts_del_wrong():
    with Env().getConnection() as r:
        r.execute_command("ts.create", 'tester')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.DEL tester not_enough_args')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.DEL tester string -1')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.DEL tester 0 string')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.DEL nonexist 0 -1')


def test_ts_del_uncompressed():
    # total samples = 101
    sample_len = 101
    with Env().getConnection() as r:
        r.execute_command("ts.create", 'test_key', 'uncompressed')

        for i in range(sample_len):
            assert i == r.execute_command("ts.add", 'test_key', i, '1')

        res = r.execute_command('ts.range', 'test_key', 0, 100)
        i = 0
        for sample in res:
            assert sample == [i, '1'.encode('ascii')]
            i += 1
        r.execute_command('ts.del', 'test_key', 0, 100)
        res = r.execute_command('ts.range', 'test_key', 0, 100)
        assert len(res) == 0


def test_ts_del_uncompressed_in_range():
    sample_len = 101
    with Env().getConnection() as r:
        r.execute_command("ts.create", 'test_key', 'uncompressed')

        for i in range(sample_len):
            assert i == r.execute_command("ts.add", 'test_key', i, '1')

        res = r.execute_command('ts.range', 'test_key', 0, 100)
        i = 0
        for sample in res:
            assert sample == [i, '1'.encode('ascii')]
            i += 1
        # delete 11 samples
        r.execute_command('ts.del', 'test_key', 50, 60)
        res = r.execute_command('ts.range', 'test_key', 0, 100)
        assert len(res) == 90


def test_ts_del_compressed():
    sample_len = 101
    with Env().getConnection() as r:
        r.execute_command("ts.create", 'test_key')

        for i in range(sample_len):
            assert i == r.execute_command("ts.add", 'test_key', i, '1')

        res = r.execute_command('ts.range', 'test_key', 0, 100)
        i = 0
        for sample in res:
            assert sample == [i, '1'.encode('ascii')]
            i += 1
        r.execute_command('ts.del', 'test_key', 0, 100)
        res = r.execute_command('ts.range', 'test_key', 0, 100)
        assert len(res) == 0


def test_ts_del_compressed_multi_chunk():
    sample_len = 1001
    with Env().getConnection() as r:
        r.execute_command("ts.create", 'test_key')

        for i in range(sample_len):
            assert i == r.execute_command("ts.add", 'test_key', i, '1')

        res = r.execute_command('ts.range', 'test_key', 0, sample_len - 1)
        i = 0
        for sample in res:
            assert sample == [i, '1'.encode('ascii')]
            i += 1
        r.execute_command('ts.del', 'test_key', 0, 999)
        res = r.execute_command('ts.range', 'test_key', 0, sample_len - 1)
        assert len(res) == 1


def test_ts_del_compressed_out_range():
    sample_len = 101
    with Env().getConnection() as r:
        r.execute_command("ts.create", 'test_key')

        for i in range(sample_len):
            assert i + 100 == r.execute_command("ts.add", 'test_key', i + 100, '1')

        res = r.execute_command('ts.range', 'test_key', 0 + 100, sample_len + 100 - 1)
        i = 0
        for sample in res:
            assert sample == [i + 100, '1'.encode('ascii')]
            i += 1
        r.execute_command('ts.del', 'test_key', 0, 500)
        res = r.execute_command('ts.range', 'test_key', 0 + 100, sample_len + 100 - 1)
        assert len(res) == 0
