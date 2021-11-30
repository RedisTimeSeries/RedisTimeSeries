from RLTest import Env
import pytest
import redis
from test_helper_classes import _get_ts_info
from includes import *


def test_ts_del_uncompressed():
    # total samples = 101
    sample_len = 101
    with Env().getClusterConnectionIfNeeded() as r:
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
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 'test_key', 'uncompressed')

        for i in range(sample_len):
            assert i == r.execute_command("ts.add", 'test_key', i, '1')

        res = r.execute_command('ts.range', 'test_key', 0, 100)
        i = 0
        for sample in res:
            assert sample == [i, '1'.encode('ascii')]
            i += 1
        # delete 11 samples
        assert 11 == r.execute_command('ts.del', 'test_key', 50, 60)
        res = r.execute_command('ts.range', 'test_key', 0, 100)
        assert len(res) == 90


def test_ts_del_compressed():
    sample_len = 101
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 'test_key')

        for i in range(sample_len):
            assert i == r.execute_command("ts.add", 'test_key', i, '1')

        res = r.execute_command('ts.range', 'test_key', 0, 100)
        i = 0
        for sample in res:
            assert sample == [i, '1'.encode('ascii')]
            i += 1
        assert sample_len == r.execute_command('ts.del', 'test_key', 0, 100)
        res = r.execute_command('ts.range', 'test_key', 0, 100)
        assert len(res) == 0


def test_ts_del_multi_chunk():
    for CHUNK_TYPE in ["compressed","uncompressed"]:
        sample_len = 1
        e = Env()
        with e.getClusterConnectionIfNeeded() as r:
            r.execute_command("ts.create", 'test_key', CHUNK_TYPE)
            while(_get_ts_info(r, 'test_key').chunk_count<2):
                assert sample_len == r.execute_command("ts.add", 'test_key', sample_len, '1')
                sample_len = sample_len + 1
            sample_len = sample_len -1
            res = r.execute_command('ts.range', 'test_key', 0, sample_len - 1)
            i = 1
            for sample in res:
                e.assertEqual(sample, [i, '1'.encode('ascii')])
                i += 1
            assert sample_len - 1 == r.execute_command('ts.del', 'test_key', 0, sample_len - 1)
            res = r.execute_command('ts.range', 'test_key', 0, sample_len)
            e.assertEqual(_get_ts_info(r, 'test_key').chunk_count,1)
            e.assertEqual(len(res), 1)
        e.flush()


def test_ts_del_compressed_out_range():
    sample_len = 101
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 'test_key')

        for i in range(sample_len):
            assert i + 100 == r.execute_command("ts.add", 'test_key', i + 100, '1')

        res = r.execute_command('ts.range', 'test_key', 0 + 100, sample_len + 100 - 1)
        i = 0
        for sample in res:
            assert sample == [i + 100, '1'.encode('ascii')]
            i += 1
        assert sample_len == r.execute_command('ts.del', 'test_key', 0, 500)
        res = r.execute_command('ts.range', 'test_key', 0 + 100, sample_len + 100 - 1)
        assert len(res) == 0


def test_bad_del(self):
    with Env().getClusterConnectionIfNeeded() as r:

        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("ts.del", "test_key", 100, 200)

        r.execute_command("ts.add", 'test_key', 120, '1')
        r.execute_command("ts.add", 'test_key', 140, '5')
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("ts.del", "test_key", 100)

        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("ts.del", "test_key", 100, '200a')

        assert r.execute_command("ts.del", "test_key", 200, 100) == 0

        assert r.execute_command("ts.del", "test_key", 100, 300) == 2

        self.assertTrue(r.execute_command("SET", "BAD_X", "NOT_TS"))
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("TS.DEL", "BAD_X", 100, 200)

def test_del_retention_with_rules(self):
    sample_len = 1010
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 'test_key_2', 'RETENTION', 1, 'compressed')
        r.execute_command("ts.create", 'test_key_3', 'compressed')
        r.execute_command('ts.createrule', 'test_key_2', 'test_key_3', 'AGGREGATION', 'avg', 10)

        for i in range(sample_len):
            assert i == r.execute_command("ts.add", 'test_key_2', i, 1)

        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("ts.del", "test_key_2", 1, 10)

        r.execute_command("ts.create", 'test_key_4', 'RETENTION', 25, 'compressed')
        r.execute_command("ts.create", 'test_key_5', 'compressed')
        r.execute_command('ts.createrule', 'test_key_4', 'test_key_5', 'AGGREGATION', 'avg', 10)

        for i in range(30):
            assert i == r.execute_command("ts.add", 'test_key_4', i, 1)

        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("ts.del", "test_key_4", 9, 10)


def test_del_with_rules(self):
    sample_len = 1010
    e = Env()
    with e.getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 'test_key_2', 'RETENTION', 5000, 'compressed')
        r.execute_command("ts.create", 'test_key_3', 'compressed')
        r.execute_command('ts.createrule', 'test_key_2', 'test_key_3', 'AGGREGATION', 'sum', 10)

        for i in range(70):
            assert i == r.execute_command("ts.add", 'test_key_2', i, 1)
        for i in range(80, sample_len):
            assert i == r.execute_command("ts.add", 'test_key_2', i, 1)

        res = r.execute_command('ts.range', 'test_key_3', 0, 9)
        e.assertEqual(len(res), 1)
        assert res[0] == [0, b'10']
        assert r.execute_command("ts.del", "test_key_2", 0, 9) == 10
        res = r.execute_command('ts.range', 'test_key_3', 0, 9)
        e.assertEqual(len(res), 0)

        res = r.execute_command('ts.range', 'test_key_3', 10, 19)
        e.assertEqual(len(res), 1)
        assert res[0] == [10, b'10']
        assert r.execute_command("ts.del", "test_key_2", 12, 14) == 3
        res = r.execute_command('ts.range', 'test_key_3', 10, 19)
        e.assertEqual(len(res), 1)
        assert res[0] == [10, b'7']

        res = r.execute_command('ts.range', 'test_key_3', 20, 39)
        e.assertEqual(len(res), 2)
        assert res == [[20, b'10'], [30, b'10']]
        assert r.execute_command("ts.del", "test_key_2", 28, 31) == 4
        res = r.execute_command('ts.range', 'test_key_3', 20, 39)
        e.assertEqual(len(res), 2)
        assert res == [[20, b'8'], [30, b'8']]

        res = r.execute_command('ts.range', 'test_key_3', 50, 89)
        e.assertEqual(len(res), 3)
        assert res == [[50, b'10'], [60, b'10'], [80, b'10']]
        # Tests empty end bucket which is not the lastest bucket:
        assert r.execute_command("ts.del", "test_key_2", 58, 79) == 12
        assert r.execute_command("ts.del", "test_key_2", 80, 81) == 2
        res = r.execute_command('ts.range', 'test_key_3', 50, 89)
        e.assertEqual(len(res), 2)
        assert res == [[50, b'8'], [80, b'8']]

        res = r.execute_command('ts.range', 'test_key_3', 990, 1009)
        e.assertEqual(len(res), 1)
        assert res[0] == [990, b'10']
        assert r.execute_command("ts.del", "test_key_2", 995, 1002) == 8
        res = r.execute_command('ts.range', 'test_key_3', 990, 1009)
        e.assertEqual(len(res), 1)
        assert res == [[990, b'5']]
        assert 1010 == r.execute_command("ts.add", 'test_key_2', 1010, 1)
        res = r.execute_command('ts.range', 'test_key_3', 990, 1009)
        e.assertEqual(len(res), 2)
        assert res == [[990, b'5'], [1000, b'7']]

        ##### delete chunk of a rule #####
        r.execute_command("ts.create", 'test_key_4', 'RETENTION', 5000, 'CHUNK_SIZE', '1024', 'compressed')
        r.execute_command("ts.create", 'test_key_5', 'CHUNK_SIZE', '1024', 'compressed')
        r.execute_command('ts.createrule', 'test_key_4', 'test_key_5', 'AGGREGATION', 'sum', 10)

        for i in range(2070):
            assert i == r.execute_command("ts.add", 'test_key_4', i, 1)

        res = r.execute_command('ts.range', 'test_key_5', 1010, 2059)
        e.assertEqual(len(res), 105)
        assert r.execute_command("ts.del", "test_key_4", 1019, 2050) == 1032
        res = r.execute_command('ts.range', 'test_key_5', 1010, 2059)
        e.assertEqual(len(res), 2)
        assert res == [[1010, b'9'], [2050, b'9']]

