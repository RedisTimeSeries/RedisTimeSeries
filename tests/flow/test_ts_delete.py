from RLTest import Env
import pytest
import redis
from test_helper_classes import _get_ts_info

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
