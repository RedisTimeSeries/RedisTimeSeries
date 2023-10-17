from RLTest import Env
import pytest
import redis
from test_helper_classes import _get_ts_info, TSInfo
from includes import *
import random

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

#bug https://github.com/RedisTimeSeries/RedisTimeSeries/issues/1176
def test_ts_del_first_sample_in_chunk():
    _from = 1
    _to = 300
    del_from = 101
    del_to = 258
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 't1{1}', 'uncompressed', "DUPLICATE_POLICY", "LAST")
        for i in range(_from, _to + 1):
            r.execute_command("ts.add", 't1{1}', i, random.uniform(0, 1))
        range1 = r.execute_command("ts.range", 't1{1}', '-', '+')
        assert r.execute_command("ts.del", 't1{1}', del_from, del_to)
        add_str = []
        add_str.append("ts.madd")
        for i in range(_from, del_to + 1):
            add_str.append("t1{1}")
            add_str.append(str(i))
            add_str.append(str(random.uniform(0, 1)))
        assert r.execute_command(*add_str)
        range2 = r.execute_command("ts.range", 't1{1}', '-', '+')
        assert len(range2) == len(range1)

def test_ts_del_last_chunk():
    _from = 1
    _to = 500
    del_from = 101
    del_to = 500
    with Env().getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 't1{1}', 'compressed', 'CHUNK_SIZE', '128')
        for i in range(_from, _to + 1):
            r.execute_command("ts.add", 't1{1}', i, random.uniform(0, 1))
        info = TSInfo(r.execute_command("ts.info", 't1{1}', 'DEBUG'))
        n_chunks = len(info.chunks)
        assert(n_chunks > 1)
        assert r.execute_command("ts.del", 't1{1}', del_from, del_to)
        info = TSInfo(r.execute_command("ts.info", 't1{1}', 'DEBUG'))
        assert len(info.chunks) < n_chunks
        assert len(info.chunks) > 1
        res = r.execute_command("ts.get", 't1{1}')
        assert res[0] == del_from - 1
        res = r.execute_command("ts.range", 't1{1}', '-', '+')
        assert res[len(res) - 1][0] == del_from - 1
        assert r.execute_command("ts.del", 't1{1}', 0, del_from - 1)
        info = TSInfo(r.execute_command("ts.info", 't1{1}', 'DEBUG'))
        assert len(info.chunks) == 1
        r.execute_command("ts.add", 't1{1}', 5, random.uniform(0, 1))
        res = r.execute_command("ts.get", 't1{1}')
        assert res[0] == 5
        res = r.execute_command("ts.range", 't1{1}', '-', '+')
        assert len(res) == 1
        assert res[len(res) - 1][0] == 5

def test_ts_del_last_sample_in_series():
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 't1{1}', "DUPLICATE_POLICY", "LAST")
        assert r.execute_command("ts.get", 't1{1}') == []
        assert r.execute_command("ts.del", 't1{1}', '0', '10') == 0
        assert r.execute_command("ts.get", 't1{1}') == []
        r.execute_command("ts.add", 't1{1}', 1, 2)
        r.execute_command("ts.add", 't1{1}', 5, 10)
        assert r.execute_command("ts.del", 't1{1}', '5', '5')
        assert r.execute_command("ts.get", 't1{1}') == [1, b'2']
        assert r.execute_command("ts.del", 't1{1}', '1', '1')
        assert r.execute_command("ts.get", 't1{1}') == []

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

def test_ts_del_with_plus():
    e = Env()
    with e.getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 't{1}')
        r.execute_command("ts.add", 't{1}', 1, 2)
        r.execute_command("ts.add", 't{1}', 100, 10)
        r.execute_command("ts.del", 't{1}', '-', '+')
        res = r.execute_command("ts.range", 't{1}', '-', '+')
        assert res == []

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

        dump = r.execute_command("dump", "test_key")
        assert r.execute_command("restore", "test_key2", "0", dump)

        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("ts.del", "test_key", 100, '200a')

        assert r.execute_command("ts.del", "test_key", 200, 100) == 0

        assert r.execute_command("ts.del", "test_key", 100, 300) == 2

        assert r.execute_command("ts.del", "test_key2", 100, 300) == 2

        self.assertTrue(r.execute_command("SET", "BAD_X", "NOT_TS"))
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("TS.DEL", "BAD_X", 100, 200)

def test_del_retention_with_rules(self):
    sample_len = 1010
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 'test_key_2{1}', 'RETENTION', 1, 'compressed')
        r.execute_command("ts.create", 'test_key_3{1}', 'compressed')
        r.execute_command('ts.createrule', 'test_key_2{1}', 'test_key_3{1}', 'AGGREGATION', 'avg', 10)

        for i in range(sample_len):
            assert i == r.execute_command("ts.add", 'test_key_2{1}', i, 1)

        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("ts.del", "test_key_2{1}", 1, 10)

        r.execute_command("ts.create", 'test_key_4{4}', 'RETENTION', 25, 'compressed')
        r.execute_command("ts.create", 'test_key_5{4}', 'compressed')
        r.execute_command('ts.createrule', 'test_key_4{4}', 'test_key_5{4}', 'AGGREGATION', 'avg', 10)

        for i in range(30):
            assert i == r.execute_command("ts.add", 'test_key_4{4}', i, 1)

        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("ts.del", "test_key_4{4}", 9, 10)


def test_del_with_rules(self):
    sample_len = 1010
    e = Env()
    with e.getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 'test_key_2{1}', 'RETENTION', 5000, 'compressed')
        r.execute_command("ts.create", 'test_key_3{1}', 'compressed')
        r.execute_command('ts.createrule', 'test_key_2{1}', 'test_key_3{1}', 'AGGREGATION', 'sum', 10)

        for i in range(70):
            assert i == r.execute_command("ts.add", 'test_key_2{1}', i, 1)
        for i in range(80, sample_len):
            assert i == r.execute_command("ts.add", 'test_key_2{1}', i, 1)

        res = r.execute_command('ts.range', 'test_key_3{1}', 0, 9)
        e.assertEqual(len(res), 1)
        assert res[0] == [0, b'10']
        assert r.execute_command("ts.del", "test_key_2{1}", 0, 9) == 10
        res = r.execute_command('ts.range', 'test_key_3{1}', 0, 9)
        e.assertEqual(len(res), 0)

        res = r.execute_command('ts.range', 'test_key_3{1}', 10, 19)
        e.assertEqual(len(res), 1)
        assert res[0] == [10, b'10']
        assert r.execute_command("ts.del", "test_key_2{1}", 12, 14) == 3
        res = r.execute_command('ts.range', 'test_key_3{1}', 10, 19)
        e.assertEqual(len(res), 1)
        assert res[0] == [10, b'7']

        res = r.execute_command('ts.range', 'test_key_3{1}', 20, 39)
        e.assertEqual(len(res), 2)
        assert res == [[20, b'10'], [30, b'10']]
        assert r.execute_command("ts.del", "test_key_2{1}", 28, 31) == 4
        res = r.execute_command('ts.range', 'test_key_3{1}', 20, 39)
        e.assertEqual(len(res), 2)
        assert res == [[20, b'8'], [30, b'8']]

        res = r.execute_command('ts.range', 'test_key_3{1}', 50, 89)
        e.assertEqual(len(res), 3)
        assert res == [[50, b'10'], [60, b'10'], [80, b'10']]
        # Tests empty end bucket which is not the lastest bucket:
        assert r.execute_command("ts.del", "test_key_2{1}", 58, 79) == 12
        assert r.execute_command("ts.del", "test_key_2{1}", 80, 81) == 2
        res = r.execute_command('ts.range', 'test_key_3{1}', 50, 89)
        e.assertEqual(len(res), 2)
        assert res == [[50, b'8'], [80, b'8']]

        res = r.execute_command('ts.range', 'test_key_3{1}', 990, 1009)
        e.assertEqual(len(res), 1)
        assert res[0] == [990, b'10']
        assert r.execute_command("ts.del", "test_key_2{1}", 995, 1002) == 8
        res = r.execute_command('ts.range', 'test_key_3{1}', 990, 1009)
        e.assertEqual(len(res), 1)
        assert res == [[990, b'5']]
        assert 1010 == r.execute_command("ts.add", 'test_key_2{1}', 1010, 1)
        res = r.execute_command('ts.range', 'test_key_3{1}', 990, 1009)
        e.assertEqual(len(res), 2)
        assert res == [[990, b'5'], [1000, b'7']]

        ##### delete chunk of a rule #####
        r.execute_command("ts.create", 'test_key_{4}', 'RETENTION', 5000, 'CHUNK_SIZE', '1024', 'compressed')
        r.execute_command("ts.create", 'test_key_{4}_agg', 'CHUNK_SIZE', '1024', 'compressed')
        r.execute_command('ts.createrule', 'test_key_{4}', 'test_key_{4}_agg', 'AGGREGATION', 'sum', 10)

        for i in range(2070):
            assert i == r.execute_command("ts.add", 'test_key_{4}', i, 1)

        res = r.execute_command('ts.range', 'test_key_{4}_agg', 1010, 2059)
        e.assertEqual(len(res), 105)
        assert r.execute_command("ts.del", "test_key_{4}", 1019, 2050) == 1032
        res = r.execute_command('ts.range', 'test_key_{4}_agg', 1010, 2059)
        e.assertEqual(len(res), 2)
        assert res == [[1010, b'9'], [2050, b'9']]



def test_del_with_rules_with_alignment(self):
    sample_len = 1005
    e = Env()
    with e.getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 'test_key_2{1}', 'RETENTION', 5000, 'compressed')
        r.execute_command("ts.create", 'test_key_3{1}', 'compressed')
        r.execute_command('ts.createrule', 'test_key_2{1}', 'test_key_3{1}', 'AGGREGATION', 'sum', 10, 5)

        for i in range(65):
            assert i == r.execute_command("ts.add", 'test_key_2{1}', i, 1)
        for i in range(75, sample_len):
            assert i == r.execute_command("ts.add", 'test_key_2{1}', i, 1)

        res = r.execute_command('ts.range', 'test_key_3{1}', 0, 4)
        e.assertEqual(len(res), 1)
        assert res[0] == [0, b'5']
        assert r.execute_command("ts.del", "test_key_2{1}", 0, 4) == 5
        res = r.execute_command('ts.range', 'test_key_3{1}', 0, 4)
        e.assertEqual(len(res), 0)

        res = r.execute_command('ts.range', 'test_key_3{1}', 5, 14)
        e.assertEqual(len(res), 1)
        assert res[0] == [5, b'10']
        assert r.execute_command("ts.del", "test_key_2{1}", 11, 13) == 3
        res = r.execute_command('ts.range', 'test_key_3{1}', 5, 14)
        e.assertEqual(len(res), 1)
        assert res[0] == [5, b'7']

        res = r.execute_command('ts.range', 'test_key_3{1}', 15, 34)
        e.assertEqual(len(res), 2)
        assert res == [[15, b'10'], [25, b'10']]
        assert r.execute_command("ts.del", "test_key_2{1}", 23, 26) == 4
        res = r.execute_command('ts.range', 'test_key_3{1}', 15, 34)
        e.assertEqual(len(res), 2)
        assert res == [[15, b'8'], [25, b'8']]

        res = r.execute_command('ts.range', 'test_key_3{1}', 45, 84)
        e.assertEqual(len(res), 3)
        assert res == [[45, b'10'], [55, b'10'], [75, b'10']]
        # Tests empty end bucket which is not the lastest bucket:
        assert r.execute_command("ts.del", "test_key_2{1}", 53, 74) == 12
        assert r.execute_command("ts.del", "test_key_2{1}", 75, 76) == 2
        res = r.execute_command('ts.range', 'test_key_3{1}', 45, 84)
        e.assertEqual(len(res), 2)
        assert res == [[45, b'8'], [75, b'8']]

        res = r.execute_command('ts.range', 'test_key_3{1}', 985, 1004)
        e.assertEqual(len(res), 1)
        assert res[0] == [985, b'10']
        assert r.execute_command("ts.del", "test_key_2{1}", 990, 997) == 8
        res = r.execute_command('ts.range', 'test_key_3{1}', 985, 1004)
        e.assertEqual(len(res), 1)
        assert res == [[985, b'5']]
        assert 1005 == r.execute_command("ts.add", 'test_key_2{1}', 1005, 1)
        res = r.execute_command('ts.range', 'test_key_3{1}', 985, 1004)
        e.assertEqual(len(res), 2)
        assert res == [[985, b'5'], [995, b'7']]

        ##### delete chunk of a rule #####
        r.execute_command("ts.create", 'test_key_{4}', 'RETENTION', 5000, 'CHUNK_SIZE', '1024', 'compressed')
        r.execute_command("ts.create", 'test_key_{4}_agg', 'CHUNK_SIZE', '1024', 'compressed')
        r.execute_command('ts.createrule', 'test_key_{4}', 'test_key_{4}_agg', 'AGGREGATION', 'sum', 10, 5)

        for i in range(2070):
            assert i == r.execute_command("ts.add", 'test_key_{4}', i, 1)

        res = r.execute_command('ts.range', 'test_key_{4}_agg', 1005, 2054)
        e.assertEqual(len(res), 105)
        assert r.execute_command("ts.del", "test_key_{4}", 1014, 2045) == 1032
        res = r.execute_command('ts.range', 'test_key_{4}_agg', 1005, 2054)
        e.assertEqual(len(res), 2)
        assert res == [[1005, b'9'], [2045, b'9']]

def test_del_outside_retention_period(self):
    e = Env()
    t1 = 't1{1}'
    t2 = 't2{1}'
    t3 = 't3{1}'
    t4 = 't4{1}'
    t5 = 't5{1}'
    t6 = 't6{1}'
    with e.getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", t1, 'RETENTION', 20)
        r.execute_command("ts.create", t2, 'RETENTION', 10)
        r.execute_command('ts.createrule', t1, t2, 'AGGREGATION', 'sum', 10)

        r.execute_command("ts.add", t1, 13, 13)
        r.execute_command("ts.add", t1, 17, 17)
        r.execute_command("ts.add", t1, 22, 22)
        r.execute_command("ts.add", t1, 31, 31)


        try:
            res = r.execute_command("ts.del", t1, 5, 25)
            assert False
        except Exception as e:
            assert str(e) == "TSDB: When a series has compactions, deleting samples or compaction buckets beyond the series retention period is not possible"

        try:
            r.execute_command("ts.del", t1, 5, 25)
            assert False
        except Exception as e:
            assert str(e) == "TSDB: When a series has compactions, deleting samples or compaction buckets beyond the series retention period is not possible"


        r.execute_command("ts.create", t3, 'RETENTION', 15)
        r.execute_command("ts.create", t4, 'RETENTION', 10)
        r.execute_command('ts.createrule', t3, t4, 'AGGREGATION', 'sum', 10)

        r.execute_command("ts.add", t3, 13, 13)
        r.execute_command("ts.add", t3, 19, 19)
        r.execute_command("ts.add", t3, 22, 22)
        r.execute_command("ts.add", t3, 30, 30)
        try:
            r.execute_command("ts.del", t3, 19, 25)
            assert False
        except Exception as e:
            assert str(e) == "TSDB: When a series has compactions, deleting samples or compaction buckets beyond the series retention period is not possible"

        r.execute_command("ts.create", t5)
        r.execute_command("ts.create", t6)
        r.execute_command('ts.createrule', t5, t6, 'AGGREGATION', 'sum', 10)

        r.execute_command("ts.add", t5, 13, 13)
        r.execute_command("ts.add", t5, 19, 19)
        r.execute_command("ts.add", t5, 22, 22)
        r.execute_command("ts.add", t5, 30, 30)
        assert r.execute_command("ts.del", t5, 5, 30) == 4
        assert r.execute_command("ts.range", t6, 5, 30) == []

# bug: https://redislabs.atlassian.net/browse/MOD-4972
# https://github.com/RedisTimeSeries/RedisTimeSeries/issues/1421
def test_del_with_rules_bug_4972(self):
    e = Env()
    with e.getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 't1{1}', 'compressed', 'RETENTION', 0)
        r.execute_command("ts.create", 't2{1}', 'compressed', 'RETENTION', 0)
        r.execute_command('ts.createrule', 't1{1}', 't2{1}', 'AGGREGATION', 'avg', 10)

        r.execute_command("ts.add", 't1{1}', 1, 1)
        r.execute_command("ts.add", 't1{1}', 3, 3)
        r.execute_command("ts.add", 't1{1}', 11, 11)
        r.execute_command("ts.del", 't1{1}', 10, 13)
        res = r.execute_command('ts.range', 't1{1}', 0, 100)
        assert res == [[1, b'1'], [3, b'3']]
        res = r.execute_command('ts.range', 't2{1}', 0, 100)
        assert res == []
        r.execute_command("ts.add", 't1{1}', 22, 22)

        res = r.execute_command('ts.range', 't1{1}', 0, 100)
        assert res == [[1, b'1'], [3, b'3'], [22, b'22']]
        res = r.execute_command('ts.range', 't2{1}', 0, 100)
        assert res == [[0, b'2']]

        r.execute_command("ts.add", 't1{1}', 24, 24)
        r.execute_command("ts.add", 't1{1}', 30, 30)
        r.execute_command("ts.add", 't1{1}', 40, 40)
        res = r.execute_command('ts.range', 't1{1}', 0, 100)
        assert res == [[1, b'1'], [3, b'3'], [22, b'22'], [24, b'24'], [30, b'30'], [40, b'40']]
        res = r.execute_command('ts.range', 't2{1}', 0, 100)
        assert res == [[0, b'2'], [20, b'23'], [30, b'30']]

# bug: https://redislabs.atlassian.net/browse/MOD-4972
# https://github.com/RedisTimeSeries/RedisTimeSeries/issues/1421
def test_del_with_rules_bug_4972_2(self):
    e = Env()
    with e.getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 't1{1}', 'compressed', 'RETENTION', 0)
        r.execute_command("ts.create", 't2{1}', 'compressed', 'RETENTION', 0)
        r.execute_command('ts.createrule', 't1{1}', 't2{1}', 'AGGREGATION', 'avg', 10)

        r.execute_command("ts.add", 't1{1}', 1, 1)
        r.execute_command("ts.add", 't1{1}', 3, 3)
        r.execute_command("ts.del", 't1{1}', 0, 8)
        res = r.execute_command('ts.range', 't1{1}', 0, 100)
        assert res == []
        res = r.execute_command('ts.range', 't2{1}', 0, 100)
        assert res == []
        r.execute_command("ts.add", 't1{1}', 11, 11)
        r.execute_command("ts.add", 't1{1}', 13, 13)
        r.execute_command("ts.add", 't1{1}', 24, 24)

        res = r.execute_command('ts.range', 't1{1}', 0, 100)
        assert res == [[11, b'11'], [13, b'13'], [24, b'24']]
        res = r.execute_command('ts.range', 't2{1}', 0, 100)
        assert res == [[10, b'12']]

        r.execute_command("ts.add", 't1{1}', 30, 30)
        r.execute_command("ts.add", 't1{1}', 40, 40)
        res = r.execute_command('ts.range', 't1{1}', 0, 100)
        assert res == [[11, b'11'], [13, b'13'], [24, b'24'], [30, b'30'], [40, b'40']]
        res = r.execute_command('ts.range', 't2{1}', 0, 100)
        assert res == [[10, b'12'], [20, b'24'], [30, b'30']]

# bug: https://redislabs.atlassian.net/browse/MOD-4972
# https://github.com/RedisTimeSeries/RedisTimeSeries/issues/1421
def test_del_with_rules_bug_4972_3(self):
    e = Env()
    with e.getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 't1{1}', 'compressed', 'RETENTION', 0)
        r.execute_command("ts.create", 't2{1}', 'compressed', 'RETENTION', 0)
        r.execute_command('ts.createrule', 't1{1}', 't2{1}', 'AGGREGATION', 'avg', 10)

        r.execute_command("ts.add", 't1{1}', 1, 1)
        r.execute_command("ts.add", 't1{1}', 3, 3)
        r.execute_command("ts.add", 't1{1}', 11, 11)
        r.execute_command("ts.del", 't1{1}', 2, 13)
        res = r.execute_command('ts.range', 't1{1}', 0, 100)
        assert res == [[1, b'1']]
        res = r.execute_command('ts.range', 't2{1}', 0, 100)
        assert res == []
        r.execute_command("ts.add", 't1{1}', 22, 22)

        res = r.execute_command('ts.range', 't1{1}', 0, 100)
        assert res == [[1, b'1'], [22, b'22']]
        res = r.execute_command('ts.range', 't2{1}', 0, 100)
        assert res == [[0, b'1']]

        r.execute_command("ts.add", 't1{1}', 24, 24)
        r.execute_command("ts.add", 't1{1}', 30, 30)
        r.execute_command("ts.add", 't1{1}', 40, 40)
        res = r.execute_command('ts.range', 't1{1}', 0, 100)
        assert res == [[1, b'1'], [22, b'22'], [24, b'24'], [30, b'30'], [40, b'40']]
        res = r.execute_command('ts.range', 't2{1}', 0, 100)
        assert res == [[0, b'1'], [20, b'23'], [30, b'30']]

# bug: https://redislabs.atlassian.net/browse/MOD-4972
# https://github.com/RedisTimeSeries/RedisTimeSeries/issues/1421
def test_del_with_rules_bug_4972_4(self):
    e = Env()
    with e.getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 't1{1}', 'compressed', 'RETENTION', 0)
        r.execute_command("ts.create", 't2{1}', 'compressed', 'RETENTION', 0)
        r.execute_command('ts.createrule', 't1{1}', 't2{1}', 'AGGREGATION', 'avg', 10)

        r.execute_command("ts.add", 't1{1}', 1, 1)
        r.execute_command("ts.add", 't1{1}', 3, 3)
        r.execute_command("ts.add", 't1{1}', 21, 21)
        r.execute_command("ts.del", 't1{1}', 21, 21)
        r.execute_command("ts.add", 't1{1}', 8, 8)
        r.execute_command("ts.add", 't1{1}', 13, 13)

        res = r.execute_command('ts.range', 't1{1}', 0, 100)
        assert res == [[1, b'1'], [3, b'3'], [8, b'8'], [13, b'13']]
        res = r.execute_command('ts.range', 't2{1}', 0, 100)
        assert res == [[0, b'4']]

def test_del_with_rules_bug_4972_5(self):
    e = Env(moduleArgs='DONT_ASSERT_ON_FAILIURE enable')
    t1 = 't1{1}'
    t2 = 't2{1}'
    with e.getClusterConnectionIfNeeded() as r:
        is_less_than_ver7_0_5 = is_redis_version_smaller_than(r, "7.0.5", e.is_cluster())
        if is_less_than_ver7_0_5:
            return
        # data was taken from test_dump_restore_dst_rule on version TS_OVERFLOW_RDB_VER
        # the commands that used to create the data are:
        # 127.0.0.1:6379> ts.create t1{1}
        # OK
        # 127.0.0.1:6379> ts.create t2{1}
        # OK
        # 127.0.0.1:6379> ts.createrule t1{1} t2{1} AGGREGATION avg 10
        # OK
        # 127.0.0.1:6379> ts.add t1{1} 5 5
        # (integer) 5
        # 127.0.0.1:6379> ts.add t1{1} 21 21
        # (integer) 21
        # 127.0.0.1:6379> ts.del t1{1} 21 21
        # (integer) 1
        data = b'\a\x81M \xc1\xf96\x0f\x10\a\x05\x05t1{1}\x02\x80\x00\x0fB@\x02P\x00\x02\x02\x02\x05\x04\x00\x00\x00\x00\x00\x00\x14@\x02\x01\x02\x00\x02\x00\x02\x00\x02\x01\x05\x05t2{1}\x02\n\x02\x00\x02\x04\x02\x14\x04\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x02\x01\x02P\x00\x02\x01\x02\x00\x02\x81@\x14\x00\x00\x00\x00\x00\x00\x02\x05\x02\x05\x02\x00\x02\x81@\x14\x00\x00\x00\x00\x00\x00\x02 \x02 \x05\xc36P\x00\x01\x00\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0{\x00\x01\x00\x00\x00\n\x00~\x12a\xa1V\xb1ep'
        r.execute_command('restore', t1, 0, data)
        data = b'\a\x81M \xc1\xf96\x0f\x10\a\x05\x05t2{1}\x02\x80\x00\x0fB@\x02P\x00\x02\x02\x02\x00\x04\x00\x00\x00\x00\x00\x00\x14@\x02\x01\x02\x00\x02\x01\x05\x05t1{1}\x02\x00\x02\x00\x02\x01\x02P\x00\x02\x01\x02\x00\x02\x81@\x14\x00\x00\x00\x00\x00\x00\x02\x00\x02\x00\x02\x00\x02\x81@\x14\x00\x00\x00\x00\x00\x00\x02 \x02 \x05\xc36P\x00\x01\x00\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0{\x00\x01\x00\x00\x00\n\x00}\xbfP[\xc1A\x84t'
        r.execute_command('restore', t2, 0, data)
        r.execute_command('ts.add', t1, 31, 31)
        res = r.execute_command('ts.range', t1, '-', '+')
        assert res == [[5, b'5'], [31, b'31']]
        r.execute_command('ts.range', t2, '-', '+')
        assert res == [[5, b'5'], [31, b'31']]

def test_del_with_rules_bug_4972_6(self):
    e = Env(moduleArgs='DONT_ASSERT_ON_FAILIURE enable')
    t1 = 't1{1}'
    t2 = 't2{1}'
    with e.getClusterConnectionIfNeeded() as r:
        is_less_than_ver7_0_5 = is_redis_version_smaller_than(r, "7.0.5", e.is_cluster())
        if is_less_than_ver7_0_5:
            return
        # data was taken from test_dump_restore_dst_rule on version TS_OVERFLOW_RDB_VER
        # the commands that used to create the data are:
        # 127.0.0.1:6379> ts.create t1{1}
        # OK
        # 127.0.0.1:6379> ts.create t2{1}
        # OK
        # 127.0.0.1:6379> ts.createrule t1{1} t2{1} AGGREGATION avg 10
        # OK
        # 127.0.0.1:6379> ts.add t1{1} 5 5
        # (integer) 5
        # 127.0.0.1:6379> ts.add t1{1} 21 21
        # (integer) 21
        # 127.0.0.1:6379> ts.del t1{1} 21 21
        # (integer) 1
        data = b'\a\x81M \xc1\xf96\x0f\x10\a\x05\x05t1{1}\x02\x80\x00\x0fB@\x02P\x00\x02\x02\x02\x05\x04\x00\x00\x00\x00\x00\x00\x14@\x02\x01\x02\x00\x02\x00\x02\x00\x02\x01\x05\x05t2{1}\x02\n\x02\x00\x02\x04\x02\x14\x04\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x02\x01\x02P\x00\x02\x01\x02\x00\x02\x81@\x14\x00\x00\x00\x00\x00\x00\x02\x05\x02\x05\x02\x00\x02\x81@\x14\x00\x00\x00\x00\x00\x00\x02 \x02 \x05\xc36P\x00\x01\x00\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0{\x00\x01\x00\x00\x00\n\x00~\x12a\xa1V\xb1ep'
        r.execute_command('restore', t1, 0, data)
        data = b'\a\x81M \xc1\xf96\x0f\x10\a\x05\x05t2{1}\x02\x80\x00\x0fB@\x02P\x00\x02\x02\x02\x00\x04\x00\x00\x00\x00\x00\x00\x14@\x02\x01\x02\x00\x02\x01\x05\x05t1{1}\x02\x00\x02\x00\x02\x01\x02P\x00\x02\x01\x02\x00\x02\x81@\x14\x00\x00\x00\x00\x00\x00\x02\x00\x02\x00\x02\x00\x02\x81@\x14\x00\x00\x00\x00\x00\x00\x02 \x02 \x05\xc36P\x00\x01\x00\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0{\x00\x01\x00\x00\x00\n\x00}\xbfP[\xc1A\x84t'
        r.execute_command('restore', t2, 0, data)
        r.execute_command('ts.add', t1, 7, 7)
        r.execute_command('ts.add', t1, 19, 19)
        res = r.execute_command('ts.range', t1, '-', '+')
        assert res == [[5, b'5'], [7, b'7'], [19, b'19']]
        res = r.execute_command('ts.range', t2, '-', '+')
