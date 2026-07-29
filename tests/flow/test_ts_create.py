import math
import time

import pytest
import redis
from includes import Env
from test_helper_classes import SAMPLE_SIZE, _get_ts_info, TSInfo
from includes import *


def test_create_params():
    with Env().getClusterConnectionIfNeeded() as r:
        # test string instead of value
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.CREATE', 'invalid', 'RETENTION', 'retention')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.CREATE', 'invalid', 'CHUNK_SIZE', 'chunk_size')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.CREATE', 'invalid', 'ENCODING')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.CREATE', 'invalid', 'ENCODING', 'bad-encoding-type')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.CREATE', 'invalid', 'LABELS', 'key', 'val', 'RETENTION', 'abc')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.CREATE', 'invalid', 'LABELS', 'key', 'val', 'RETENTION', '-2')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.CREATE', 'invalid', 'LABELS', 'key', 'val', 'CHUNK_SIZE', 'abc')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.CREATE', 'invalid', 'LABELS', 'key', 'val', 'CHUNK_SIZE', '-2')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.CREATE', 'invalid', 'LABELS', 'key', 'val', 'CHUNK_SIZE', '4000000000')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.CREATE', 'invalid', 'LABELS', 'key', 'val', 'ENCODING', 'bad-encoding-type')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.CREATE', 'invalid', 'LABELS', 'key', 'val', 'DUPLICATE_POLICY', 'bla')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.CREATE', 'invalid', 'LABELS', 'key', 'val', 'label', 'DUPLICATE_POLICY', 'bla')

        # test for mem leak
        assert r.execute_command('TS.CREATE', 't1', 'LABELS', 'key', 'val')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.CREATE', 't1', 'LABELS', 'key', 'val')


        r.execute_command('TS.CREATE', 'a')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.CREATE', 'a')  # filter exists


def test_create_oversized_duplicate_policy():
    # An oversized DUPLICATE_POLICY must be rejected with a clean error, not crash
    # the shard: the argument length is client-controlled, so a huge value must not
    # reach an on-stack copy sized by it.
    with Env().getClusterConnectionIfNeeded() as r:
        huge_policy = 'A' * 9_000_000
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.CREATE', 'oversized', 'DUPLICATE_POLICY', huge_policy)
        # the shard must still be responsive
        assert r.ping()


def test_mixed_case_duplicate_policy_and_aggregation():
    # Both parsers match keywords case-insensitively. Assert the resulting behaviour,
    # not just that the command was accepted, so a mixed-case spelling resolving to the
    # wrong enum would still fail here.
    with Env().getClusterConnectionIfNeeded() as r:
        # 'LaSt' must behave as LAST: the newest sample wins
        assert r.execute_command('TS.CREATE', 'mixed_dp_last', 'DUPLICATE_POLICY', 'LaSt')
        assert r.execute_command('TS.ADD', 'mixed_dp_last', 1000, 1)
        assert r.execute_command('TS.ADD', 'mixed_dp_last', 1000, 2)
        assert r.execute_command('TS.RANGE', 'mixed_dp_last', 1000, 1000) == [[1000, b'2']]

        # 'MiN' must behave as MIN: the smallest value wins
        assert r.execute_command('TS.CREATE', 'mixed_dp_min', 'DUPLICATE_POLICY', 'MiN')
        assert r.execute_command('TS.ADD', 'mixed_dp_min', 1000, 5)
        assert r.execute_command('TS.ADD', 'mixed_dp_min', 1000, 3)
        assert r.execute_command('TS.RANGE', 'mixed_dp_min', 1000, 1000) == [[1000, b'3']]

        # 'bLoCk' must behave as BLOCK: the duplicate is rejected
        assert r.execute_command('TS.CREATE', 'mixed_dp_block', 'DUPLICATE_POLICY', 'bLoCk')
        assert r.execute_command('TS.ADD', 'mixed_dp_block', 1000, 1)
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.ADD', 'mixed_dp_block', 1000, 2)

        # mixed-case aggregation types must resolve to the right aggregator, including
        # a keyword containing a non-alphabetic character ('std.p')
        assert r.execute_command('TS.CREATE', 'mixed_agg')
        assert r.execute_command('TS.ADD', 'mixed_agg', 1000, 10)
        assert r.execute_command('TS.ADD', 'mixed_agg', 1001, 20)
        for agg_type, expected in [('AvG', b'15'), ('CoUnT', b'2'),
                                   ('StD.p', b'5'), ('CoUnTaLl', b'2')]:
            assert r.execute_command('TS.RANGE', 'mixed_agg', '-', '+',
                                     'AGGREGATION', agg_type, 100) == [[1000, expected]]


def test_oversized_aggregation_type():
    # Same guarantee for the aggregation-type parser, which shares the pattern:
    # an oversized AGGREGATION argument must be rejected, not crash the shard.
    with Env().getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 'agg_oversized')
        assert r.execute_command('TS.ADD', 'agg_oversized', 1000, 5)
        huge_agg = 'A' * 9_000_000
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.RANGE', 'agg_oversized', '-', '+', 'AGGREGATION', huge_agg, 100)
        # the shard must still be responsive
        assert r.ping()


def test_create_retention():
    with Env().getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 'tester', 'RETENTION', 1000)

        assert r.execute_command('TS.ADD', 'tester', 500, 10)
        expected_result = [[500, b'10']]
        actual_result = r.execute_command('TS.range', 'tester', '-', '+')
        assert expected_result == actual_result
        # check for (lastTimestamp - retension < 0)
        assert _get_ts_info(r, 'tester').total_samples == 1

        assert r.execute_command('TS.ADD', 'tester', 1001, 20)
        expected_result = [[500, b'10'], [1001, b'20']]
        actual_result = r.execute_command('TS.range', 'tester', '-', '+')
        assert expected_result == actual_result
        assert _get_ts_info(r, 'tester').total_samples == 2

        assert r.execute_command('TS.ADD', 'tester', 2000, 30)
        expected_result = [[1001, b'20'], [2000, b'30']]
        actual_result = r.execute_command('TS.range', 'tester', '-', '+')
        assert expected_result == actual_result
        assert _get_ts_info(r, 'tester').total_samples == 2

        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.CREATE', 'negative', 'RETENTION', -10)


def test_create_with_negative_chunk_size():
    with Env().getClusterConnectionIfNeeded() as r:
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.CREATE', 'tester', 'CHUNK_SIZE', -10)


def test_check_retention_64bit():
    with Env().getClusterConnectionIfNeeded() as r:
        huge_timestamp = 4000000000  # larger than uint32
        r.execute_command('TS.CREATE', 'tester', 'RETENTION', huge_timestamp)
        assert _get_ts_info(r, 'tester').retention_msecs == huge_timestamp
        for i in range(10):
            r.execute_command('TS.ADD', 'tester', int(huge_timestamp * i / 4), i)
        assert r.execute_command('TS.RANGE', 'tester', 0, "+") == \
               [[5000000000, b'5'], [6000000000, b'6'], [7000000000, b'7'],
                [8000000000, b'8'], [9000000000, b'9']]


def test_uncompressed():
    with Env().getClusterConnectionIfNeeded() as r:
        # test simple commands
        r.execute_command('ts.create', 'not_compressed', 'UNCOMPRESSED')
        assert 1 == r.execute_command('ts.add', 'not_compressed', 1, 3.5)
        assert 3.5 == float(r.execute_command('ts.get', 'not_compressed')[1])
        assert 2 == r.execute_command('ts.add', 'not_compressed', 2, 4.5)
        assert 3 == r.execute_command('ts.add', 'not_compressed', 3, 5.5)
        assert 5.5 == float(r.execute_command('ts.get', 'not_compressed')[1])
        assert [[1, b'3.5'], [2, b'4.5'], [3, b'5.5']] == \
               r.execute_command('ts.range', 'not_compressed', 0, '+')
        info = _get_ts_info(r, 'not_compressed')
        assert info.total_samples == 3 and info.memory_usage >= 4136

        # rdb load
        data = r.execute_command('dump', 'not_compressed')
        r.execute_command('del', 'not_compressed')

    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('RESTORE', 'not_compressed', 0, data)
        assert [[1, b'3.5'], [2, b'4.5'], [3, b'5.5']] == \
               r.execute_command('ts.range', 'not_compressed', 0, "+")
        info = _get_ts_info(r, 'not_compressed')
        assert info.total_samples == 3 and info.memory_usage >= 4136
        # test deletion
        assert r.delete('not_compressed')


def test_trim():
    with Env().getClusterConnectionIfNeeded() as r:
        for mode in ["UNCOMPRESSED", "COMPRESSED"]:
            samples = 2000
            chunk_size = 64 * SAMPLE_SIZE
            total_chunk_count = math.ceil(float(samples) / float(chunk_size) * SAMPLE_SIZE)
            r.execute_command('ts.create', 'trim_me', 'CHUNK_SIZE', chunk_size, 'RETENTION', 10, mode)
            r.execute_command('ts.create', 'dont_trim_me', 'CHUNK_SIZE', chunk_size, mode)
            for i in range(samples):
                r.execute_command('ts.add', 'trim_me', i, i * 1.1)
                r.execute_command('ts.add', 'dont_trim_me', i, i * 1.1)

            trimmed_info = _get_ts_info(r, 'trim_me')
            untrimmed_info = _get_ts_info(r, 'dont_trim_me')
            assert 2 == trimmed_info.chunk_count
            assert samples == untrimmed_info.total_samples
            # extra test for uncompressed
            if mode == "UNCOMPRESSED":
                assert 11 == trimmed_info.total_samples
                assert total_chunk_count == untrimmed_info.chunk_count

            r.delete("trim_me")
            r.delete("dont_trim_me")


def test_empty():
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'empty')
        info = _get_ts_info(r, 'empty')
        assert info.total_samples == 0
        assert [] == r.execute_command('TS.range', 'empty', 0, "+")
        assert [] == r.execute_command('TS.get', 'empty')

        r.execute_command('ts.create', 'empty_uncompressed', 'uncompressed')
        info = _get_ts_info(r, 'empty_uncompressed')
        assert info.total_samples == 0
        assert [] == r.execute_command('TS.range', 'empty_uncompressed', 0, "+")
        assert [] == r.execute_command('TS.get', 'empty')


def test_issue299():
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'issue299')
        for i in range(1000):
            r.execute_command('ts.add', 'issue299', i * 10, i)
        actual_result = r.execute_command('ts.range', 'issue299', 0, "+", 'aggregation', 'avg', 10)
        assert actual_result[0] == [0, b'0']
        actual_result = r.execute_command('ts.range', 'issue299', 0, "+", 'aggregation', 'avg', 100)
        assert actual_result[0] == [0, b'4.5']

        r.execute_command('del', 'issue299')
        r.execute_command('ts.create', 'issue299')
        for i in range(100, 1000):
            r.execute_command('ts.add', 'issue299', i * 10, i)
        actual_result = r.execute_command('ts.range', 'issue299', 0, "+", 'aggregation', 'avg', 10)
        assert actual_result[0] != [0, b'0']


def test_expire():
    Env().skipOnCluster()
    with Env().getConnection() as r:
        assert r.execute_command('ts.create', 'test') == b'OK'
        assert r.execute_command('keys', '*') == [b'test']
        assert r.execute_command('expire', 'test', 1) == 1
        time.sleep(2)
        assert r.execute_command('keys', '*') == []

def test_ts_create_encoding():
    for ENCODING in ['compressed','uncompressed']:
        e = Env()
        e.flush()
        with e.getClusterConnectionIfNeeded() as r:
            r.execute_command('ts.create', 't1', 'ENCODING', ENCODING)
            e.assertEqual(TSInfo(r.execute_command('TS.INFO', 't1')).chunk_type, ENCODING.encode())
            # backwards compatible check
            r.execute_command('ts.create', 't1_bc', ENCODING)
            e.assertEqual(TSInfo(r.execute_command('TS.INFO', 't1_bc')).chunk_type, ENCODING.encode())
