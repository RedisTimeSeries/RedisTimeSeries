import pytest
import redis
from utils import Env
from test_helper_classes import _assert_alter_cmd, _ts_alter_cmd, _fill_data, _insert_data


def test_alter_cmd():
    start_ts = 1511885909
    samples_count = 1500
    end_ts = start_ts + samples_count
    key = 'tester'

    with Env().getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', key, 'CHUNK_SIZE', '360',
                                 'LABELS', 'name', 'brown', 'color', 'pink')
        _insert_data(r, key, start_ts, samples_count, 5)

        expected_data = [[start_ts + i, str(5).encode('ascii')] for i in range(samples_count)]

        # test alter retention, chunk size and labels
        expected_labels = [[b'A', b'1'], [b'B', b'2'], [b'C', b'3']]
        expected_retention = 500
        expected_chunk_size = 100
        _ts_alter_cmd(r, key, expected_retention, expected_chunk_size, expected_labels)
        _assert_alter_cmd(r, key, end_ts - 501, end_ts, expected_data[-501:], expected_retention,
                          expected_chunk_size, expected_labels)

        # test alter retention
        expected_retention = 200
        _ts_alter_cmd(r, key, set_retention=expected_retention)
        _assert_alter_cmd(r, key, end_ts - 201, end_ts, expected_data[-201:], expected_retention,
                          expected_chunk_size, expected_labels)

        # test alter chunk size
        expected_chunk_size = 100
        expected_labels = [[b'A', b'1'], [b'B', b'2'], [b'C', b'3']]
        _ts_alter_cmd(r, key, set_chunk_size=expected_chunk_size)
        _assert_alter_cmd(r, key, end_ts - 201, end_ts, expected_data[-201:], expected_retention,
                          expected_chunk_size, expected_labels)

        # test alter labels
        expected_labels = [[b'A', b'1']]
        _ts_alter_cmd(r, key, expected_retention, set_labels=expected_labels)
        _assert_alter_cmd(r, key, end_ts - 201, end_ts, expected_data[-201:], expected_retention,
                          expected_chunk_size, expected_labels)

        # test indexer was updated
        assert r.execute_command('TS.QUERYINDEX', 'A=1') == [key.encode('ascii')]
        assert r.execute_command('TS.QUERYINDEX', 'name=brown') == []


def test_alter_key(self):
    with Env().getClusterConnectionIfNeeded() as r:
        key = 'tester'
        r.execute_command('TS.CREATE', key)
        date_ranges = _fill_data(r, key)
        overrided_ts = date_ranges[0][0] + 10
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.ADD', key, overrided_ts, 10)

        r.execute_command('TS.ALTER', key, 'DUPLICATE_POLICY', 'LAST')
        assert r.execute_command('TS.RANGE', key, overrided_ts, overrided_ts) == [
            [overrided_ts, str(overrided_ts).encode("ascii")]]
        r.execute_command('TS.ADD', key, date_ranges[0][0] + 10, 10)
        assert r.execute_command('TS.RANGE', key, overrided_ts, overrided_ts) == [[overrided_ts, b'10']]
