import pytest
import redis
from utils import Env
from test_helper_classes import _assert_alter_cmd, _ts_alter_cmd, _fill_data, _insert_data
from includes import *


def test_alter_cmd(env):
    start_ts = 1511885909
    samples_count = 1500
    end_ts = start_ts + samples_count
    key = 'tester'

    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', key, 'CHUNK_SIZE', '360',
                   'LABELS', 'name', 'brown', 'color', 'pink', conn=r).noError()
        _insert_data(r, key, start_ts, samples_count, 5)

        expected_data = [[start_ts + i, str(5)] for i in range(samples_count)]

        # test alter retention, chunk size and labels
        expected_labels = [['A', '1'], ['B', '2'], ['C', '3']]
        expected_retention = 500
        expected_chunk_size = 100
        _ts_alter_cmd(r, key, expected_retention, expected_chunk_size, expected_labels)
        _assert_alter_cmd(env, r, key, end_ts - 501, end_ts, expected_data[-501:],
                          expected_retention, expected_chunk_size, expected_labels)

        # test alter retention
        expected_retention = 200
        _ts_alter_cmd(r, key, set_retention=expected_retention)
        _assert_alter_cmd(env, r, key, end_ts - 201, end_ts, expected_data[-201:],
                          expected_retention, expected_chunk_size, expected_labels)

        # test alter chunk size
        expected_chunk_size = 100
        expected_labels = [['A', '1'], ['B', '2'], ['C', '3']]
        _ts_alter_cmd(r, key, set_chunk_size=expected_chunk_size)
        _assert_alter_cmd(env, r, key, end_ts - 201, end_ts, expected_data[-201:],
                          expected_retention, expected_chunk_size, expected_labels)

        # test alter labels
        expected_labels = [['A', '1']]
        _ts_alter_cmd(r, key, expected_retention, set_labels=expected_labels)
        _assert_alter_cmd(env, r, key, end_ts - 201, end_ts, expected_data[-201:],
                          expected_retention, expected_chunk_size, expected_labels)

        # test indexer was updated
        env.expect('TS.QUERYINDEX', 'A=1', conn=r).equal([key])
        env.expect('TS.QUERYINDEX', 'name=brown', conn=r).equal([])


def test_alter_key(env):
    with env.getClusterConnectionIfNeeded() as r:
        key = 'tester'
        r.execute_command('TS.CREATE', key)
        date_ranges = _fill_data(r, key)
        overrided_ts = date_ranges[0][0] + 10
        env.expect('TS.ADD', key, overrided_ts, 10, conn=r).raiseError()

        r.execute_command('TS.ALTER', key, 'DUPLICATE_POLICY', 'LAST')
        env.expect('TS.RANGE', key, overrided_ts, overrided_ts, conn=r).equal([[overrided_ts, str(overrided_ts)]])
        r.execute_command('TS.ADD', key, date_ranges[0][0] + 10, 10)
        env.expect('TS.RANGE', key, overrided_ts, overrided_ts, conn=r).equal([[overrided_ts, '10']])
