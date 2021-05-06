from RLTest import Env
from test_helper_classes import ALLOWED_ERROR, _insert_data, _get_ts_info



def test_simple_dump_restore(self):
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'test_key', 'UNCOMPRESSED')
        r.execute_command('ts.add', 'test_key', 1, 1)
        dump = r.execute_command('dump', 'test_key')
        r.execute_command('del', 'test_key')
        r.execute_command('restore', 'test_key', 0, dump)

def test_rdb():
    start_ts = 1511885909
    samples_count = 1500
    data = None
    key_name = 'tester{abc}'
    with Env().getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', key_name, 'RETENTION', '0', 'CHUNK_SIZE', '360', 'LABELS', 'name',
                                 'brown', 'color', 'pink')
        assert r.execute_command('TS.CREATE', '{}_agg_avg_10'.format(key_name))
        assert r.execute_command('TS.CREATE', '{}_agg_max_10'.format(key_name))
        assert r.execute_command('TS.CREATE', '{}_agg_sum_10'.format(key_name))
        assert r.execute_command('TS.CREATE', '{}_agg_stds_10'.format(key_name))
        assert r.execute_command('TS.CREATERULE', key_name, '{}_agg_avg_10'.format(key_name), 'AGGREGATION', 'AVG', 10)
        assert r.execute_command('TS.CREATERULE', key_name, '{}_agg_max_10'.format(key_name), 'AGGREGATION', 'MAX', 10)
        assert r.execute_command('TS.CREATERULE', key_name, '{}_agg_sum_10'.format(key_name), 'AGGREGATION', 'SUM', 10)
        assert r.execute_command('TS.CREATERULE', key_name, '{}_agg_stds_10'.format(key_name), 'AGGREGATION', 'STD.S', 10)
        _insert_data(r, key_name, start_ts, samples_count, 5)

        data = r.execute_command('DUMP', key_name)
        avg_data = r.execute_command('DUMP', '{}_agg_avg_10'.format(key_name))

        r.execute_command('DEL', key_name, '{}_agg_avg_10'.format(key_name))

        r.execute_command('RESTORE', key_name, 0, data)
        r.execute_command('RESTORE', '{}_agg_avg_10'.format(key_name), 0, avg_data)

        expected_result = [[start_ts + i, b'5'] for i in range(samples_count)]
        actual_result = r.execute_command('TS.range', key_name, start_ts, start_ts + samples_count)
        assert expected_result == actual_result
        actual_result = r.execute_command('TS.range', key_name, start_ts, start_ts + samples_count, 'count', 3)
        assert expected_result[:3] == actual_result

        assert _get_ts_info(r, key_name).rules == [[bytes('{}_agg_avg_10'.format(key_name), encoding="ascii"), 10, b'AVG'],
                                                   [bytes('{}_agg_max_10'.format(key_name), encoding="ascii"), 10, b'MAX'],
                                                   [bytes('{}_agg_sum_10'.format(key_name), encoding="ascii"), 10, b'SUM'],
                                                   [bytes('{}_agg_stds_10'.format(key_name), encoding="ascii"), 10, b'STD.S']]

        assert _get_ts_info(r, '{}_agg_avg_10'.format(key_name)).sourceKey == bytes(key_name, encoding="ascii")


def test_rdb_aggregation_context():
    """
    Check that the aggregation context of the rules is saved in rdb. Write data with not a full bucket,
    then save it and restore, add more data to the bucket and check the rules results considered the previous data
    that was in that bucket in their calculation. Check on avg and min, since all the other rules use the same
    context as min.
    """
    start_ts = 3
    samples_count = 4  # 1 full bucket and another one with 1 value
    key_name = 'tester{abc}'
    with Env().getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', key_name)
        assert r.execute_command('TS.CREATE', '{}_agg_avg_3'.format(key_name))
        assert r.execute_command('TS.CREATE', '{}_agg_min_3'.format(key_name))
        assert r.execute_command('TS.CREATE', '{}_agg_sum_3'.format(key_name))
        assert r.execute_command('TS.CREATE', '{}_agg_std_3'.format(key_name))
        assert r.execute_command('TS.CREATERULE', key_name, '{}_agg_avg_3'.format(key_name), 'AGGREGATION', 'AVG', 3)
        assert r.execute_command('TS.CREATERULE', key_name, '{}_agg_min_3'.format(key_name), 'AGGREGATION', 'MIN', 3)
        assert r.execute_command('TS.CREATERULE', key_name, '{}_agg_sum_3'.format(key_name), 'AGGREGATION', 'SUM', 3)
        assert r.execute_command('TS.CREATERULE', key_name, '{}_agg_std_3'.format(key_name), 'AGGREGATION', 'STD.S', 3)
        _insert_data(r, key_name, start_ts, samples_count, list(range(samples_count)))
        data_tester = r.execute_command('dump', key_name)
        data_avg_tester = r.execute_command('dump', '{}_agg_avg_3'.format(key_name))
        data_min_tester = r.execute_command('dump', '{}_agg_min_3'.format(key_name))
        data_sum_tester = r.execute_command('dump', '{}_agg_sum_3'.format(key_name))
        data_std_tester = r.execute_command('dump', '{}_agg_std_3'.format(key_name))
        r.execute_command('DEL',
                          key_name,
                          '{}_agg_avg_3'.format(key_name),
                          '{}_agg_min_3'.format(key_name),
                          '{}_agg_sum_3'.format(key_name),
                          '{}_agg_std_3'.format(key_name))
        r.execute_command('RESTORE', key_name, 0, data_tester)
        r.execute_command('RESTORE', '{}_agg_avg_3'.format(key_name), 0, data_avg_tester)
        r.execute_command('RESTORE', '{}_agg_min_3'.format(key_name), 0, data_min_tester)
        r.execute_command('RESTORE', '{}_agg_sum_3'.format(key_name), 0, data_sum_tester)
        r.execute_command('RESTORE', '{}_agg_std_3'.format(key_name), 0, data_std_tester)
        assert r.execute_command('TS.ADD', key_name, start_ts + samples_count, samples_count)
        assert r.execute_command('TS.ADD', key_name, start_ts + samples_count + 10, 0)  # closes the last time_bucket
        # if the aggregation context wasn't saved, the results were considering only the new value added
        expected_result_avg = [[start_ts, b'1'], [start_ts + 3, b'3.5']]
        expected_result_min = [[start_ts, b'0'], [start_ts + 3, b'3']]
        expected_result_sum = [[start_ts, b'3'], [start_ts + 3, b'7']]
        expected_result_std = [[start_ts, b'1'], [start_ts + 3, b'0.7071']]
        actual_result_avg = r.execute_command('TS.range', '{}_agg_avg_3'.format(key_name), start_ts, start_ts + samples_count)
        assert actual_result_avg == expected_result_avg
        actual_result_min = r.execute_command('TS.range', '{}_agg_min_3'.format(key_name), start_ts, start_ts + samples_count)
        assert actual_result_min == expected_result_min
        actual_result_sum = r.execute_command('TS.range', '{}_agg_sum_3'.format(key_name), start_ts, start_ts + samples_count)
        assert actual_result_sum == expected_result_sum
        actual_result_std = r.execute_command('TS.range', '{}_agg_std_3'.format(key_name), start_ts, start_ts + samples_count)
        assert actual_result_std[0] == expected_result_std[0]
        assert abs(float(actual_result_std[1][1]) - float(expected_result_std[1][1])) < ALLOWED_ERROR


def test_dump_trimmed_series(self):
    with Env().getClusterConnectionIfNeeded() as r:
        samples = 120
        start_ts = 1589461305983
        r.execute_command('ts.create', 'test_key', 'RETENTION', 3000, 'CHUNK_SIZE', 160, 'UNCOMPRESSED')
        for i in range(1, samples):
            r.execute_command('ts.add', 'test_key', start_ts + i * 1000, i)
        assert r.execute_command('ts.range', 'test_key', 0, -1) == \
               [[1589461421983, b'116'], [1589461422983, b'117'], [1589461423983, b'118'], [1589461424983, b'119']]
        before = r.execute_command('ts.range', 'test_key', '-', '+')
        dump = r.execute_command('dump', 'test_key')
        r.execute_command('del', 'test_key')
        r.execute_command('restore', 'test_key', 0, dump)
        assert r.execute_command('ts.range', 'test_key', '-', '+') == before


def test_empty_series():
    with Env().getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 'tester')
        agg_list = ['avg', 'sum', 'min', 'max', 'range', 'first', 'last',
                    'std.p', 'std.s', 'var.p', 'var.s']
        for agg in agg_list:
            assert [] == r.execute_command('TS.range', 'tester', 0, -1, 'aggregation', agg, 1000)
        assert r.execute_command('DUMP', 'tester')
