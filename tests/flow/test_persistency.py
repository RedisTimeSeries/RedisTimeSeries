from RLTest import Env
from test_helper_classes import ALLOWED_ERROR, _insert_data, _get_ts_info


def test_rdb():
    start_ts = 1511885909
    samples_count = 1500
    data = None
    with Env().getConnection() as r:
        assert r.execute_command('TS.CREATE', 'tester', 'RETENTION', '0', 'CHUNK_SIZE', '360', 'LABELS', 'name',
                                 'brown', 'color', 'pink')
        assert r.execute_command('TS.CREATE', 'tester_agg_avg_10')
        assert r.execute_command('TS.CREATE', 'tester_agg_max_10')
        assert r.execute_command('TS.CREATE', 'tester_agg_sum_10')
        assert r.execute_command('TS.CREATE', 'tester_agg_stds_10')
        assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_avg_10', 'AGGREGATION', 'AVG', 10)
        assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_max_10', 'AGGREGATION', 'MAX', 10)
        assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_sum_10', 'AGGREGATION', 'SUM', 10)
        assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_stds_10', 'AGGREGATION', 'STD.S', 10)
        _insert_data(r, 'tester', start_ts, samples_count, 5)

        data = r.execute_command('DUMP', 'tester')
        avg_data = r.execute_command('DUMP', 'tester_agg_avg_10')

        r.execute_command('DEL', 'tester', 'tester_agg_avg_10')

        r.execute_command('RESTORE', 'tester', 0, data)
        r.execute_command('RESTORE', 'tester_agg_avg_10', 0, avg_data)

        expected_result = [[start_ts + i, b'5'] for i in range(samples_count)]
        actual_result = r.execute_command('TS.range', 'tester', start_ts, start_ts + samples_count)
        assert expected_result == actual_result
        actual_result = r.execute_command('TS.range', 'tester', start_ts, start_ts + samples_count, 'count', 3)
        assert expected_result[:3] == actual_result

        assert _get_ts_info(r, 'tester').rules == [[b'tester_agg_avg_10', 10, b'AVG'],
                                                   [b'tester_agg_max_10', 10, b'MAX'],
                                                   [b'tester_agg_sum_10', 10, b'SUM'],
                                                   [b'tester_agg_stds_10', 10, b'STD.S']]

        assert _get_ts_info(r, 'tester_agg_avg_10').sourceKey == b'tester'


def test_rdb_aggregation_context():
    """
    Check that the aggregation context of the rules is saved in rdb. Write data with not a full bucket,
    then save it and restore, add more data to the bucket and check the rules results considered the previous data
    that was in that bucket in their calculation. Check on avg and min, since all the other rules use the same
    context as min.
    """
    start_ts = 3
    samples_count = 4  # 1 full bucket and another one with 1 value
    with Env().getConnection() as r:
        assert r.execute_command('TS.CREATE', 'tester')
        assert r.execute_command('TS.CREATE', 'tester_agg_avg_3')
        assert r.execute_command('TS.CREATE', 'tester_agg_min_3')
        assert r.execute_command('TS.CREATE', 'tester_agg_sum_3')
        assert r.execute_command('TS.CREATE', 'tester_agg_std_3')
        assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_avg_3', 'AGGREGATION', 'AVG', 3)
        assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_min_3', 'AGGREGATION', 'MIN', 3)
        assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_sum_3', 'AGGREGATION', 'SUM', 3)
        assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_std_3', 'AGGREGATION', 'STD.S', 3)
        _insert_data(r, 'tester', start_ts, samples_count, list(range(samples_count)))
        data_tester = r.execute_command('dump', 'tester')
        data_avg_tester = r.execute_command('dump', 'tester_agg_avg_3')
        data_min_tester = r.execute_command('dump', 'tester_agg_min_3')
        data_sum_tester = r.execute_command('dump', 'tester_agg_sum_3')
        data_std_tester = r.execute_command('dump', 'tester_agg_std_3')
        r.execute_command('DEL', 'tester', 'tester_agg_avg_3', 'tester_agg_min_3', 'tester_agg_sum_3',
                          'tester_agg_std_3')
        r.execute_command('RESTORE', 'tester', 0, data_tester)
        r.execute_command('RESTORE', 'tester_agg_avg_3', 0, data_avg_tester)
        r.execute_command('RESTORE', 'tester_agg_min_3', 0, data_min_tester)
        r.execute_command('RESTORE', 'tester_agg_sum_3', 0, data_sum_tester)
        r.execute_command('RESTORE', 'tester_agg_std_3', 0, data_std_tester)
        assert r.execute_command('TS.ADD', 'tester', start_ts + samples_count, samples_count)
        assert r.execute_command('TS.ADD', 'tester', start_ts + samples_count + 10, 0)  # closes the last time_bucket
        # if the aggregation context wasn't saved, the results were considering only the new value added
        expected_result_avg = [[start_ts, b'1'], [start_ts + 3, b'3.5']]
        expected_result_min = [[start_ts, b'0'], [start_ts + 3, b'3']]
        expected_result_sum = [[start_ts, b'3'], [start_ts + 3, b'7']]
        expected_result_std = [[start_ts, b'1'], [start_ts + 3, b'0.7071']]
        actual_result_avg = r.execute_command('TS.range', 'tester_agg_avg_3', start_ts, start_ts + samples_count)
        assert actual_result_avg == expected_result_avg
        actual_result_min = r.execute_command('TS.range', 'tester_agg_min_3', start_ts, start_ts + samples_count)
        assert actual_result_min == expected_result_min
        actual_result_sum = r.execute_command('TS.range', 'tester_agg_sum_3', start_ts, start_ts + samples_count)
        assert actual_result_sum == expected_result_sum
        actual_result_std = r.execute_command('TS.range', 'tester_agg_std_3', start_ts, start_ts + samples_count)
        assert actual_result_std[0] == expected_result_std[0]
        assert abs(float(actual_result_std[1][1]) - float(expected_result_std[1][1])) < ALLOWED_ERROR


def test_dump_trimmed_series(self):
    with Env().getConnection() as r:
        samples = 120
        start_ts = 1589461305983
        r.execute_command('ts.create test_key RETENTION 3000 CHUNK_SIZE 160 UNCOMPRESSED ')
        for i in range(1, samples):
            r.execute_command('ts.add test_key', start_ts + i * 1000, i)
        assert r.execute_command('ts.range test_key 0 -1') == \
               [[1589461421983, b'116'], [1589461422983, b'117'], [1589461423983, b'118'], [1589461424983, b'119']]
        before = r.execute_command('ts.range test_key - +')
        dump = r.execute_command('dump test_key')
        r.execute_command('del test_key')
        r.execute_command('restore test_key 0', dump)
        assert r.execute_command('ts.range test_key - +') == before


def test_empty_series():
    with Env().getConnection() as r:
        assert r.execute_command('TS.CREATE', 'tester')
        agg_list = ['avg', 'sum', 'min', 'max', 'range', 'first', 'last',
                    'std.p', 'std.s', 'var.p', 'var.s']
        for agg in agg_list:
            assert [] == r.execute_command('TS.range tester 0 -1 aggregation ' + agg + ' 1000')
        assert [[0, b'0']] == r.execute_command('TS.range tester 0 -1 aggregation count 1000')
        assert r.execute_command('DUMP', 'tester')
