from RLTest import Env
from test_helper_classes import ALLOWED_ERROR, _insert_data, _get_ts_info
from includes import *


def test_simple_dump_restore(env):
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'test_key', 'UNCOMPRESSED')
        r.execute_command('ts.add', 'test_key', 1, 1)
        dump = r.dump('test_key')
        r.execute_command('del', 'test_key')
        r.execute_command('restore', 'test_key', 0, dump)


def test_rdb(env):
    start_ts = 1511885909
    samples_count = 1500
    data = None
    key_name = 'tester{abc}'
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', key_name, 'RETENTION', '0', 'CHUNK_SIZE', '360',
                   'LABELS', 'name', 'brown', 'color', 'pink', conn=r).noError()
        env.expect('TS.CREATE', '{}_agg_avg_10'.format(key_name), conn=r).ok()
        env.expect('TS.CREATE', '{}_agg_max_10'.format(key_name), conn=r).noError()
        env.expect('TS.CREATE', '{}_agg_sum_10'.format(key_name), conn=r).noError()
        env.expect('TS.CREATE', '{}_agg_stds_10'.format(key_name), conn=r).noError()
        env.expect('TS.CREATERULE', key_name, '{}_agg_avg_10'.format(key_name), 'AGGREGATION', 'AVG', 10, conn=r).noError()
        env.expect('TS.CREATERULE', key_name, '{}_agg_max_10'.format(key_name), 'AGGREGATION', 'MAX', 10, conn=r).noError()
        env.expect('TS.CREATERULE', key_name, '{}_agg_sum_10'.format(key_name), 'AGGREGATION', 'SUM', 10, conn=r).noError()
        env.expect('TS.CREATERULE', key_name, '{}_agg_stds_10'.format(key_name), 'AGGREGATION', 'STD.S', 10, conn=r).noError()
        _insert_data(r, key_name, start_ts, samples_count, 5)

        data = r.dump(key_name)
        avg_data = r.dump('{}_agg_avg_10'.format(key_name))

        r.execute_command('DEL', key_name, '{}_agg_avg_10'.format(key_name))

        r.execute_command('RESTORE', key_name, 0, data)
        r.execute_command('RESTORE', '{}_agg_avg_10'.format(key_name), 0, avg_data)

        expected_result = [[start_ts + i, '5'] for i in range(samples_count)]
        actual_result = r.execute_command('TS.range', key_name, start_ts, start_ts + samples_count)
        env.assertEqual(expected_result, actual_result)
        actual_result = r.execute_command('TS.range', key_name, start_ts, start_ts + samples_count, 'count', 3)
        env.assertEqual(expected_result[:3], actual_result)

        env.assertEqual(_get_ts_info(r, key_name).rules, [[f'{key_name}_agg_avg_10', 10, 'AVG'],
                                                          [f'{key_name}_agg_max_10', 10, 'MAX'],
                                                          [f'{key_name}_agg_sum_10', 10, 'SUM'],
                                                          [f'{key_name}_agg_stds_10', 10, 'STD.S']])

        env.assertEqual(_get_ts_info(r, '{}_agg_avg_10'.format(key_name)).sourceKey, key_name)


def test_rdb_aggregation_context(env):
    """
    Check that the aggregation context of the rules is saved in rdb. Write data with not a full bucket,
    then save it and restore, add more data to the bucket and check the rules results considered the previous data
    that was in that bucket in their calculation. Check on avg and min, since all the other rules use the same
    context as min.
    """
    start_ts = 3
    samples_count = 4  # 1 full bucket and another one with 1 value
    key_name = 'tester{abc}'
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', key_name, conn=r).noError()
        env.expect('TS.CREATE', '{}_agg_avg_3'.format(key_name), conn=r).noError()
        env.expect('TS.CREATE', '{}_agg_min_3'.format(key_name), conn=r).noError()
        env.expect('TS.CREATE', '{}_agg_sum_3'.format(key_name), conn=r).noError()
        env.expect('TS.CREATE', '{}_agg_std_3'.format(key_name), conn=r).noError()
        env.expect('TS.CREATERULE', key_name, '{}_agg_avg_3'.format(key_name), 'AGGREGATION', 'AVG', 3, conn=r).noError()
        env.expect('TS.CREATERULE', key_name, '{}_agg_min_3'.format(key_name), 'AGGREGATION', 'MIN', 3, conn=r).noError()
        env.expect('TS.CREATERULE', key_name, '{}_agg_sum_3'.format(key_name), 'AGGREGATION', 'SUM', 3, conn=r).noError()
        env.expect('TS.CREATERULE', key_name, '{}_agg_std_3'.format(key_name), 'AGGREGATION', 'STD.S', 3, conn=r).noError()
        _insert_data(r, key_name, start_ts, samples_count, list(range(samples_count)))
        data_tester = r.dump(key_name)
        data_avg_tester = r.dump('{}_agg_avg_3'.format(key_name))
        data_min_tester = r.dump('{}_agg_min_3'.format(key_name))
        data_sum_tester = r.dump('{}_agg_sum_3'.format(key_name))
        data_std_tester = r.dump('{}_agg_std_3'.format(key_name))
        r.execute_command('DEL', key_name, f'{key_name}_agg_avg_3', f'{key_name}_agg_min_3',
                          f'{key_name}_agg_sum_3', f'{key_name}_agg_std_3')
        r.execute_command('RESTORE', key_name, 0, data_tester)
        r.execute_command('RESTORE', '{}_agg_avg_3'.format(key_name), 0, data_avg_tester)
        r.execute_command('RESTORE', '{}_agg_min_3'.format(key_name), 0, data_min_tester)
        r.execute_command('RESTORE', '{}_agg_sum_3'.format(key_name), 0, data_sum_tester)
        r.execute_command('RESTORE', '{}_agg_std_3'.format(key_name), 0, data_std_tester)
        env.expect('TS.ADD', key_name, start_ts + samples_count, samples_count, conn=r).noError()
        env.expect('TS.ADD', key_name, start_ts + samples_count + 10, 0, conn=r).noError()  # closes the last time_bucket
        # if the aggregation context wasn't saved, the results were considering only the new value added
        expected_result_avg = [[start_ts, '1'], [start_ts + 3, '3.5']]
        expected_result_min = [[start_ts, '0'], [start_ts + 3, '3']]
        expected_result_sum = [[start_ts, '3'], [start_ts + 3, '7']]
        expected_result_std = [[start_ts, '1'], [start_ts + 3, '0.7071']]
        env.expect('TS.range', '{}_agg_avg_3'.format(key_name), start_ts, start_ts + samples_count, conn=r).equal(expected_result_avg)
        env.expect('TS.range', '{}_agg_min_3'.format(key_name), start_ts, start_ts + samples_count, conn=r).equal(expected_result_min)
        env.expect('TS.range', '{}_agg_sum_3'.format(key_name), start_ts, start_ts + samples_count, conn=r).equal(expected_result_sum)
        actual_result_std = r.execute_command('TS.range', '{}_agg_std_3'.format(key_name), start_ts, start_ts + samples_count)
        env.assertEqual(actual_result_std[0], expected_result_std[0])
        env.assertTrue(abs(float(actual_result_std[1][1]) - float(expected_result_std[1][1])) < ALLOWED_ERROR)


def test_dump_trimmed_series(env):
    with env.getClusterConnectionIfNeeded() as r:
        samples = 120
        start_ts = 1589461305983
        r.execute_command('ts.create', 'test_key', 'RETENTION', 3000, 'CHUNK_SIZE', 160, 'UNCOMPRESSED')
        for i in range(1, samples):
            r.execute_command('ts.add', 'test_key', start_ts + i * 1000, i)
        env.expect('ts.range', 'test_key', '-', '+', conn=r).equal(
               [[1589461421983, '116'], [1589461422983, '117'], [1589461423983, '118'], [1589461424983, '119']])
        before = r.execute_command('ts.range', 'test_key', '-', '+')
        dump = r.dump('test_key')
        r.execute_command('del', 'test_key')
        r.execute_command('restore', 'test_key', 0, dump)
        env.expect('ts.range', 'test_key', '-', '+', conn=r).equal(before)


def test_empty_series(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'tester', conn=r).noError()
        agg_list = ['avg', 'sum', 'min', 'max', 'range', 'first', 'last',
                    'std.p', 'std.s', 'var.p', 'var.s']
        for agg in agg_list:
            env.expect('TS.range', 'tester', '-', '+', 'aggregation', agg, 1000, conn=r).equal([])
        env.assertNotEqual(r.dump('tester'), [])


def test_533_dump_rules(env):
    with env.getClusterConnectionIfNeeded() as r:
        key1 = 'ts1{a}'
        key2 = 'ts2{a}'
        r.execute_command('TS.CREATE', key1)
        r.execute_command('TS.CREATE', key2)
        r.execute_command('TS.CREATERULE', key1, key2, 'AGGREGATION', 'avg', 60000)

        env.assertEqual(_get_ts_info(r, key2).sourceKey, key1)
        env.assertEqual(len(_get_ts_info(r, key1).rules), 1)

        data = r.dump(key1)
        r.execute_command('DEL', key1)
        r.execute_command('restore', key1, 0, data)

        env.assertEqual(len(_get_ts_info(r, key1).rules), 1)
        env.assertEqual(_get_ts_info(r, key2).sourceKey, key1)
