import math
import random
import statistics

# import pytest
import redis
from RLTest import Env
from test_helper_classes import _get_series_value, calc_rule, ALLOWED_ERROR, _insert_data, \
    _get_ts_info, _insert_agg_data
from includes import *


key_name = 'tester{abc}'
agg_key_name = '{}_agg_max_10'.format(key_name)

def test_compaction_rules(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', key_name, 'CHUNK_SIZE', '360', conn=r).noError()
        env.expect('TS.CREATE', agg_key_name, conn=r).noError()
        env.expect('TS.CREATERULE', key_name, agg_key_name, 'AGGREGATION', 'avg', -10, conn=r).error()
        env.expect('TS.CREATERULE', key_name, agg_key_name, 'AGGREGATION', 'avg', 0, conn=r).error()
        env.expect('TS.CREATERULE', key_name, agg_key_name, 'AGGREGATION', 'avg', 10, conn=r).noError()

        start_ts = 1488823384
        samples_count = 1500
        _insert_data(env, r, key_name, start_ts, samples_count, 5)
        last_ts = start_ts + samples_count + 10
        r.execute_command('TS.ADD', key_name, last_ts, 5)

        actual_result = r.execute_command('TS.RANGE', agg_key_name, start_ts, start_ts + samples_count)

        env.assertEqual(len(actual_result), samples_count / 10)

        info = _get_ts_info(r, key_name)
        env.assertEqual(info.rules, [[agg_key_name, 10, 'AVG']])


def test_create_compaction_rule_with_wrong_aggregation(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', key_name, conn=r).noError()
        env.expect('TS.CREATE', agg_key_name, conn=r).noError()
        env.expect('TS.CREATERULE', key_name, agg_key_name, 'AGGREGATION', 'MAXX', 10, conn=r).error()

        env.expect('TS.CREATERULE', key_name, agg_key_name, 'AGGREGATION', 'MA', 10, conn=r).error()


def test_create_compaction_rule_without_dest_series(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', key_name, conn=r).noError()
        env.expect('TS.CREATERULE', key_name, agg_key_name, 'AGGREGATION', 'MAX', 10, conn=r).error()


def test_create_compaction_rule_twice(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', key_name, conn=r).noError()
        env.expect('TS.CREATE', agg_key_name, conn=r).noError()
        env.expect('TS.CREATERULE', key_name, agg_key_name, 'AGGREGATION', 'MAX', 10, conn=r).noError()
        env.expect('TS.CREATERULE', key_name, agg_key_name, 'AGGREGATION', 'MAX', 10, conn=r).error()


def test_create_compaction_rule_override_dest(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', key_name, conn=r).noError()
        env.expect('TS.CREATE', 'tester2', conn=r).noError()
        env.expect('TS.CREATE', agg_key_name, conn=r).noError()
        env.expect('TS.CREATERULE', key_name, agg_key_name, 'AGGREGATION', 'MAX', 10, conn=r).noError()
        env.expect('TS.CREATERULE', 'tester2', agg_key_name, 'AGGREGATION', 'MAX', 10, conn=r).error()


def test_create_compaction_rule_from_target(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', key_name, conn=r).noError()
        env.expect('TS.CREATE', 'tester2', conn=r).noError()
        env.expect('TS.CREATE', agg_key_name, conn=r).noError()
        env.expect('TS.CREATERULE', key_name, agg_key_name, 'AGGREGATION', 'MAX', 10, conn=r).noError()
        env.expect('TS.CREATERULE', agg_key_name, 'tester2', 'AGGREGATION', 'MAX', 10, conn=r).error()


def test_create_compaction_rule_own(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', key_name, conn=r).noError()
        env.expect('TS.CREATERULE', key_name, key_name, 'AGGREGATION', 'MAX', 10, conn=r).error()


def test_create_compaction_rule_and_del_dest_series(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', key_name, conn=r).noError()
        env.expect('TS.CREATE', agg_key_name, conn=r).noError()
        env.expect('TS.CREATERULE', key_name, agg_key_name, 'AGGREGATION', 'AVG', 10, conn=r).noError()
        env.assertTrue(r.delete(agg_key_name))

        start_ts = 1488823384
        samples_count = 1500
        _insert_data(env, r, key_name, start_ts, samples_count, 5)


def test_std_var_func(env):
    with env.getClusterConnectionIfNeeded() as r:
        raw_key = 'raw{abc}'
        std_key = 'std_key{abc}'
        var_key = 'var_key{abc}'

        random_numbers = 100
        random.seed(0)
        items = random.sample(range(random_numbers), random_numbers)

        stdev = statistics.stdev(items)
        var = statistics.variance(items)
        env.expect('TS.CREATE', raw_key, conn=r).noError()
        env.expect('TS.CREATE', std_key, conn=r).noError()
        env.expect('TS.CREATE', var_key, conn=r).noError()
        env.expect('TS.CREATERULE', raw_key, std_key, "AGGREGATION", 'std.s', random_numbers, conn=r).noError()
        env.expect('TS.CREATERULE', raw_key, var_key, "AGGREGATION", 'var.s', random_numbers, conn=r).noError()

        for i in range(random_numbers):
            r.execute_command('TS.ADD', raw_key, i, items[i])
        r.execute_command('TS.ADD', raw_key, random_numbers, 0)  # close time bucket

        env.assertTrue(abs(stdev - float(r.execute_command('TS.GET', std_key)[1])) < ALLOWED_ERROR)
        env.assertTrue(abs(var - float(r.execute_command('TS.GET', var_key)[1])) < ALLOWED_ERROR)


def test_delete_key(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', key_name, 'CHUNK_SIZE', '360', conn=r).noError()
        env.expect('TS.CREATE', agg_key_name, conn=r).noError()
        env.expect('TS.CREATERULE', key_name, agg_key_name, 'AGGREGATION', 'avg', 10, conn=r).noError()
        env.assertTrue(r.delete(agg_key_name))
        env.assertEqual(_get_ts_info(r, key_name).rules, [])

        env.expect('TS.CREATE', agg_key_name, conn=r).noError()
        env.expect('TS.CREATERULE', key_name, agg_key_name, 'AGGREGATION', 'avg', 11, conn=r).noError()
        env.assertTrue(r.delete(key_name))
        env.assertEqual(_get_ts_info(r, agg_key_name).sourceKey, None)

        env.expect('TS.CREATE', key_name, conn=r).noError()
        env.expect('TS.CREATERULE', key_name, agg_key_name, 'AGGREGATION', 'avg', 12, conn=r).noError()
        env.assertEqual(_get_ts_info(r, key_name).rules, [[agg_key_name, 12, 'AVG']])


def test_downsampling_current(env):
    with env.getClusterConnectionIfNeeded() as r:
        key = 'src{a}'
        agg_key = 'dest{a}'
        type_list = ['', 'uncompressed']
        agg_list = ['avg', 'sum', 'min', 'max', 'count', 'range', 'first', 'last', 'std.p', 'std.s', 'var.p',
                    'var.s']  # more
        for chunk_type in type_list:
            for agg_type in agg_list:
                env.expect('TS.CREATE', key, chunk_type, "DUPLICATE_POLICY", "LAST", conn=r).noError()
                env.expect('TS.CREATE', agg_key, chunk_type, conn=r).noError()
                env.expect('TS.CREATERULE', key, agg_key, "AGGREGATION", agg_type, 10, conn=r).noError()

                # present update
                env.expect('TS.ADD', key, 3, 3, conn=r).equal(3)
                env.expect('TS.ADD', key, 5, 5, conn=r).equal(5)
                env.expect('TS.ADD', key, 7, 7, conn=r).equal(7)
                env.expect('TS.ADD', key, 5, 2, conn=r).equal(5)
                env.expect('TS.ADD', key, 10, 10, conn=r).equal(10)

                expected_result = r.execute_command('TS.RANGE', key, 0, '+', 'aggregation', agg_type, 10)
                actual_result = r.execute_command('TS.RANGE', agg_key, 0, '+')
                env.assertEqual(expected_result[0], actual_result[0])

                # present add
                env.expect('TS.ADD', key, 11, 11, conn=r).equal(11)
                env.expect('TS.ADD', key, 15, 15, conn=r).equal(15)
                env.expect('TS.ADD', key, 14, 14, conn=r).equal(14)
                env.expect('TS.ADD', key, 20, 20, conn=r).equal(20)

                expected_result = r.execute_command('TS.RANGE', key, 0, '+', 'aggregation', agg_type, 10)
                actual_result = r.execute_command('TS.RANGE', agg_key, 0, '+')
                env.assertEqual(expected_result[0:1], actual_result[0:1])

                # present + past add
                env.expect('TS.ADD', key, 23, 23, conn=r).equal(23)
                env.expect('TS.ADD', key, 15, 22, conn=r).equal(15)
                env.expect('TS.ADD', key, 27, 27, conn=r).equal(27)
                env.expect('TS.ADD', key, 23, 25, conn=r).equal(23)
                env.expect('TS.ADD', key, 30, 30, conn=r).equal(30)

                expected_result = r.execute_command('TS.RANGE', key, 0, '+', 'aggregation', agg_type, 10)
                actual_result = r.execute_command('TS.RANGE', agg_key, 0, '+')
                env.assertEqual(expected_result[0:3], actual_result[0:3])
                env.assertEqual(3, _get_ts_info(r, agg_key).total_samples)
                env.assertEqual(11, _get_ts_info(r, key).total_samples)

                r.execute_command('DEL', key)
                r.execute_command('DEL', agg_key)


def test_downsampling_extensive(env):
    with env.getClusterConnectionIfNeeded() as r:
        key = 'tester{abc}'
        fromTS = 10
        toTS = 10000
        type_list = ['', 'uncompressed']
        for chunk_type in type_list:
            agg_list = ['avg', 'sum', 'min', 'max', 'count', 'range', 'first', 'last', 'std.p', 'std.s', 'var.p',
                        'var.s']  # more
            for agg in agg_list:
                agg_key = _insert_agg_data(env, r, key, agg, chunk_type, fromTS, toTS,
                                           key_create_args=['DUPLICATE_POLICY', 'LAST'])

                # sanity + check result have changed
                expected_result1 = r.execute_command('TS.RANGE', key, fromTS, toTS, 'aggregation', agg, 10)
                actual_result1 = r.execute_command('TS.RANGE', agg_key, fromTS, toTS)
                env.assertEqual(expected_result1, actual_result1)
                env.assertEqual(len(expected_result1), 999)

                for i in range(fromTS + 5, toTS - 4, 10):
                    env.expect('TS.ADD', key, i, 42, conn=r).noError()

                expected_result2 = r.execute_command('TS.RANGE', key, fromTS, toTS, 'aggregation', agg, 10)
                actual_result2 = r.execute_command('TS.RANGE', agg_key, fromTS, toTS)
                env.assertEqual(expected_result2, actual_result2)

                # remove aggs with identical results
                compare_list = ['avg', 'sum', 'min', 'range', 'std.p', 'std.s', 'var.p', 'var.s']
                if agg in compare_list:
                    env.assertNotEqual(expected_result1, expected_result2)
                    env.assertNotEqual(actual_result1, actual_result2)

                r.execute_command('DEL', key)
                r.execute_command('DEL', agg_key)


def test_downsampling_rules(env):
    """
    Test downsmapling rules - avg,min,max,count,sum with 4 keys each.
    Downsample in resolution of:
    1sec (should be the same length as the original series),
    3sec (number of samples is divisible by 10),
    10s (number of samples is not divisible by 10),
    1000sec (series should be empty since there are not enough samples)
    Insert some data and check that the length, the values and the info of the downsample series are as expected.
    """
    with env.getClusterConnectionIfNeeded() as r:
        key = 'tester{abc}'
        env.expect('TS.CREATE', key, conn=r).noError()
        rules = ['avg', 'sum', 'count', 'max', 'min']
        resolutions = [1, 3, 10, 1000]
        for rule in rules:
            for resolution in resolutions:
                agg_key = '{}_{}_{}'.format(key, rule, resolution)
                env.expect('TS.CREATE', agg_key, conn=r).noError()
                env.expect('TS.CREATERULE', key, agg_key, 'AGGREGATION', rule, resolution, conn=r).noError()

        start_ts = 0
        samples_count = 501
        end_ts = start_ts + samples_count
        values = list(range(samples_count))
        _insert_data(env, r, key, start_ts, samples_count, values)
        r.execute_command('TS.ADD', key, 3000, 7.77)

        for rule in rules:
            for resolution in resolutions:
                actual_result = r.execute_command('TS.RANGE', '{}_{}_{}'.format(key, rule, resolution),
                                                  start_ts, end_ts)
                env.assertEqual(len(actual_result), math.ceil(samples_count / float(resolution)))
                expected_result = calc_rule(rule, values, resolution)
                env.assertEqual(_get_series_value(actual_result), expected_result)
                # last time stamp should be the beginning of the last bucket
                env.assertEqual(_get_ts_info(r, '{}_{}_{}'.format(key, rule, resolution)).last_time_stamp, (samples_count - 1) - (samples_count - 1) % resolution)

        # test for results after empty buckets
        r.execute_command('TS.ADD', key, 6000, 0)
        for rule in rules:
            for resolution in resolutions:
                actual_result = r.execute_command('TS.RANGE', '{}_{}_{}'.format(key, rule, resolution),
                                                  3000, 6000)
                env.assertEqual(len(actual_result), 1)
                env.assertEqual(_get_series_value(actual_result) == [7.77] or _get_series_value(actual_result), [1])


def test_backfill_downsampling(env):
    with env.getClusterConnectionIfNeeded() as r:
        key = 'tester{a}'
        type_list = ['', 'uncompressed']
        for chunk_type in type_list:
            agg_list = ['sum', 'min', 'max', 'count', 'first', 'last']  # more
            for agg in agg_list:
                agg_key = _insert_agg_data(env, r, key, agg, chunk_type, key_create_args=['DUPLICATE_POLICY', 'LAST'])

                expected_result = r.execute_command('TS.RANGE', key, 10, 50, 'aggregation', agg, 10)
                actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
                env.assertEqual(expected_result, actual_result)
                env.expect('TS.ADD', key, 15, 50, conn=r).equal(15)
                expected_result = r.execute_command('TS.RANGE', key, 10, 50, 'aggregation', agg, 10)
                actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
                env.assertEqual(expected_result, actual_result)

                # add in latest window
                r.execute_command('TS.ADD', key, 1055, 50) == 1055
                r.execute_command('TS.ADD', key, 1053, 55) == 1053
                r.execute_command('TS.ADD', key, 1062, 60) == 1062
                expected_result = r.execute_command('TS.RANGE', key, 10, 1060, 'aggregation', agg, 10)
                actual_result = r.execute_command('TS.RANGE', agg_key, 10, 1060)
                env.assertEqual(expected_result, actual_result)

                # update in latest window
                r.execute_command('TS.ADD', key, 1065, 65) == 1065
                r.execute_command('TS.ADD', key, 1066, 66) == 1066
                r.execute_command('TS.ADD', key, 1001, 42) == 1001
                r.execute_command('TS.ADD', key, 1075, 50) == 1075
                expected_result = r.execute_command('TS.RANGE', key, 10, 1070, 'aggregation', agg, 10)
                actual_result = r.execute_command('TS.RANGE', agg_key, 10, 1070)
                env.assertEqual(expected_result, actual_result)
                r.execute_command('DEL', key)
                r.execute_command('DEL', agg_key)


def test_rule_timebucket_64bit(env):
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        BELOW_32BIT_LIMIT = 2147483647
        ABOVE_32BIT_LIMIT = 2147483648
        r.execute_command("ts.create", 'test_key', 'RETENTION', ABOVE_32BIT_LIMIT)
        r.execute_command("ts.create", 'below_32bit_limit')
        r.execute_command("ts.create", 'above_32bit_limit')
        r.execute_command("ts.createrule", 'test_key', 'below_32bit_limit', 'AGGREGATION', 'max', BELOW_32BIT_LIMIT)
        r.execute_command("ts.createrule", 'test_key', 'above_32bit_limit', 'AGGREGATION', 'max', ABOVE_32BIT_LIMIT)
        info = _get_ts_info(r, 'test_key')
        env.assertEqual(info.rules[0][1], BELOW_32BIT_LIMIT)
        env.assertEqual(info.rules[1][1], ABOVE_32BIT_LIMIT)
