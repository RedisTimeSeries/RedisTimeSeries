import math

# import pytest
import redis
from RLTest import Env
from test_helper_classes import TSInfo, ALLOWED_ERROR, _insert_data, _get_ts_info, \
    _insert_agg_data
from includes import *


def test_range_query(env):
    start_ts = 1488823384
    samples_count = 1500
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'tester', 'uncompressed', 'RETENTION', samples_count - 100, conn=r).noError()
        _insert_data(env, r, 'tester', start_ts, samples_count, 5)

        expected_result = [[start_ts + i, str(5)] for i in range(99, 151)]
        actual_result = r.execute_command('TS.range', 'tester', start_ts + 50, start_ts + 150)
        env.assertEqual(expected_result, actual_result)
        rev_result = r.execute_command('TS.revrange', 'tester', start_ts + 50, start_ts + 150)
        rev_result.reverse()
        env.assertEqual(expected_result, rev_result)

        # test out of range returns empty list
        env.expect('TS.range', 'tester', int(start_ts * 2), '+', conn=r).equal([])
        env.expect('TS.range', 'tester', int(start_ts / 3), int(start_ts / 2), conn=r).equal([])

        env.expect('TS.revrange', 'tester', int(start_ts * 2), '+', conn=r).equal([])
        env.expect('TS.revrange', 'tester', int(start_ts / 3), int(start_ts / 2), conn=r).equal([])

        env.expect('TS.RANGE', 'tester', 'string', '+', conn=r).error()
        env.expect('TS.RANGE', 'tester', 0, 'string', conn=r).error()
        env.expect('TS.RANGE', 'tester', 0, -1, conn=r).error()
        env.expect('TS.RANGE', 'tester', -1, 1000, conn=r).error()
        env.expect('TS.RANGE', 'nonexist', 0, '+', conn=r).error()
        env.expect('TS.RANGE', 'tester', 0, '+', '', 'aggregation', conn=r).error()
        env.expect('TS.RANGE', 'tester', 0, '+', 'count', 'number', conn=r).error()
        env.expect('TS.RANGE', 'tester', 0, '+', 'count', conn=r).error()
        env.expect('TS.RANGE', 'tester', 0, '+', 'aggregation', 'count', 'number', conn=r).error()
        env.expect('TS.RANGE', 'tester', 0, '+', 'aggregation', 'count', conn=r).error()
        env.expect('TS.RANGE', 'tester', 0, '+', 'aggregation', '', conn=r).error()
        env.expect('TS.RANGE', 'tester', 0, '+', 'aggregation', 'not_aggregation_function', conn=r).error()
        env.expect('TS.RANGE', 'tester', 0, '+', 'aggregation', '', conn=r).error()
        env.expect('TS.RANGE', 'tester', '-', '+', 'FILTER_BY_VALUE', conn=r).error()
        env.expect('TS.RANGE', 'tester', '-', '+', 'FILTER_BY_TS', conn=r).error()
        env.expect('TS.RANGE', 'tester', '-', '+', 'FILTER_BY_TS', 'FILTER_BY_VALUE', conn=r).error()
        env.expect('TS.RANGE', 'tester', '-', '+', 'FILTER_BY_VALUE', 'FILTER_BY_TS', conn=r).error()
        env.expect('TS.RANGE', 'tester', '-', '+', 'FILTER_BY_VALUE', 10, 'FILTER_BY_TS', conn=r).error()

def test_range_midrange(env):
    samples_count = 5000
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'tester', 'UNCOMPRESSED', conn=r).noError()
        for i in range(samples_count):
            r.execute_command('TS.ADD', 'tester', i, i)
        # sample_count is not in range() so not included
        env.expect('TS.RANGE', 'tester', samples_count - 500, samples_count, conn=r).apply(len).equal(500)
        env.expect('TS.RANGE', 'tester', samples_count - 1500, samples_count - 1000, conn=r).apply(len).equal(501)

        # test for empty range between two full ranges
        for i in range(samples_count):
            r.execute_command('TS.ADD', 'tester', samples_count * 2 + i, i)
        env.expect('TS.RANGE', 'tester', int(samples_count * 1.1), int(samples_count + 1.2), conn=r).equal([])


def test_range_with_agg_query(env):
    start_ts = 1488823384
    samples_count = 1500
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'tester', conn=r).noError()
        _insert_data(env, r, 'tester', start_ts, samples_count, 5)

        expected_result = [[1488823000, '116'], [1488823500, '500'], [1488824000, '500'], [1488824500, '384']]
        env.expect('TS.range', 'tester', start_ts, start_ts + samples_count, 'AGGREGATION',
                   'count', 500, conn=r).equal(expected_result)

        # test first aggregation is not [0,0] if out of range
        expected_result = [[1488823000, '116'], [1488823500, '500']]
        env.expect('TS.range', 'tester', 1488822000, 1488823999, 'AGGREGATION',
                   'count', 500, conn=r).equal(expected_result)

        env.expect('TS.range', 'tester', start_ts, start_ts + samples_count, 'AGGREGATION', 'count', -1, conn=r).error()


def test_agg_std_p(env):
    with env.getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(env, r, 'tester{a}', 'std.p')

        expected_result = [[10, '25.869'], [20, '25.869'], [30, '25.869'], [40, '25.869']]
        actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
        for i in range(len(expected_result)):
            env.assertTrue(abs(float(expected_result[i][1]) - float(actual_result[i][1])) < ALLOWED_ERROR)


def test_agg_std_s(env):
    with env.getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(env, r, 'tester{a}', 'std.s')

        expected_result = [[10, '27.269'], [20, '27.269'], [30, '27.269'], [40, '27.269']]
        actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
        for i in range(len(expected_result)):
            env.assertTrue(abs(float(expected_result[i][1]) - float(actual_result[i][1])) < ALLOWED_ERROR)


def test_agg_var_p(env):
    with env.getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(env, r, 'tester{a}', 'var.p')

        expected_result = [[10, '669.25'], [20, '669.25'], [30, '669.25'], [40, '669.25']]
        actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
        for i in range(len(expected_result)):
            env.assertTrue(abs(float(expected_result[i][1]) - float(actual_result[i][1])) < ALLOWED_ERROR)


def test_agg_var_s(env):
    with env.getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(env, r, 'tester{a}', 'var.s')

        expected_result = [[10, '743.611'], [20, '743.611'], [30, '743.611'], [40, '743.611']]
        actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
        for i in range(len(expected_result)):
            env.assertTrue(abs(float(expected_result[i][1]) - float(actual_result[i][1])) < ALLOWED_ERROR)


def test_agg_sum(env):
    with env.getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(env, r, 'tester{a}', 'sum')

        expected_result = [[10, '1565'], [20, '2565'], [30, '3565'], [40, '4565']]
        env.expect('TS.RANGE', agg_key, 10, 50, conn=r).equal(expected_result)


def test_agg_count(env):
    with env.getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(env, r, 'tester{a}', 'count')

        expected_result = [[10, '10'], [20, '10'], [30, '10'], [40, '10']]
        env.expect('TS.RANGE', agg_key, 10, 50, conn=r).equal(expected_result)


def test_agg_first(env):
    with env.getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(env, r, 'tester{a}', 'first')

        expected_result = [[10, '131'], [20, '231'], [30, '331'], [40, '431']]
        env.expect('TS.RANGE', agg_key, 10, 50, conn=r).equal(expected_result)


def test_agg_last(env):
    with env.getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(env, r, 'tester{a}', 'last')

        expected_result = [[10, '184'], [20, '284'], [30, '384'], [40, '484']]
        env.expect('TS.RANGE', agg_key, 10, 50, conn=r).equal(expected_result)


def test_agg_range(env):
    with env.getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(env, r, 'tester{a}', 'range')

        expected_result = [[10, '74'], [20, '74'], [30, '74'], [40, '74']]
        env.expect('TS.RANGE', agg_key, 10, 50, conn=r).equal(expected_result)


def test_range_count(env):
    start_ts = 1511885908
    samples_count = 50

    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'tester1')
        for i in range(samples_count):
            r.execute_command('TS.ADD', 'tester1', start_ts + i, i)
        full_results = r.execute_command('TS.RANGE', 'tester1', 0, '+')
        env.assertEqual(len(full_results), samples_count)
        env.expect('TS.RANGE', 'tester1', 0, '+', conn=r).apply(len).equal(samples_count)
        env.expect('TS.RANGE', 'tester1', 0, '+', 'COUNT', 10, conn=r).equal(full_results[:10])
        env.expect('TS.RANGE', 'tester1', 0, '+', 'COUNT', 10, 'AGGREGATION', 'COUNT', 3, conn=r).apply(len).equal(10)
        env.expect('TS.RANGE', 'tester1', 0, '+', 'AGGREGATION', 'COUNT', 4, 'COUNT', 10, conn=r).apply(len).equal(10)
        env.expect('TS.RANGE', 'tester1', 0, '+', 'AGGREGATION', 'COUNT', 3, conn=r).apply(len).equal(math.ceil(samples_count / 3.0))


def test_agg_min(env):
    with env.getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(env, r, 'tester{a}', 'min')

        expected_result = [[10, '123'], [20, '223'], [30, '323'], [40, '423']]
        env.expect('TS.RANGE', agg_key, 10, 50, conn=r).equal(expected_result)


def test_agg_max(env):
    with env.getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(env, r, 'tester{a}', 'max')

        expected_result = [[10, '197'], [20, '297'], [30, '397'], [40, '497']]
        env.expect('TS.RANGE', agg_key, 10, 50, conn=r).equal(expected_result)


def test_agg_avg(env):
    with env.getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(env, r, 'tester{a}', 'avg')

        expected_result = [[10, '156.5'], [20, '256.5'], [30, '356.5'], [40, '456.5']]
        env.expect('TS.RANGE', agg_key, 10, 50, conn=r).equal(expected_result)


def test_series_ordering(env):
    with env.getClusterConnectionIfNeeded() as r:
        sample_len = 1024
        chunk_size = 4

        r.execute_command("ts.create", 'test_key', 0, chunk_size)
        for i in range(sample_len):
            r.execute_command("ts.add", 'test_key', i, i)

        res = r.execute_command('ts.range', 'test_key', 0, sample_len)
        i = 0
        for sample in res:
            env.assertEqual(sample, [i, str(i)])
            i += 1


def test_sanity(env):
    start_ts = 1511885909
    samples_count = 1500
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'tester', 'RETENTION', '0', 'CHUNK_SIZE', '1024',
                   'LABELS', 'name', 'brown', 'color', 'pink', conn=r).noEqual([])
        _insert_data(env, r, 'tester', start_ts, samples_count, 5)

        expected_result = [[start_ts + i, str(5)] for i in range(samples_count)]
        env.expect('TS.range', 'tester', start_ts, start_ts + samples_count, conn=r).equal(expected_result)

        expected_result = [
            'totalSamples', 1500, 'memoryUsage', 1166,
            'firstTimestamp', start_ts, 'chunkCount', 1,
            'labels', [['name', 'brown'], ['color', 'pink']],
            'lastTimestamp', start_ts + samples_count - 1,
            'chunkSize', 1024, 'retentionTime', 0,
            'sourceKey', None, 'rules', []]
        env.assertEqual(TSInfo(expected_result), _get_ts_info(r, 'tester'))


def test_sanity_pipeline(env):
    start_ts = 1488823384
    samples_count = 1500
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'tester', conn=r).noError()
        with r.pipeline(transaction=False) as p:
            p.set("name", "danni")
            _insert_data(env, p, 'tester', start_ts, samples_count, 5)
            p.execute()
        expected_result = [[start_ts + i, str(5)] for i in range(samples_count)]
        env.expect('TS.range', 'tester', start_ts, start_ts + samples_count, conn=r).equal(expected_result)


def test_issue358(env):
    filepath = "./issue358.txt"
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'issue358')

        with open(filepath) as fp:
            line = fp.readline()
            while line:
                line = fp.readline()
                if line != '':
                    r.execute_command(*line.split())
        range_res = r.execute_command('ts.range', 'issue358', 1582848000, '+')[0][1]
        env.expect('ts.get', 'issue358', conn=r).apply(lambda x: x[1]).equal(range_res)


def test_filter_by(env):
    start_ts = 1511885909
    samples_count = 1500
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'tester', 'RETENTION', '0', 'CHUNK_SIZE', '1024', 'LABELS', 'name',
                   'brown', 'color', 'pink', conn=r).ok()
        _insert_data(env, r, 'tester', start_ts, samples_count, list(i for i in range(samples_count)))

        res = r.execute_command('ts.range', 'tester', start_ts, '+', 'FILTER_BY_VALUE', 40, 52)
        env.assertEqual(len(res), 13)
        env.assertEqual([int(sample[1]) for sample in res], list(range(40, 53)))

        res = r.execute_command('ts.range', 'tester', start_ts, '+',
                          'FILTER_BY_TS', start_ts+1021, start_ts+1022, start_ts+1025, start_ts+1029)
        env.assertEqual(res, [[start_ts+1021, '1021'], [start_ts+1022, '1022'], [start_ts+1025, '1025'], [start_ts+1029, '1029']])

        exp_res = [[start_ts+1022, '1022'], [start_ts+1023, '1023'], [start_ts+1025, '1025']]
        env.expect('ts.range', 'tester', start_ts, '+',
                   'FILTER_BY_TS', start_ts+1021, start_ts+1022, start_ts+1023, start_ts+1025, start_ts+1029,
                   'FILTER_BY_VALUE', 1022, 1025, conn=r).equal(exp_res)


def get_bucket(timsetamp, alignment_ts, aggregation_bucket_size):
    return timsetamp - ((timsetamp - alignment_ts) % aggregation_bucket_size)


def build_expected_aligned_data(start_ts, end_ts, agg_size, alignment_ts):
    expected_data = []
    last_bucket = get_bucket(start_ts, alignment_ts, agg_size)
    curr_bucket_val = 0
    curr_bucket = None
    # import pdb;pdb.set_trace()
    for i in range(start_ts, end_ts):
        current_ts = i
        curr_bucket = get_bucket(current_ts, alignment_ts, agg_size)
        if curr_bucket  <= last_bucket:
            curr_bucket_val+=1
        else:
            expected_data.append([last_bucket, str(curr_bucket_val)])
            last_bucket = curr_bucket
            curr_bucket_val = 1
    expected_data.append([curr_bucket, str(curr_bucket_val)])
    return expected_data


def test_aggreataion_alignment(env):
    start_ts = 1511885909
    samples_count = 1200
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'tester', conn=r).ok()
        _insert_data(env, r, 'tester', start_ts, samples_count, list(i for i in range(samples_count)))

        agg_size = 60
        expected_data = build_expected_aligned_data(start_ts, start_ts + samples_count, agg_size, start_ts)

        env.expect('TS.range', 'tester', start_ts, '+', 'ALIGN', 'start', 'AGGREGATION', 'count', agg_size, conn=r).equal(expected_data)
        env.expect('TS.range', 'tester', start_ts, '+', 'ALIGN', '-', 'AGGREGATION', 'count', agg_size, conn=r).equal(expected_data)

        specific_ts = start_ts + 50
        expected_data = build_expected_aligned_data(start_ts, start_ts + samples_count, agg_size, specific_ts)
        env.expect('TS.range', 'tester', '-', '+', 'ALIGN', specific_ts, 'AGGREGATION', 'count', agg_size, conn=r).equal(expected_data)

        end_ts = start_ts + samples_count - 1
        expected_data = build_expected_aligned_data(start_ts, start_ts + samples_count, agg_size, end_ts)
        env.expect('TS.range', 'tester', '-', end_ts, 'ALIGN', 'end', 'AGGREGATION', 'count', agg_size, conn=r).equal(expected_data)
        env.expect('TS.range', 'tester', '-', end_ts, 'ALIGN', '+', 'AGGREGATION', 'count', agg_size, conn=r).equal(expected_data)
