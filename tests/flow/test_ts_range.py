import math

import pytest
import redis
from RLTest import Env
from test_helper_classes import TSInfo, ALLOWED_ERROR, _insert_data, _get_ts_info, \
    _insert_agg_data
from includes import *
from utils import timeit


def test_range_query():
    start_ts = 1488823384
    samples_count = 1500
    with Env().getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 'tester', 'uncompressed', 'RETENTION', samples_count - 100)
        _insert_data(r, 'tester', start_ts, samples_count, 5)

        expected_result = [[start_ts + i, str(5).encode('ascii')] for i in range(99, 151)]
        actual_result = r.execute_command('TS.range', 'tester', start_ts + 50, start_ts + 150)
        assert expected_result == actual_result
        rev_result = r.execute_command('TS.revrange', 'tester', start_ts + 50, start_ts + 150)
        rev_result.reverse()
        assert expected_result == rev_result

        # test out of range returns empty list
        assert [] == r.execute_command('TS.range', 'tester', int(start_ts * 2), '+')
        assert [] == r.execute_command('TS.range', 'tester', int(start_ts / 3), int(start_ts / 2))

        assert [] == r.execute_command('TS.revrange', 'tester', int(start_ts * 2), '+')
        assert [] == r.execute_command('TS.revrange', 'tester', int(start_ts / 3), int(start_ts / 2))

        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.RANGE', 'tester', 'string', '+')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.RANGE', 'tester', 0, 'string')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.RANGE', 'tester', 0, -1)
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.RANGE', 'tester', -1, 1000)
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.RANGE', 'nonexist', 0, '+')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.RANGE', 'tester', 0, '+', '', 'aggregation')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.RANGE', 'tester', 0, '+', 'count', 'number')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.RANGE', 'tester', 0, '+', 'count')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.RANGE', 'tester', 0, '+', 'aggregation', 'count', 'number')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.RANGE', 'tester', 0, '+', 'aggregation', 'count')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.RANGE', 'tester', 0, '+', 'aggregation', '')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.RANGE', 'tester', 0, '+', 'aggregation', 'not_aggregation_function')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.RANGE', 'tester', 0, '+', 'aggregation', '')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.RANGE', 'tester', '-', '+', 'FILTER_BY_VALUE')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.RANGE', 'tester', '-', '+', 'FILTER_BY_TS')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.RANGE', 'tester', '-', '+', 'FILTER_BY_TS', 'FILTER_BY_VALUE')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.RANGE', 'tester', '-', '+', 'FILTER_BY_VALUE', 'FILTER_BY_TS')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.RANGE', 'tester', '-', '+', 'FILTER_BY_VALUE', 10, 'FILTER_BY_TS')


def test_range_midrange():
    samples_count = 5000
    with Env().getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 'tester', 'UNCOMPRESSED')
        for i in range(samples_count):
            r.execute_command('TS.ADD', 'tester', i, i)
        res = r.execute_command('TS.RANGE', 'tester', samples_count - 500, samples_count)
        assert len(res) == 500  # sample_count is not in range() so not included
        res = r.execute_command('TS.RANGE', 'tester', samples_count - 1500, samples_count - 1000)
        assert len(res) == 501

        # test for empty range between two full ranges
        for i in range(samples_count):
            r.execute_command('TS.ADD', 'tester', samples_count * 2 + i, i)
        res = r.execute_command('TS.RANGE', 'tester', int(samples_count * 1.1), int(samples_count + 1.2))
        assert res == []


def test_range_with_agg_query():
    start_ts = 1488823384
    samples_count = 1500
    with Env().getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 'tester')
        _insert_data(r, 'tester', start_ts, samples_count, 5)

        expected_result = [[1488823000, b'116'], [1488823500, b'500'], [1488824000, b'500'], [1488824500, b'384']]
        actual_result = r.execute_command('TS.range', 'tester', start_ts, start_ts + samples_count, 'AGGREGATION',
                                          'count', 500)
        assert expected_result == actual_result

        # test first aggregation is not [0,0] if out of range
        expected_result = [[1488823000, b'116'], [1488823500, b'500']]
        actual_result = r.execute_command('TS.range', 'tester', 1488822000, 1488823999, b'AGGREGATION',
                                          'count', 500)
        assert expected_result == actual_result

        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.range', 'tester', start_ts, start_ts + samples_count, 'AGGREGATION',
                                     'count', -1)


def test_agg_std_p():
    with Env().getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(r, 'tester{a}', 'std.p')

        expected_result = [[10, b'25.869'], [20, b'25.869'], [30, b'25.869'], [40, b'25.869']]
        actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
        for i in range(len(expected_result)):
            assert abs(float(expected_result[i][1]) - float(actual_result[i][1])) < ALLOWED_ERROR


def test_agg_std_s():
    with Env().getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(r, 'tester{a}', 'std.s')

        expected_result = [[10, b'27.269'], [20, b'27.269'], [30, b'27.269'], [40, b'27.269']]
        actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
        for i in range(len(expected_result)):
            assert abs(float(expected_result[i][1]) - float(actual_result[i][1])) < ALLOWED_ERROR


def test_agg_var_p():
    with Env().getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(r, 'tester{a}', 'var.p')

        expected_result = [[10, b'669.25'], [20, b'669.25'], [30, b'669.25'], [40, b'669.25']]
        actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
        for i in range(len(expected_result)):
            assert abs(float(expected_result[i][1]) - float(actual_result[i][1])) < ALLOWED_ERROR


def test_agg_var_s():
    with Env().getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(r, 'tester{a}', 'var.s')

        expected_result = [[10, b'743.611'], [20, b'743.611'], [30, b'743.611'], [40, b'743.611']]
        actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
        for i in range(len(expected_result)):
            assert abs(float(expected_result[i][1]) - float(actual_result[i][1])) < ALLOWED_ERROR


def test_agg_sum():
    with Env().getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(r, 'tester{a}', 'sum')

        expected_result = [[10, b'1565'], [20, b'2565'], [30, b'3565'], [40, b'4565']]
        actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
        assert expected_result == actual_result


def test_agg_count():
    with Env().getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(r, 'tester{a}', 'count')

        expected_result = [[10, b'10'], [20, b'10'], [30, b'10'], [40, b'10']]
        actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
        assert expected_result == actual_result


def test_agg_first():
    with Env().getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(r, 'tester{a}', 'first')

        expected_result = [[10, b'131'], [20, b'231'], [30, b'331'], [40, b'431']]
        actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
        assert expected_result == actual_result


def test_agg_last():
    with Env().getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(r, 'tester{a}', 'last')

        expected_result = [[10, b'184'], [20, b'284'], [30, b'384'], [40, b'484']]
        actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
        assert expected_result == actual_result


def test_agg_range():
    with Env().getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(r, 'tester{a}', 'range')

        expected_result = [[10, b'74'], [20, b'74'], [30, b'74'], [40, b'74']]
        actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
        assert expected_result == actual_result


def test_range_count():
    start_ts = 1511885908
    samples_count = 50

    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'tester1')
        for i in range(samples_count):
            r.execute_command('TS.ADD', 'tester1', start_ts + i, i)
        full_results = r.execute_command('TS.RANGE', 'tester1', 0, '+')
        assert len(full_results) == samples_count
        count_results = r.execute_command('TS.RANGE', 'tester1', 0, '+', b'COUNT', 10)
        assert count_results == full_results[:10]
        count_results = r.execute_command('TS.RANGE', 'tester1', 0, '+', b'COUNT', 10, b'AGGREGATION', 'COUNT', 3)
        assert len(count_results) == 10
        count_results = r.execute_command('TS.RANGE', 'tester1', 0, '+', b'AGGREGATION', 'COUNT', 4, b'COUNT', 10)
        assert len(count_results) == 10
        count_results = r.execute_command('TS.RANGE', 'tester1', 0, '+', b'AGGREGATION', 'COUNT', 3)
        assert len(count_results) == math.ceil(samples_count / 3.0)


def test_agg_min():
    with Env().getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(r, 'tester{a}', 'min')

        expected_result = [[10, b'123'], [20, b'223'], [30, b'323'], [40, b'423']]
        actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
        assert expected_result == actual_result


def test_agg_max():
    with Env().getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(r, 'tester{a}', 'max')

        expected_result = [[10, b'197'], [20, b'297'], [30, b'397'], [40, b'497']]
        actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
        assert expected_result == actual_result


def test_agg_avg():
    with Env().getClusterConnectionIfNeeded() as r:
        agg_key = _insert_agg_data(r, 'tester{a}', 'avg')

        expected_result = [[10, b'156.5'], [20, b'256.5'], [30, b'356.5'], [40, b'456.5']]
        actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
        assert expected_result == actual_result

        #test overflow
        MAX_DOUBLE = 1.7976931348623157E+308
        assert r.execute_command('TS.CREATE', 'ts1')
        assert r.execute_command('TS.ADD', 'ts1', 1, MAX_DOUBLE - 10)
        assert r.execute_command('TS.ADD', 'ts1', 2, MAX_DOUBLE - 8)
        assert r.execute_command('TS.ADD', 'ts1', 3, MAX_DOUBLE - 6)

        actual_result = r.execute_command('TS.RANGE', 'ts1', 0, 3, 'AGGREGATION', 'avg', 5)
        #MAX_DOUBLE - 10 equals MAX_DOUBLE cause of precision limit

        # If this test fails in the future it means
        # it run on an OS/compiler that doesn't support long double need to remove the comment below
        # if(VALGRIND || sizeof(c_longdouble) > 8):
        if(VALGRIND):
            assert actual_result == [[0, b'1.7976931348623155E308']]
        else:
            assert actual_result == [[0, b'1.7976931348623157E308']]

        MIN_DOUBLE = -1.7976931348623157E+308
        assert r.execute_command('TS.CREATE', 'ts2')
        assert r.execute_command('TS.ADD', 'ts2', 1, MIN_DOUBLE + 10)
        assert r.execute_command('TS.ADD', 'ts2', 2, MIN_DOUBLE + 8)
        assert r.execute_command('TS.ADD', 'ts2', 3, MIN_DOUBLE + 6)

        actual_result = r.execute_command('TS.RANGE', 'ts2', 0, 3, 'AGGREGATION', 'avg', 5)
        #MIN_DOUBLE + 10 equals MIN_DOUBLE cause of precision limit
        if(VALGRIND):
            assert actual_result == [[0, b'-1.7976931348623155E308']]
        else:
            assert actual_result == [[0, b'-1.7976931348623157E308']]


def test_series_ordering():
    with Env().getClusterConnectionIfNeeded() as r:
        sample_len = 1024
        chunk_size = 4

        r.execute_command("ts.create", 'test_key', 0, chunk_size)
        for i in range(sample_len):
            r.execute_command("ts.add", 'test_key', i, i)

        res = r.execute_command('ts.range', 'test_key', 0, sample_len)
        i = 0
        for sample in res:
            assert sample == [i, str(i).encode('ascii')]
            i += 1


def test_sanity():
    start_ts = 1511885909
    samples_count = 1500
    with Env().getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 'tester', 'RETENTION', '0', 'CHUNK_SIZE', '1024', 'LABELS', 'name',
                                 'brown', 'color', 'pink')
        _insert_data(r, 'tester', start_ts, samples_count, 5)

        expected_result = [[start_ts + i, str(5).encode('ascii')] for i in range(samples_count)]
        actual_result = r.execute_command('TS.range', 'tester', start_ts, start_ts + samples_count)
        assert expected_result == actual_result
        expected_result = [
            b'totalSamples', 1500, b'memoryUsage', 1166,
            b'firstTimestamp', start_ts, b'chunkCount', 1,
            b'labels', [[b'name', b'brown'], [b'color', b'pink']],
            b'lastTimestamp', start_ts + samples_count - 1,
            b'chunkSize', 1024, b'retentionTime', 0,
            b'sourceKey', None, b'rules', []]
        assert TSInfo(expected_result) == _get_ts_info(r, 'tester')


def test_sanity_pipeline():
    start_ts = 1488823384
    samples_count = 1500
    with Env().getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 'tester')
        with r.pipeline(transaction=False) as p:
            p.set("name", "danni")
            _insert_data(p, 'tester', start_ts, samples_count, 5)
            p.execute()
        expected_result = [[start_ts + i, str(5).encode('ascii')] for i in range(samples_count)]
        actual_result = r.execute_command('TS.range', 'tester', start_ts, start_ts + samples_count)
        assert expected_result == actual_result


def test_issue358():
    filepath = "./issue358.txt"
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'issue358')

        with open(filepath) as fp:
            line = fp.readline()
            while line:
                line = fp.readline()
                if line != '':
                    r.execute_command(*line.split())
        range_res = r.execute_command('ts.range', 'issue358', 1582848000, '+')[0][1]
        get_res = r.execute_command('ts.get', 'issue358')[1]
        assert range_res == get_res


def test_filter_by():
    start_ts = 1511885909
    samples_count = 1500
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 'tester', 'RETENTION', '0', 'CHUNK_SIZE', '1024', 'LABELS', 'name',
                                 'brown', 'color', 'pink')
        _insert_data(r, 'tester', start_ts, samples_count, list(i for i in range(samples_count)))

        res = r.execute_command('ts.range', 'tester', start_ts, '+', 'FILTER_BY_VALUE', 40, 52)

        assert len(res) == 13
        assert [int(sample[1]) for sample in res] == list(range(40, 53))

        res = r.execute_command('ts.range', 'tester', start_ts, '+',
                                'FILTER_BY_TS', start_ts + 1021, start_ts + 1022, start_ts + 1025, start_ts + 1029)
        env.assertEqual(res, [[start_ts + 1021, b'1021'], [start_ts + 1022, b'1022'], [start_ts + 1025, b'1025'],
                              [start_ts + 1029, b'1029']])

        res = r.execute_command('ts.range', 'tester', start_ts, '+',
                                'FILTER_BY_TS', start_ts + 1021, start_ts + 1022, start_ts + 1023, start_ts + 1025,
                                start_ts + 1029,
                                'FILTER_BY_VALUE', 1022, 1025)
        env.assertEqual(res, [[start_ts + 1022, b'1022'], [start_ts + 1023, b'1023'], [start_ts + 1025, b'1025']])

def test_filter_by_extensive():
    env = Env()
    #skip cause it takes too much time
    env.skipOnCluster()
    env.skipOnAOF()
    env.skipOnSlave()
    max_samples_count = 40
    for ENCODING in ['uncompressed']:
        for ev_odd in ['even', 'odd']:
            if(ev_odd == 'even'):
                start_ts = 2
            else:
                start_ts = 3
            env.flush()
            with env.getClusterConnectionIfNeeded() as r:
                assert r.execute_command('TS.CREATE', 't1', ENCODING, 'RETENTION', '0', 'CHUNK_SIZE', '512')
                # 32 samples in chunk, 40 samples: 2 chunk in total
                # inset even numbers
                for samples_count in range(0, max_samples_count + 1):
                    last_ts = start_ts + samples_count*2
                    r.execute_command('TS.ADD', 't1', last_ts, last_ts)

                    for rev in [False, True]:
                        query = 'ts.revrange' if rev else 'ts.range'
                        for first_ts in range(0, last_ts + 3):
                            str_first_ts = str(first_ts)
                            for second_ts in range(first_ts, last_ts + 3):
                                str_second_ts = str(second_ts)
                                expected_res = []
                                reminder = 0 if ev_odd == 'even' else 1
                                if(first_ts%2 == reminder and first_ts >= start_ts and first_ts <= last_ts):
                                    expected_res.append([first_ts, str_first_ts.encode()])
                                if(first_ts != second_ts and second_ts%2 == reminder and second_ts >= start_ts and second_ts <= last_ts):
                                    if(rev): #prepend
                                        expected_res.insert(0, [second_ts, str_second_ts.encode()])
                                    else:    # append
                                        expected_res.append([second_ts, str_second_ts.encode()])
                                res = r.execute_command(query, 't1', '-', '+', 'FILTER_BY_TS', first_ts, second_ts)
                                env.assertEqual(res, expected_res)

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
        if curr_bucket <= last_bucket:
            curr_bucket_val += 1
        else:
            expected_data.append([last_bucket, str(curr_bucket_val)])
            last_bucket = curr_bucket
            curr_bucket_val = 1
    expected_data.append([curr_bucket, str(curr_bucket_val)])
    return expected_data


def test_aggreataion_alignment():
    start_ts = 1511885909
    samples_count = 1200
    env = Env(decodeResponses=True)
    with env.getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 'tester')
    _insert_data(r, 'tester', start_ts, samples_count, list(i for i in range(samples_count)))

    agg_size = 60
    expected_data = build_expected_aligned_data(start_ts, start_ts + samples_count, agg_size, start_ts)

    assert expected_data == \
           decode_if_needed(r.execute_command('TS.range', 'tester', start_ts, '+', 'ALIGN', 'start', 'AGGREGATION', 'count', agg_size))

    assert expected_data == \
           decode_if_needed(r.execute_command('TS.range', 'tester', start_ts, '+', 'ALIGN', '-', 'AGGREGATION', 'count', agg_size))

    specific_ts = start_ts + 50
    expected_data = build_expected_aligned_data(start_ts, start_ts + samples_count, agg_size, specific_ts)
    assert expected_data == \
           decode_if_needed(r.execute_command('TS.range', 'tester', '-', '+', 'ALIGN', specific_ts, 'AGGREGATION', 'count', agg_size))

    end_ts = start_ts + samples_count - 1
    expected_data = build_expected_aligned_data(start_ts, start_ts + samples_count, agg_size, end_ts)
    assert expected_data == \
           decode_if_needed(r.execute_command('TS.range', 'tester', '-', end_ts, 'ALIGN', 'end', 'AGGREGATION', 'count', agg_size))
    assert expected_data == \
           decode_if_needed(r.execute_command('TS.range', 'tester', '-', end_ts, 'ALIGN', '+', 'AGGREGATION', 'count', agg_size))
