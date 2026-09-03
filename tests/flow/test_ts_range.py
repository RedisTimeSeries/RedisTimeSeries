import math

# import pytest
# import redis
# from RLTest import Env
from test_helper_classes import TSInfo, ALLOWED_ERROR, _insert_data, _get_ts_info, \
    _insert_agg_data
from includes import *
from ctypes import *
from utils import timeit
import random
from datetime import datetime


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
        count_results = r.execute_command('TS.RANGE', 'tester1', 0, '+', 'COUNT', 10, b'AGGREGATION', 'COUNT', 4)
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
        if(VALGRIND or (sizeof(c_longdouble) == 8)):
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
        if(VALGRIND or (sizeof(c_longdouble) == 8)):
            assert actual_result == [[0, b'-1.7976931348623155E308']]
        else:
            assert actual_result == [[0, b'-1.7976931348623157E308']]

def test_agg_twa():
    #https://redislabs.atlassian.net/jira/software/c/projects/PM/boards/263?modal=detail&selectedIssue=PM-1229
    with Env().getClusterConnectionIfNeeded() as r:
        #case 1:
        assert r.execute_command('TS.CREATE', 'ts1')
        assert r.execute_command('TS.ADD', 'ts1', 8, 8)
        assert r.execute_command('TS.ADD', 'ts1', 9, 9)
        assert r.execute_command('TS.ADD', 'ts1', 10, 10)
        assert r.execute_command('TS.ADD', 'ts1', 13, 13)
        assert r.execute_command('TS.ADD', 'ts1', 14, 14)
        assert r.execute_command('TS.ADD', 'ts1', 23, 23)
        v1, v2, v3, v4, v5 = 9.0, 10.0, 13.0, 14.0, 23.0
        t1, t2, t3, t4, t5 = 9.0, 10.0, 13.0, 14.0, 23.0
        ta, tb = 10.0, 20.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        vb = v4+(v5-v4)*(tb-t4)/(t5-t4)
        s = (va+v2)*(t2-ta) + (v2+v3)*(t3-t2) + (v3+v4)*(t4-t3) + (vb+v4)*(tb-t4)
        res = s / (2*(tb-ta))
        expected_result = [10, str(int(res)).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts1', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts1', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 2:
        assert r.execute_command('TS.CREATE', 'ts2')
        assert r.execute_command('TS.ADD', 'ts2', 8, 8)
        assert r.execute_command('TS.ADD', 'ts2', 9, 9)
        assert r.execute_command('TS.ADD', 'ts2', 13, 13)
        assert r.execute_command('TS.ADD', 'ts2', 14, 14)
        assert r.execute_command('TS.ADD', 'ts2', 23, 23)
        v1, v2, v3, v4 = 9.0, 13.0, 14.0, 23.0
        t1, t2, t3, t4 = 9.0, 13.0, 14.0, 23.0
        ta, tb = 10.0, 20.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        vb = v3+(v4-v3)*(tb-t3)/(t4-t3)
        s = (va+v2)*(t2-ta) + (v2+v3)*(t3-t2) + (vb+v3)*(tb-t3)
        res = s / (2*(tb-ta))
        expected_result = [10, str(int(res)).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts2', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts2', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 3:
        assert r.execute_command('TS.CREATE', 'ts3')
        assert r.execute_command('TS.ADD', 'ts3', 8, 8)
        assert r.execute_command('TS.ADD', 'ts3', 9, 9)
        assert r.execute_command('TS.ADD', 'ts3', 13, 13)
        assert r.execute_command('TS.ADD', 'ts3', 14, 14)
        assert r.execute_command('TS.ADD', 'ts3', 26, 26)
        v1, v2, v3, v4 = 9.0, 13.0, 14.0, 26.0
        t1, t2, t3, t4 = 9.0, 13.0, 14.0, 26.0
        ta, tb = 10.0, 20.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        vb = v3+(v4-v3)*(tb-t3)/(t4-t3)
        s = (va+v2)*(t2-ta) + (v2+v3)*(t3-t2) + (vb+v3)*(tb-t3)
        res = s / (2*(tb-ta))
        expected_result = [10, str(int(res)).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts3', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts3', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 4:
        assert r.execute_command('TS.CREATE', 'ts4')
        assert r.execute_command('TS.ADD', 'ts4', 8, 8)
        assert r.execute_command('TS.ADD', 'ts4', 9, 9)
        assert r.execute_command('TS.ADD', 'ts4', 13, 13)
        assert r.execute_command('TS.ADD', 'ts4', 14, 14)
        assert r.execute_command('TS.ADD', 'ts4', 27, 27)
        v1, v2, v3, v4 = 9.0, 13.0, 14.0, 27.0
        t1, t2, t3, t4 = 9.0, 13.0, 14.0, 27.0
        ta, tb = 10.0, 20.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        vb = v3+(v4-v3)*(tb-t3)/(t4-t3)
        s = (va+v2)*(t2-ta) + (v2+v3)*(t3-t2) + (vb+v3)*(tb-t3)
        res = s / (2*(tb-ta))
        expected_result = [10, str(int(res)).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts4', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts4', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 5:
        assert r.execute_command('TS.CREATE', 'ts5')
        assert r.execute_command('TS.ADD', 'ts5', 3, 3)
        assert r.execute_command('TS.ADD', 'ts5', 7, 7)
        assert r.execute_command('TS.ADD', 'ts5', 13, 13)
        assert r.execute_command('TS.ADD', 'ts5', 14, 14)
        assert r.execute_command('TS.ADD', 'ts5', 27, 27)
        v1, v2, v3, v4 = 7.0, 13.0, 14.0, 27.0
        t1, t2, t3, t4 = 7.0, 13.0, 14.0, 27.0
        ta, tb = 10.0, 20.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        vb = v3+(v4-v3)*(tb-t3)/(t4-t3)
        s = (va+v2)*(t2-ta) + (v2+v3)*(t3-t2) + (vb+v3)*(tb-t3)
        res = s / (2*(tb-ta))
        expected_result = [10, str(int(res)).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts5', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts5', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 6:
        assert r.execute_command('TS.CREATE', 'ts6')
        assert r.execute_command('TS.ADD', 'ts6', 3, 3)
        assert r.execute_command('TS.ADD', 'ts6', 6, 6)
        assert r.execute_command('TS.ADD', 'ts6', 13, 13)
        assert r.execute_command('TS.ADD', 'ts6', 14, 14)
        assert r.execute_command('TS.ADD', 'ts6', 27, 27)
        v1, v2, v3, v4 = 6.0, 13.0, 14.0, 27.0
        t1, t2, t3, t4 = 6.0, 13.0, 14.0, 27.0
        ta, tb = 10.0, 20.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        vb = v3+(v4-v3)*(tb-t3)/(t4-t3)
        s = (va+v2)*(t2-ta) + (v2+v3)*(t3-t2) + (vb+v3)*(tb-t3)
        res = s / (2*(tb-ta))
        expected_result = [10, str(int(res)).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts6', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts6', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 7:
        assert r.execute_command('TS.CREATE', 'ts7')
        assert r.execute_command('TS.ADD', 'ts7', 3, 3)
        assert r.execute_command('TS.ADD', 'ts7', 9, 9)
        assert r.execute_command('TS.ADD', 'ts7', 13, 13)
        assert r.execute_command('TS.ADD', 'ts7', 22, 22)
        v1, v2, v3 = 9.0, 13.0, 22.0
        t1, t2, t3 = 9.0, 13.0, 22.0
        ta, tb = 10.0, 20.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        vb = v2+(v3-v2)*(tb-t2)/(t3-t2)
        s = (va+v2)*(t2-ta) + (vb+v2)*(tb-t2)
        res = s / (2*(tb-ta))
        expected_result = [10, str(int(res)).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts7', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts7', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 8:
        assert r.execute_command('TS.CREATE', 'ts8')
        assert r.execute_command('TS.ADD', 'ts8', 3, 3)
        assert r.execute_command('TS.ADD', 'ts8', 13, 13)
        assert r.execute_command('TS.ADD', 'ts8', 28, 28)
        v1, v2, v3 = 3.0, 13.0, 28.0
        t1, t2, t3 = 3.0, 13.0, 28.0
        ta, tb = 10.0, 20.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        vb = v2+(v3-v2)*(tb-t2)/(t3-t2)
        s = (va+v2)*(t2-ta) + (vb+v2)*(tb-t2)
        res = s / (2.0*(tb-ta))
        expected_result = [10, str(int(res)).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts8', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts8', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 9:
        assert r.execute_command('TS.CREATE', 'ts9')
        assert r.execute_command('TS.ADD', 'ts9', 13, 13)
        assert r.execute_command('TS.ADD', 'ts9', 28, 28)
        v1, v2, = 13.0, 28.0
        t1, t2, = 13.0, 28.0
        ta, tb = 10.0, 20.0
        vb = v1+(v2-v1)*(tb-t1)/(t2-t1)
        s = (v1+vb)*(tb-t1)
        res = s / (2.0*(tb-t1))
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts9', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts9', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 10:
        assert r.execute_command('TS.CREATE', 'ts10')
        assert r.execute_command('TS.ADD', 'ts10', 13, 13)
        assert r.execute_command('TS.ADD', 'ts10', 21, 21)
        v1, v2, = 13.0, 21.0
        t1, t2, = 13.0, 21.0
        ta, tb = 10.0, 20.0
        vb = v1+(v2-v1)*(tb-t1)/(t2-t1)
        s = (v1+vb)*(tb-t1)
        res = s / (2.0*(tb-t1))
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts10', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts10', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 11:
        assert r.execute_command('TS.CREATE', 'ts11')
        assert r.execute_command('TS.ADD', 'ts11', 17, 17)
        assert r.execute_command('TS.ADD', 'ts11', 21, 21)
        v1, v2, = 17.0, 21.0
        t1, t2, = 17.0, 21.0
        ta, tb = 10.0, 20.0
        vb = v1+(v2-v1)*(tb-t1)/(t2-t1)
        s = (v1+vb)*(tb-t1)
        res = s / (2.0*(tb-t1))
        expected_result = [10, str(res).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts11', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts11', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 12:
        assert r.execute_command('TS.CREATE', 'ts12')
        assert r.execute_command('TS.ADD', 'ts12', 3, 3)
        assert r.execute_command('TS.ADD', 'ts12', 17, 17)
        v1, v2, = 3.0, 17.0
        t1, t2, = 3.0, 17.0
        ta, tb = 10.0, 20.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        s = (va+v2)*(t2-ta)
        res = s / (2.0*(t2-ta))
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts12', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts12', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 14:
        assert r.execute_command('TS.CREATE', 'ts14')
        assert r.execute_command('TS.ADD', 'ts14', 5, 5)
        assert r.execute_command('TS.ADD', 'ts14', 12, 12)
        v1, v2, = 5.0, 12.0
        t1, t2, = 5.0, 12.0
        ta, tb = 10.0, 20.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        s = (va+v2)*(t2-ta)
        res = s / (2.0*(t2-ta))
        expected_result = [10, str(int(res)).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts14', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts14', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 15:
        assert r.execute_command('TS.CREATE', 'ts15')
        assert r.execute_command('TS.ADD', 'ts15', 7, 7)
        assert r.execute_command('TS.ADD', 'ts15', 15, 15)
        v1, v2, = 7.0, 15.0
        t1, t2, = 7.0, 15.0
        ta, tb = 10.0, 20.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        s = (va+v2)*(t2-ta)
        res = s / (2.0*(t2-ta))
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts15', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts15', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 16:
        assert r.execute_command('TS.CREATE', 'ts16')
        assert r.execute_command('TS.ADD', 'ts16', 15, 15)
        res = 15
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts16', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts16', 10, 20, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 17:
        assert r.execute_command('TS.CREATE', 'ts17')
        assert r.execute_command('TS.ADD', 'ts17', 0, 5) == 0
        assert r.execute_command('TS.ADD', 'ts17', 9, 12)
        v1, v2, = 5, 12.0
        t1, t2, = 0, 9.0
        ta, tb = 0, 10.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        s = (va+v2)*(t2-ta)
        res = s / (2.0*(t2-ta))
        expected_result = [0, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts17', 0, 10, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts17', 0, 10, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 18:
        assert r.execute_command('TS.CREATE', 'ts18')
        assert r.execute_command('TS.ADD', 'ts18', 10, 100)
        assert r.execute_command('TS.ADD', 'ts18', 13, 110)
        assert r.execute_command('TS.ADD', 'ts18', 15, 115)
        assert r.execute_command('TS.ADD', 'ts18', 19, 109)
        assert r.execute_command('TS.ADD', 'ts18', 25, 130)
        v1, v2, v3, v4, v5 = 100.0, 110.0, 115.0, 109.0, 130.0
        t1, t2, t3, t4, t5 = 10.0, 13.0, 15.0, 19.0, 25.0
        ta, tb = 12.0, 20.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        vb = v4+(v5-v4)*(tb-t4)/(t5-t4)
        s = (va+v2)*(t2-ta) + (v2+v3)*(t3-t2) + (v3+v4)*(t4-t3) + (vb+v4)*(tb-t4)
        res = s / (2.0*(tb-ta))
        expected_result = [0, str(res).encode('ascii')]

        # Test case #1:
        actual_result = r.execute_command('TS.RANGE', 'ts18', 12, 20, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts18', 12, 20, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.RANGE', 'ts18', 12, 20, 'AGGREGATION', 'twa', 1000)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts18', 12, 20, 'AGGREGATION', 'twa', 1000)
        assert actual_result[0] == expected_result

        ta, tb = 11.0, 30.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        s = (va+v2)*(t2-ta) + (v2+v3)*(t3-t2) + (v3+v4)*(t4-t3) + (v5+v4)*(t5-t4)
        res = s / (2.0*(t5-ta))
        expected_result = [0, str(res).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts18', 11, 30, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts18', 11, 30, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result

        # Test case #2:
        ta, tb = 11.0, 20.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        vb = v4+(v5-v4)*(tb-t4)/(t5-t4)
        s = (va+v2)*(t2-ta) + (v2+v3)*(t3-t2) + (v3+v4)*(t4-t3) + (vb+v4)*(tb-t4)
        res = s / (2.0*(tb-ta))
        expected_result = [0, str(res).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts18', 11, 20, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts18', 11, 20, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result

        # Test case #3:
        ta, tb = 12.0, 24.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        vb = v4+(v5-v4)*(tb-t4)/(t5-t4)
        s = (va+v2)*(t2-ta) + (v2+v3)*(t3-t2) + (v3+v4)*(t4-t3) + (vb+v4)*(tb-t4)
        res = s / (2*(tb-ta))
        expected_result = [0, str(res).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts18', 12, 24, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts18', 12, 24, 'AGGREGATION', 'twa', 100)
        round(float(actual_result[0][1]), 10) == round(res, 10)
        assert actual_result[0][0] == expected_result[0]

        # Test case #4:
        ta, tb = 11.0, 24.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        vb = v4+(v5-v4)*(tb-t4)/(t5-t4)
        s = (va+v2)*(t2-ta) + (v2+v3)*(t3-t2) + (v3+v4)*(t4-t3) + (vb+v4)*(tb-t4)
        res = s / (2.0*(tb-ta))
        expected_result = [0, str(res).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts18', 11, 24, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts18', 11, 24, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result

        # Test case #5:
        ta, tb = 8.0, 26.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        s = (v1+v2)*(t2-t1) + (v2+v3)*(t3-t2) + (v3+v4)*(t4-t3) + (v5+v4)*(t5-t4)
        res = s / (2.0*(t5-t1))
        expected_result = [0, str(res).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts18', 8, 26, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        expected_result = [0, str(res).encode('ascii')]
        actual_result = r.execute_command('TS.REVRANGE', 'ts18', 8, 26, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result

        # Test case #6:
        ta, tb = 8.0, 30.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        s = (v1+v2)*(t2-t1) + (v2+v3)*(t3-t2) + (v3+v4)*(t4-t3) + (v5+v4)*(t5-t4)
        res = s / (2.0*(t5-t1))
        expected_result = [0, str(res).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts18', 8, 30, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        expected_result = [0, str(res).encode('ascii')]
        actual_result = r.execute_command('TS.REVRANGE', 'ts18', 8, 30, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result

        # Test case #7:
        ta, tb = 9.0, 30.0
        s = (v1+v2)*(t2-t1) + (v2+v3)*(t3-t2) + (v3+v4)*(t4-t3) + (v5+v4)*(t5-t4)
        res = s / (2.0*(t5-t1))
        expected_result = [0, str(res).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts18', 9, 30, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        expected_result = [0, str(res).encode('ascii')]
        actual_result = r.execute_command('TS.REVRANGE', 'ts18', 9, 30, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result

        #Test case 19:
        assert r.execute_command('TS.CREATE', 'ts19')
        assert r.execute_command('TS.ADD', 'ts19', 10, 100)
        assert r.execute_command('TS.ADD', 'ts19', 20, 110)

        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts19', 16, 18, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts19', 16, 18, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result


        v1, v2 = 100.0, 110.0
        t1, t2 = 10.0, 20.0
        ta, tb = 16.0, 18.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        vb = v1+(v2-v1)*(tb-t1)/(t2-t1)
        res = (va + vb)/2.0
        expected_result = [0, str(int(res)).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts19', 16, 18, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts19', 16, 18, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result[0] == expected_result

        #Test case 20:
        ta, tb = 12.0, 14.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        vb = v1+(v2-v1)*(tb-t1)/(t2-t1)
        res = (va + vb)/2.0
        expected_result = [0, str(int(res)).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts19', 12, 14, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result[0] == expected_result
        expected_result = [0, str(int(res)).encode('ascii')]
        actual_result = r.execute_command('TS.REVRANGE', 'ts19', 12, 14, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result[0] == expected_result

        #Test case 21:
        ta, tb = 14.0, 19.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        vb = v1+(v2-v1)*(tb-t1)/(t2-t1)
        res = (va + vb)/2.0
        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts19', 14, 19, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts19', 14, 19, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result

        expected_result = [0, str(res).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts19', 14, 19, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts19', 14, 19, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result[0] == expected_result

        ta, tb = 11.0, 16.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        vb = v1+(v2-v1)*(tb-t1)/(t2-t1)
        res = (va + vb)/2.0
        expected_result = [0, str(res).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts19', 11, 16, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result[0] == expected_result
        expected_result = [0, str(res).encode('ascii')]
        actual_result = r.execute_command('TS.REVRANGE', 'ts19', 11, 16, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result[0] == expected_result

        #Test case 22:
        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts19', 11, 15, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts19', 11, 15, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result

        #case 4:
        assert r.execute_command('TS.CREATE', 'ts20')
        assert r.execute_command('TS.ADD', 'ts20', 20, 100)
        assert r.execute_command('TS.ADD', 'ts20', 30, 110)
        v1, v2 = 100.0, 110.0
        t1, t2 = 20.0, 30.0

        # Test case #13:
        ta, tb = 14.0, 22.0
        vb = v1+(v2-v1)*(tb-t1)/(t2-t1)
        res = (v1 + vb)/2.0
        expected_result = [0, str(int(res)).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts20', 14, 22, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 14, 22, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result

        # Test case #14:
        ta, tb = 14.0, 28.0
        vb = v1+(v2-v1)*(tb-t1)/(t2-t1)
        res = (v1 + vb)/2.0
        expected_result = [0, str(int(res)).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts20', 14, 28, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 14, 28, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result

        # Test case #15:
        ta, tb = 16.0, 22.0
        vb = v1+(v2-v1)*(tb-t1)/(t2-t1)
        res = (v1 + vb)/2.0
        expected_result = [0, str(int(res)).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts20', 16, 22, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 16, 22, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result

        # Test case #16:
        ta, tb = 16.0, 28.0
        vb = v1+(v2-v1)*(tb-t1)/(t2-t1)
        res = (v1 + vb)/2.0
        expected_result = [0, str(int(res)).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts20', 16, 28, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 16, 28, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result

        # Test case #17:
        ta, tb = 24.0, 32.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        res = (va + v2)/2.0
        expected_result = [0, str(int(res)).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts20', 24, 32, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 24, 32, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result

        # Test case #18:
        ta, tb = 26.0, 32.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        res = (va + v2)/2.0
        expected_result = [0, str(int(res)).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts20', 26, 32, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 26, 32, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result

        # Test case #19:
        ta, tb = 24.0, 38.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        res = (va + v2)/2.0
        expected_result = [0, str(int(res)).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts20', 24, 38, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 24, 38, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result

        # Test case #20:
        ta, tb = 26.0, 38.0
        va = v1+(v2-v1)*(ta-t1)/(t2-t1)
        res = (va + v2)/2.0
        expected_result = [0, str(int(res)).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts20', 26, 38, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 26, 38, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result

        #Test case 21:
        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts20', 32, 34, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 32, 34, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result

        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts20', 32, 34, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 32, 34, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result == expected_result

        #Test case 22:
        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts20', 32, 100, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 32, 100, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result

        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts20', 32, 100, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 32, 100, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result == expected_result

        #Test case 23:
        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts20', 38, 100, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 38, 100, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result

        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts20', 38, 100, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 38, 100, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result == expected_result

        #Test case 24:
        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts20', 16, 18, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 16, 18, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result

        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts20', 16, 18, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 16, 18, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result == expected_result

        #Test case 25:
        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts20', 10, 18, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 10, 18, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result

        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts20', 10, 18, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 10, 18, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result == expected_result

        #Test case 26:
        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts20', 10, 14, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 10, 14, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result

        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts20', 10, 14, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts20', 10, 14, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result == expected_result

        #case 8:
        assert r.execute_command('TS.CREATE', 'ts21')
        assert r.execute_command('TS.ADD', 'ts21', 20, 100)

        # Test case #27:
        expected_result = [0, str(100).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts21', 10, 30, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        expected_result = [0, str(100).encode('ascii')]
        actual_result = r.execute_command('TS.REVRANGE', 'ts21', 10, 30, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result

        # Test case #28:
        expected_result = [0, str(100).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts21', 10, 20, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        expected_result = [0, str(100).encode('ascii')]
        actual_result = r.execute_command('TS.REVRANGE', 'ts21', 10, 20, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result

        # Test case #29:
        expected_result = [0, str(100).encode('ascii')]
        actual_result = r.execute_command('TS.RANGE', 'ts21', 20, 30, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result
        expected_result = [0, str(100).encode('ascii')]
        actual_result = r.execute_command('TS.REVRANGE', 'ts21', 20, 30, 'AGGREGATION', 'twa', 100)
        assert actual_result[0] == expected_result

        # Test case #30:
        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts21', 10, 15, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts21', 10, 15, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts21', 10, 15, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts21', 10, 15, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result == expected_result

        # Test case #35:
        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts21', 25, 35, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts21', 25, 35, 'AGGREGATION', 'twa', 100)
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.RANGE', 'ts21', 25, 35, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result == expected_result
        expected_result = []
        actual_result = r.execute_command('TS.REVRANGE', 'ts21', 25, 35, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result == expected_result

        # Test case 100:
        assert r.execute_command('TS.CREATE', 'ts22')
        assert r.execute_command('TS.ADD', 'ts22', 20, 100)
        assert r.execute_command('TS.ADD', 'ts22', 50, 130)
        v1, v2 = 100.0, 130.0
        t1, t2 = 20.0, 50.0
        ta, tb = 20.0, 50.0
        res = (v1 + v2)/2.0
        expected_result = [[0, str(int(res)).encode('ascii')]]
        actual_result = r.execute_command('TS.RANGE', 'ts22', 20, 50, 'AGGREGATION', 'twa', 100, 'EMPTY')
        assert actual_result == expected_result
        m = (130.0 - 100.0)/(50.0 - 20.0)
        va_fun = lambda ta: v1 + (ta - t1)*m
        expected_result = []
        for i in range(20, 60, 10):
            va = va_fun(i)
            vb = va_fun(min(i+10, 50))
            res = (va + vb)/2.0
            expected_result += [[i, str(int(res)).encode('ascii')]]
        actual_result = r.execute_command('TS.RANGE', 'ts22', 20, 50, 'AGGREGATION', 'twa', 10, 'EMPTY')
        assert actual_result == expected_result
        expected_result.reverse()
        actual_result = r.execute_command('TS.REVRANGE', 'ts22', 20, 50, 'AGGREGATION', 'twa', 10, 'EMPTY')
        assert actual_result == expected_result

        # Test case 101:
        assert r.execute_command('TS.CREATE', 'ts23')
        assert r.execute_command('TS.ADD', 'ts23', 40, 100)
        assert r.execute_command('TS.ADD', 'ts23', 50, 130)
        res = (130.0 + 100.0)/2.0
        expected_result = [[40, str(int(res)).encode('ascii')], [50, str(130).encode('ascii')]]
        actual_result = r.execute_command('TS.RANGE', 'ts23', 29, 70, 'AGGREGATION', 'twa', 10, 'EMPTY')
        assert actual_result == expected_result
        expected_result.reverse()
        actual_result = r.execute_command('TS.REVRANGE', 'ts23', 29, 70, 'AGGREGATION', 'twa', 10, 'EMPTY')
        assert actual_result == expected_result

        expected_result.reverse()
        actual_result = r.execute_command('TS.RANGE', 'ts23', 39, 70, 'AGGREGATION', 'twa', 10, 'EMPTY')
        assert actual_result == expected_result
        expected_result.reverse()
        actual_result = r.execute_command('TS.REVRANGE', 'ts23', 39, 70, 'AGGREGATION', 'twa', 10, 'EMPTY')
        assert actual_result == expected_result

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
            b'totalSamples', 1500,
            b'firstTimestamp', start_ts, b'chunkCount', 1,
            b'labels', [[b'name', b'brown'], [b'color', b'pink']],
            b'lastTimestamp', start_ts + samples_count - 1,
            b'chunkSize', 1024, b'retentionTime', 0,
            b'sourceKey', None, b'rules', [], b'ignoreMaxTimeDiff', 0, b'ignoreMaxValDiff', b'0']
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

def test_large_compressed_range():
    random.seed()
    with Env().getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 't1', 'compressed', 'RETENTION', '0', 'CHUNK_SIZE', '128')
        _len = 400
        n_samples_dict = {}
        ts = random.randint(2, _len) # For taking more space and chunks
        for i in range(_len):
            r.execute_command('TS.ADD', 't1', ts , i)
            n_samples_dict[ts] = i
            ts += random.randint(2, _len) # For taking more space and chunks

        res = r.execute_command('ts.range', 't1', '-', '+')
        assert len(res) == _len

        for key1, val1 in n_samples_dict.items():
            for key2, val2 in n_samples_dict.items():
                if(key2 < key1):
                    continue
                res = r.execute_command('ts.range', 't1', key1, key2)
                assert len(res) == val2 - val1 + 1
                res = r.execute_command('ts.range', 't1', key1 - 1, key2 + 1)
                assert len(res) == val2 - val1 + 1

def test_large_compressed_revrange():
    random.seed()
    with Env().getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 't1', 'compressed', 'RETENTION', '0', 'CHUNK_SIZE', '128')
        _len = 400
        n_samples_dict = {}
        ts = random.randint(2, _len) # For taking more space and chunks
        for i in range(_len):
            r.execute_command('TS.ADD', 't1', ts , i)
            n_samples_dict[ts] = i
            ts += random.randint(2, _len) # For taking more space and chunks

        res = r.execute_command('ts.range', 't1', '-', '+')
        assert len(res) == _len

        for key1, val1 in n_samples_dict.items():
            for key2, val2 in n_samples_dict.items():
                if(key2 < key1):
                    continue
                res = r.execute_command('ts.revrange', 't1', key1, key2)
                assert len(res) == val2 - val1 + 1
                res = r.execute_command('ts.revrange', 't1', key1 - 1, key2 + 1)
                assert len(res) == val2 - val1 + 1

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
                                'FILTER_BY_TS', start_ts + 1021, start_ts + 1021, start_ts + 1022, start_ts + 1022, start_ts + 1025, start_ts + 1029, start_ts + 1029)
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

def test_max_extensive():
    env = Env()
    #skip cause it takes too much time
    env.skipOnCluster()
    env.skipOnAOF()
    env.skipOnSlave()
    max_samples_count = 40
    n_chunk_samples = 32
    random_values = [None]*(max_samples_count*3)
    random.seed()
    try:
        for i in range(0, max_samples_count*3):
            random_values[i] = random.uniform(0, max_samples_count)
        max_val = random.uniform(max_samples_count, max_samples_count + 10)
        for ENCODING in ['uncompressed', 'compressed']:
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
                        r.execute_command('TS.ADD', 't1', last_ts, random_values[samples_count])

                        for rev in [False, True]:
                            query = 'ts.revrange' if rev else 'ts.range'
                            for bucket_size in [1, 2, 3, 5, 7, 10, n_chunk_samples - 1,
                            n_chunk_samples, n_chunk_samples+1, (2*n_chunk_samples)-1,
                            (2*n_chunk_samples), (2*n_chunk_samples)+1, (3*n_chunk_samples)-1,
                            (3*n_chunk_samples), (3*n_chunk_samples)+1]:
                                for alignment in [0, 2, 5, 7]:
                                    normalized_alignment = alignment%bucket_size
                                    for max_index in range(start_ts, last_ts + 1):
                                        r.execute_command('TS.ADD', 't1', max_index, max_val, 'ON_DUPLICATE', 'last')
                                        start_ts_bucket = (start_ts - normalized_alignment)//bucket_size
                                        max_bucket_reply_index = (max_index - normalized_alignment)//bucket_size - start_ts_bucket
                                        max_bucket_ts = max(0, max_index - ((max_index - normalized_alignment)%bucket_size))
                                        res = r.execute_command(query, 't1', '-', '+', 'ALIGN', alignment, 'AGGREGATION', 'max', bucket_size)
                                        if rev:
                                            max_bucket_reply_index = len(res) - 1 - max_bucket_reply_index
                                        expected_res = [max_bucket_ts, str(max_val).encode()]
                                        env.assertEqual(res[max_bucket_reply_index], expected_res)

                                        #restore to the old val
                                        r.execute_command('TS.ADD', 't1', max_index, random_values[i], 'ON_DUPLICATE', 'last')
    except Exception as e:
        print(seed)
        raise e

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
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        assert r.execute_command('TS.CREATE', 'tester')
        _insert_data(r1, 'tester', start_ts, samples_count, list(i for i in range(samples_count)))

        agg_size = 60
        expected_data = build_expected_aligned_data(start_ts, start_ts + samples_count, agg_size, start_ts)

        assert expected_data == \
               decode_if_needed(r1.execute_command('TS.range', 'tester', start_ts, '+', 'ALIGN', 'start', 'AGGREGATION', 'count', agg_size))

        assert expected_data == \
               decode_if_needed(r1.execute_command('TS.range', 'tester', start_ts, '+', 'ALIGN', '-', 'AGGREGATION', 'count', agg_size))

        specific_ts = start_ts + 50
        expected_data = build_expected_aligned_data(start_ts, start_ts + samples_count, agg_size, specific_ts)
        assert expected_data == \
               decode_if_needed(r1.execute_command('TS.range', 'tester', '-', '+', 'ALIGN', specific_ts, 'AGGREGATION', 'count', agg_size))

        end_ts = start_ts + samples_count - 1
        expected_data = build_expected_aligned_data(start_ts, start_ts + samples_count, agg_size, end_ts)
        assert expected_data == \
               decode_if_needed(r1.execute_command('TS.range', 'tester', '-', end_ts, 'ALIGN', 'end', 'AGGREGATION', 'count', agg_size))
        assert expected_data == \
               decode_if_needed(r1.execute_command('TS.range', 'tester', '-', end_ts, 'ALIGN', '+', 'AGGREGATION', 'count', agg_size))

def test_keyword_named_key_not_misparsed():
    # Regression for MOD-15893 / PR #2052 ("Scope TS.RANGE/NRANGE option parsing
    # past the key block"). Option keywords used to be located by scanning the
    # entire argv, so a single-key TS.RANGE/TS.REVRANGE whose series key was
    # NAMED like an option keyword (ALIGN, COUNT, LATEST, AGGREGATION,
    # FILTER_BY_VALUE, FILTER_BY_TS) had its key matched as that option and the
    # following token consumed as the option's value -- e.g. a key named 'align'
    # matched ALIGN and ate the fromTimestamp as the alignment value. The shared
    # parseRangeArguments now scopes option scanning to argv+start_index, so the
    # key name must no longer affect parsing.
    #
    # The guard is an equivalence check: a series keyed by an option keyword must
    # return exactly what an identically populated control series returns. Before
    # the fix the no-ALIGN query on key 'align' returned [[1,'2'],[11,'1'],[21,'1']]
    # (aligned to from=1) while the control returned [[0,'2'],[10,'1'],[20,'1']],
    # so this would have failed.
    keyword_keys = ['align', 'count', 'latest', 'aggregation', 'filter_by_value', 'filter_by_ts']
    samples = [(1, 10), (3, 5), (11, 10), (25, 11)]
    env = Env(decodeResponses=True)
    with env.getClusterConnectionIfNeeded() as r:
        def populate(key):
            assert r.execute_command('TS.CREATE', key)
            for ts, val in samples:
                r.execute_command('TS.ADD', key, ts, val)
        populate('control')
        for k in keyword_keys:
            populate(k)

        # Every query must parse identically regardless of the key's name.
        queries = [
            ('1', '30'),                                                  # no options
            ('1', '30', 'AGGREGATION', 'count', '10'),                    # no ALIGN -> default align 0
            ('1', '30', 'ALIGN', 'start', 'AGGREGATION', 'count', '10'),
            ('1', '30', 'ALIGN', 'end', 'AGGREGATION', 'count', '10'),
            ('1', '30', 'ALIGN', '5', 'AGGREGATION', 'count', '10'),
            ('1', '30', 'COUNT', '2'),
            ('1', '30', 'FILTER_BY_TS', '1', '11'),
            ('1', '30', 'FILTER_BY_VALUE', '5', '10'),
            ('1', '30', 'LATEST'),
        ]
        for cmd in ('TS.RANGE', 'TS.REVRANGE'):
            for q in queries:
                expected = decode_if_needed(r.execute_command(cmd, 'control', *q))
                for k in keyword_keys:
                    got = decode_if_needed(r.execute_command(cmd, k, *q))
                    assert got == expected, \
                        f"{cmd} on key '{k}' parsed differently than control for args {q}: {got} != {expected}"

        # Lock in the correct (post-fix) values for the 'align'-named key, the
        # exact scenario the Jedis #align test hit.
        align_expected = {
            ('1', '30', 'AGGREGATION', 'count', '10'):                   [[0, '2'], [10, '1'], [20, '1']],
            ('1', '30', 'ALIGN', 'start', 'AGGREGATION', 'count', '10'): [[1, '2'], [11, '1'], [21, '1']],
            ('1', '30', 'ALIGN', 'end', 'AGGREGATION', 'count', '10'):   [[0, '2'], [10, '1'], [20, '1']],
            ('1', '30', 'ALIGN', '5', 'AGGREGATION', 'count', '10'):     [[0, '2'], [5, '1'], [25, '1']],
        }
        for q, exp in align_expected.items():
            assert decode_if_needed(r.execute_command('TS.RANGE', 'align', *q)) == exp, \
                f"TS.RANGE align {q}"

def test_empty():
    agg_size = 10
    env = Env(decodeResponses=True)
    with env.getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 't1')
        assert r.execute_command('TS.add', 't1', 15, 1)
        assert r.execute_command('TS.add', 't1', 17, 4)
        assert r.execute_command('TS.add', 't1', 51, 3)
        assert r.execute_command('TS.add', 't1', 73, 5)
        assert r.execute_command('TS.add', 't1', 75, 3)
        assert r.execute_command('TS.CREATE', 't2')
        assert r.execute_command('TS.add', 't2', 10, 1)
        assert r.execute_command('TS.add', 't2', 30, 4)
        expected_data = [[10, '4'], [20, 'NaN'], [30, 'NaN'], [40, 'NaN'], [50, '3'], [60, 'NaN'], [70, '5']]
        assert expected_data == \
        decode_if_needed(r.execute_command('TS.range', 't1', '0', '100', 'ALIGN', '0', 'AGGREGATION', 'max', agg_size, 'EMPTY'))
        expected_data.reverse()
        assert expected_data == \
        decode_if_needed(r.execute_command('TS.revrange', 't1', '0', '100', 'ALIGN', '0', 'AGGREGATION', 'max', agg_size, 'EMPTY'))

        expected_data_2 = [[10, '4'], [20, '4'], [30, '4'], [40, '4'], [50, '3'], [60, '3'], [70, '3']]
        assert expected_data_2 == \
        decode_if_needed(r.execute_command('TS.range', 't1', '0', '100', 'ALIGN', '0', 'AGGREGATION', 'last', agg_size, 'EMPTY'))
        expected_data_2.reverse()
        assert expected_data_2 == \
        decode_if_needed(r.execute_command('TS.revrange', 't1', '0', '100', 'ALIGN', '0', 'AGGREGATION', 'last', agg_size, 'EMPTY'))

        expected_data = [[10, '5'], [20, '0'], [30, '0'], [40, '0'], [50, '3'], [60, '0'], [70, '8']]
        assert expected_data == \
        decode_if_needed(r.execute_command('TS.range', 't1', '0', '100', 'ALIGN', '0', 'AGGREGATION', 'sum', agg_size, 'EMPTY'))
        expected_data.reverse()
        assert expected_data == \
        decode_if_needed(r.execute_command('TS.revrange', 't1', '0', '100', 'ALIGN', '0', 'AGGREGATION', 'sum', agg_size, 'EMPTY'))

        expected_data = [[10, '1'], [20, 'NaN'], [30, '4']]
        assert expected_data == \
        decode_if_needed(r.execute_command('TS.range', 't2', '0', '30', 'ALIGN', '0', 'AGGREGATION', 'max', agg_size, 'EMPTY'))
        expected_data.reverse()
        assert expected_data == \
        decode_if_needed(r.execute_command('TS.revrange', 't2', '0', '30', 'ALIGN', '0', 'AGGREGATION', 'max', agg_size, 'EMPTY'))


# MOD-8187: EMPTY gap filling must work for every aggregator, not only TWA, when the empty buckets
# are a prefix, a suffix, or the whole queried range (cases 3, 4 and 5 of PM-1229). Before the fix
# only TWA emitted those buckets, so LAST (and the rest) returned a truncated / empty result.
# The existing test_empty only exercised *interior* gaps (first/last bucket hold real samples),
# which is why these cases were never caught.
def test_empty_gap_fill_prefix_suffix_whole_range():
    env = Env(decodeResponses=True)
    with env.getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 'a')
        assert r.execute_command('TS.MADD', 'a', 10, 100, 'a', 20, 110) == [10, 20]

        def rng(start, end, agg):
            return decode_if_needed(
                r.execute_command('TS.range', 'a', start, end, 'ALIGN', '0', 'AGGREGATION', agg, 1, 'EMPTY'))

        def revrng(start, end, agg):
            return decode_if_needed(
                r.execute_command('TS.revrange', 'a', start, end, 'ALIGN', '0', 'AGGREGATION', agg, 1, 'EMPTY'))

        # --- LAST: repeats the previous value (LOCF) for the gap-filled buckets ---
        # Case 3: whole range is a gap between sample 10 and sample 20 -> all carry 100.
        case3 = [[ts, '100'] for ts in range(11, 17)]
        assert rng(11, 16, 'last') == case3
        assert revrng(11, 16, 'last') == list(reversed(case3))

        # Case 4: sample 10 is in range; trailing buckets 11,12 are an interior gap (sample 20
        # follows) -> LOCF 100. Buckets 8,9 (before the first-ever sample) are dropped.
        case4 = [[10, '100'], [11, '100'], [12, '100']]
        assert rng(8, 12, 'last') == case4
        assert revrng(8, 12, 'last') == list(reversed(case4))

        # Case 5: leading buckets 18,19 are an interior gap -> LOCF 100; bucket 20 holds 110.
        # Buckets 21,22 (after the last-ever sample) are dropped.
        case5 = [[18, '100'], [19, '100'], [20, '110']]
        assert rng(18, 22, 'last') == case5
        assert revrng(18, 22, 'last') == list(reversed(case5))

        # Case prefix-emit: a leading gap whose older neighbor is OUT of range. Sample 10 (=100)
        # is before the query, sample 20 (=110) is the first in-range sample. Buckets 13..19 form
        # a prefix gap that must be EMITTED via LOCF from the out-of-range sample 10, then 20 holds
        # 110. Unlike case 3 (whole-range, entered via agg_iter_on_empty_chunk), this drives the
        # forward agg_iter_apply_empty_prefix emit path directly.
        case_prefix = [[ts, '100'] for ts in range(13, 20)] + [[20, '110']]
        assert rng(13, 20, 'last') == case_prefix
        assert revrng(13, 20, 'last') == list(reversed(case_prefix))

        # Edge gaps with no neighbor on one side are dropped for every aggregator (PM canceled
        # cases 6 and 7): a range entirely before the first / after the last sample is empty.
        for agg in ('last', 'twa', 'avg', 'max', 'min', 'sum', 'first', 'count'):
            assert rng(2, 5, agg) == []
            assert rng(25, 30, agg) == []
            assert revrng(2, 5, agg) == []
            assert revrng(25, 30, agg) == []

        # Non-LOCF aggregators report the documented empty-bucket value in interior gaps.
        nan6 = [[ts, 'NaN'] for ts in range(11, 17)]
        for agg in ('avg', 'max', 'min', 'first', 'range', 'std.p', 'var.p'):
            assert rng(11, 16, agg) == nan6
        zero6 = [[ts, '0'] for ts in range(11, 17)]
        for agg in ('sum', 'count'):
            assert rng(11, 16, agg) == zero6

        # TWA was already correct - guard against regressions in the shared code path.
        twa3 = [[11, '101.5'], [12, '102.5'], [13, '103.5'], [14, '104.5'], [15, '105.5'], [16, '106']]
        assert rng(11, 16, 'twa') == twa3
        assert revrng(11, 16, 'twa') == list(reversed(twa3))

        # --- bucket size > 1 and non-zero ALIGN: exercises the CalcBucketStart / +-delta edge math
        # in the prefix & suffix helpers (the bucket-1/ALIGN-0 cases above keep every ts in its own
        # bucket and never hit it). Samples 12 (=100) and 42 (=200); bucket size 5, ALIGN 2 -> bucket
        # starts at 2,7,12,...  [12,17) and [42,47) are real; [17..42) is an interior gap; buckets
        # before 12 and after 47 are edge gaps and must be dropped. ---
        assert r.execute_command('TS.CREATE', 'b')
        assert r.execute_command('TS.MADD', 'b', 12, 100, 'b', 42, 200) == [12, 42]

        def brng(start, end, agg):
            return decode_if_needed(
                r.execute_command('TS.range', 'b', start, end, 'ALIGN', '2', 'AGGREGATION', agg, 5, 'EMPTY'))

        def brevrng(start, end, agg):
            return decode_if_needed(
                r.execute_command('TS.revrange', 'b', start, end, 'ALIGN', '2', 'AGGREGATION', agg, 5, 'EMPTY'))

        last_aligned = [[12, '100'], [17, '100'], [22, '100'], [27, '100'], [32, '100'], [37, '100'], [42, '200']]
        assert brng(0, 60, 'last') == last_aligned
        assert brevrng(0, 60, 'last') == list(reversed(last_aligned))

        sum_aligned = [[12, '100'], [17, '0'], [22, '0'], [27, '0'], [32, '0'], [37, '0'], [42, '200']]
        assert brng(0, 60, 'sum') == sum_aligned

        max_aligned = [[12, '100'], [17, 'NaN'], [22, 'NaN'], [27, 'NaN'], [32, 'NaN'], [37, 'NaN'], [42, '200']]
        assert brng(0, 60, 'max') == max_aligned

        # Edge-only ranges (before first / after last real bucket) stay empty under alignment too.
        for agg in ('last', 'twa', 'sum', 'max'):
            assert brng(0, 11, agg) == []
            assert brng(47, 60, agg) == []


def test_bucket_timestamp():
    agg_size = 10
    env = Env(decodeResponses=True)
    with env.getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 't1')
        assert r.execute_command('TS.add', 't1', 15, 1)
        assert r.execute_command('TS.add', 't1', 17, 4)
        assert r.execute_command('TS.add', 't1', 51, 3)
        assert r.execute_command('TS.add', 't1', 73, 5)
        assert r.execute_command('TS.add', 't1', 75, 3)

        expected_data = [[10, '4'], [50, '3'], [70, '5']]
        assert expected_data == \
        decode_if_needed(r.execute_command('TS.range', 't1', '0', '100', 'ALIGN', '0', 'AGGREGATION', 'max', agg_size, 'BUCKETTIMESTAMP', '-'))
        expected_data.reverse()
        assert expected_data == \
        decode_if_needed(r.execute_command('TS.revrange', 't1', '0', '100', 'ALIGN', '0', 'AGGREGATION', 'max', agg_size, 'BUCKETTIMESTAMP', '-'))

        expected_data = [[15, '4'], [55, '3'], [75, '5']]
        assert expected_data == \
        decode_if_needed(r.execute_command('TS.range', 't1', '0', '74', 'ALIGN', '0', 'AGGREGATION', 'max', agg_size, 'BUCKETTIMESTAMP', '~'))
        expected_data.reverse()
        assert expected_data == \
        decode_if_needed(r.execute_command('TS.revrange', 't1', '0', '100', 'ALIGN', '0', 'AGGREGATION', 'max', agg_size, 'BUCKETTIMESTAMP', '~'))

        expected_data = [[20, '4'], [60, '3'], [80, '5']]
        assert expected_data == \
        decode_if_needed(r.execute_command('TS.range', 't1', '0', '74', 'ALIGN', '0', 'AGGREGATION', 'max', agg_size, 'BUCKETTIMESTAMP', '+'))
        expected_data.reverse()
        assert expected_data == \
        decode_if_needed(r.execute_command('TS.revrange', 't1', '0', '100', 'ALIGN', '0', 'AGGREGATION', 'max', agg_size, 'BUCKETTIMESTAMP', '+'))

def test_latest_flag_range():
    env = Env(decodeResponses=True)
    key1 = 't1{1}'
    key2 = 't2{1}'
    with env.getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', key1)
        assert r.execute_command('TS.CREATE', key2)
        assert r.execute_command('TS.CREATERULE', key1, key2, 'AGGREGATION', 'SUM', 10)
        assert r.execute_command('TS.add', key1, 1, 1)
        assert r.execute_command('TS.add', key1, 2, 3)
        assert r.execute_command('TS.add', key1, 11, 7)
        assert r.execute_command('TS.add', key1, 13, 1)
        res = r.execute_command('TS.range', key1, 0, 20)
        assert res == [[1, '1'], [2, '3'], [11, '7'], [13, '1']] or res == [[1, b'1'], [2, b'3'], [11, b'7'], [13, b'1']]
        res = r.execute_command('TS.range', key2, 0, 10)
        assert res == [[0, '4']] or res == [[0, b'4']]
        res = r.execute_command('TS.range', key2, 0, 10, "LATEST")
        assert res == [[0, '4'], [10, '8']] or res == [[0, b'4'], [10, b'8']]
        res = r.execute_command('TS.range', key2, 0, 9, "LATEST")
        assert res == [[0, '4']] or res == [[0, b'4']]
        res = r.execute_command('TS.range', key2, 0, 1, "LATEST")
        assert res == [[0, '4']] or res == [[0, b'4']]
        res = r.execute_command('TS.range', key2, 20, 30, "LATEST")
        assert res == []
        res = r.execute_command('TS.range', key2, 11, 30, "LATEST")
        assert res == []

        # make sure LATEST haven't changed anything in the keys
        res = r.execute_command('TS.range', key2, 0, 10)
        assert res == [[0, '4']] or res == [[0, b'4']]
        res = r.execute_command('TS.range', key1, 0, 20)
        assert res == [[1, '1'], [2, '3'], [11, '7'], [13, '1']] or res == [[1, b'1'], [2, b'3'], [11, b'7'], [13, b'1']]

#https://github.com/RedisTimeSeries/RedisTimeSeries/issues/1247
def test_latest_flag_range_plus():
        env = Env(decodeResponses=True)
        key3 = 't3{1}'
        key4 = 't4{1}'
        with env.getClusterConnectionIfNeeded() as r:
            assert r.execute_command('TS.CREATE', key3)
            assert r.execute_command('TS.CREATE', key4)
            assert r.execute_command('TS.CREATERULE', key3, key4, 'AGGREGATION', 'RANGE', 10)
            assert r.execute_command('TS.add', key3, 10, 20)
            assert r.execute_command('TS.add', key3, 11, 30)
            res = r.execute_command('TS.range', key4, 0, 10000, "LATEST")
            assert res == [[10, '10']] or res == [[10, b'10']]
            res = r.execute_command('TS.range', key4, "-", "+", "LATEST")
            assert res == [[10, '10']] or res == [[10, b'10']]

def test_latest_flag_revrange():
    env = Env(decodeResponses=True)
    key1 = 't1{1}'
    key2 = 't2{1}'
    with env.getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', key1)
        assert r.execute_command('TS.CREATE', key2)
        assert r.execute_command('TS.CREATERULE', key1, key2, 'AGGREGATION', 'SUM', 10)
        assert r.execute_command('TS.add', key1, 1, 1)
        assert r.execute_command('TS.add', key1, 2, 3)
        assert r.execute_command('TS.add', key1, 11, 7)
        assert r.execute_command('TS.add', key1, 13, 1)
        res = r.execute_command('TS.range', key1, 0, 20)
        assert res == [[1, '1'], [2, '3'], [11, '7'], [13, '1']] or res == [[1, b'1'], [2, b'3'], [11, b'7'], [13, b'1']]
        res = r.execute_command('TS.revrange', key2, 0, 10)
        assert res == [[0, '4']] or res == [[0, b'4']]
        res = r.execute_command('TS.revrange', key2, 0, 10, "LATEST")
        assert res == [[10, '8'], [0, '4']] or res == [[10, b'8'], [0, b'4']]
        res = r.execute_command('TS.revrange', key2, 0, 9, "LATEST")
        assert res == [[0, '4']] or res == [[0, b'4']]

        # make sure LATEST haven't changed anything in the keys
        res = r.execute_command('TS.revrange', key2, 0, 10)
        assert res == [[0, '4']] or res == [[0, b'4']]
        res = r.execute_command('TS.range', key1, 0, 20)
        assert res == [[1, '1'], [2, '3'], [11, '7'], [13, '1']] or res == [[1, b'1'], [2, b'3'], [11, b'7'], [13, b'1']]

# issue: #1370
def test_multi_chunk_revrange():
    env = Env(decodeResponses=True)
    t1 = 't1{1}'
    with env.getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', t1, 'CHUNK_SIZE', 128)
        n_samples = 10000

        # Fill 1 chunks
        for i in range(1, 103):
            assert r.execute_command('TS.ADD', t1, i, i)

        # fill 2 chunks
        for i in range(120, 220):
            assert r.execute_command('TS.ADD', t1, i, i)

        res = r.execute_command('TS.revrange', t1, 110, 215, 'AGGREGATION', 'count', 2*n_samples)
        assert res == [[0, '96']] or res == [[0, b'96']]

def test_errors():
    env = Env(decodeResponses=True)
    t1 = 't1{1}'
    t2 = 't2{1}'
    with env.getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', t1, 'CHUNK_SIZE', 128)
        assert r.execute_command('ts.add', t1, 1, 1)
        try:
            r.execute_command('ts.range', t1, 0, 10, 'AGGREGATION', 'avg', 5, 'BUCKETTIMESTAMP', 'high')
            assert False
        except Exception as e:
            assert str(e) == 'TSDB: unknown BUCKETTIMESTAMP parameter'

        assert r.execute_command('TS.CREATE', t2, 'CHUNK_SIZE', 128)
        try:
            r.execute_command('TS.CREATERULE', t1, t2, 'AGGREGATION', 'avg', 5, 'high')
            assert False
        except Exception as e:
            assert str(e) == "TSDB: Couldn't parse alignTimestamp"


def test_latest_flag_with_deleted_source():
    """
    Test scenario:
     - Create a compaction key.
     - Delete the source key and recreate it. It will not have the compaction rule.
     - Query dst key with LATEST flag to test we handle this case without a crash.
    """
    env = Env(decodeResponses=True)
    src = 't1{1}'
    dst = 't2{1}'
    with env.getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', src)
        assert r.execute_command('TS.CREATE', dst)
        assert r.execute_command('TS.CREATERULE', src, dst, 'AGGREGATION', 'SUM', 10)

        # Delete src, recreate it and query with LATEST flag
        assert r.execute_command('DEL', src)
        assert r.execute_command('TS.CREATE', src, '100', '100')
        assert r.execute_command('TS.RANGE', dst, '-', '+', 'LATEST', 'AGGREGATION', 'sum', 10) == []
        assert r.execute_command('TS.REVRANGE', dst, '-', '+', 'LATEST', 'AGGREGATION', 'sum', 10) == []
        # Delete src, recreate it with a sample in it and query with LATEST flag
        assert r.execute_command('DEL', src)
        assert r.execute_command('TS.ADD', src, '100', '100')
        assert r.execute_command('TS.RANGE', dst, '-', '+', 'LATEST', 'AGGREGATION', 'sum', 10) == []
        assert r.execute_command('TS.GET', dst, 'LATEST') == []

        # Run the same test with non-empty key
        r.execute_command('DEL', src, dst)
        assert r.execute_command('TS.CREATE', src)
        assert r.execute_command('TS.CREATE', dst)
        assert r.execute_command('TS.CREATERULE', src, dst, 'AGGREGATION', 'SUM', 10)
        for i in range(100):
            r.execute_command('TS.ADD', src, i, i)

        # Delete src, recreate it and query with LATEST flag
        assert r.execute_command('DEL', src)
        assert r.execute_command('TS.CREATE', src, '100', '100')
        assert r.execute_command('TS.RANGE', dst, '-', '+', 'LATEST', 'AGGREGATION', 'sum', 10) != []
        assert r.execute_command('TS.GET', dst, 'LATEST') != []
        # Delete src, recreate it with a sample in it and query with LATEST flag
        assert r.execute_command('DEL', src)
        assert r.execute_command('TS.ADD', src, '100', '100')
        assert r.execute_command('TS.RANGE', dst, '-', '+', 'LATEST', 'AGGREGATION', 'sum', 10) != []
        assert r.execute_command('TS.GET', dst, 'LATEST') != []


def test_ts_range_count_validation():
    """
    Validate COUNT argument is non-positive for TS.RANGE family commands
    """
    env = Env(decodeResponses=True)
    env.skipOnCluster()
    key = 'x'
    with env.getClusterConnectionIfNeeded() as r:
        for i in range(10):
            r.execute_command('TS.ADD', key, i, i)

    env.expect('TS.RANGE', key, '-', '+', 'COUNT', '0').error().contains('TSDB: Invalid COUNT value')
    env.expect('TS.RANGE', key, '-', '+', 'COUNT', '-1').error().contains('TSDB: Invalid COUNT value')
    env.expect('TS.RANGE', key, '-', '+', 'COUNT', '-2').error().contains('TSDB: Invalid COUNT value')
    env.expect('TS.REVRANGE', key, '-', '+', 'COUNT', '-1000').error().contains('TSDB: Invalid COUNT value')
    env.expect('TS.MRANGE', '-', '+', 'COUNT', '-2', 'FILTER', 'a=x').error().contains('TSDB: Invalid COUNT value')
    env.expect('TS.MREVRANGE', '-', '+', 'COUNT', '0', 'FILTER', 'a=x').error().contains('TSDB: Invalid COUNT value')

    assert r.execute_command('TS.RANGE', key, '-', '+', 'COUNT', 2) == [[0, '0'], [1, '1']]
    assert r.execute_command('TS.RANGE', key, '-', '+', 'COUNT', 2, 'AGGREGATION', 'sum', 5) == [[0, '10'], [5, '35']]


def test_ts_range_NaN_values():
    """
    Validates that all the aggregation functions handle NaN values correctly.
    NaN values should be treated as empty buckets when the EMPTY flag is set.
    Aggregation functions should ignore NaN values.
    """
    with Env().getClusterConnectionIfNeeded() as r:
        for encoding in ['compressed', 'uncompressed']:
            Env().flush()
            key = 'ts_nan_test{a}{encoding}'
            
            # Create a series with mixed NaN and valid values
            # Bucket 0-99: values 10, NaN, 20, NaN, 30 (valid: 10, 20, 30)
            # Bucket 100-199: only NaN values (should be treated as empty)
            # Bucket 200-299: values 40, 50 (valid: 40, 50)
            r.execute_command('TS.CREATE', key, 'ENCODING', encoding)
            
            # Bucket 0-99: mixed NaN and valid values
            r.execute_command('TS.ADD', key, 10, 10)
            r.execute_command('TS.ADD', key, 20, 'nan')
            r.execute_command('TS.ADD', key, 30, 20)
            r.execute_command('TS.ADD', key, 40, 'nan')
            r.execute_command('TS.ADD', key, 50, 30)
            
            # Bucket 100-199: only NaN values
            r.execute_command('TS.ADD', key, 110, 'nan')
            r.execute_command('TS.ADD', key, 120, 'nan')
            r.execute_command('TS.ADD', key, 130, 'nan')
            
            # Bucket 200-299: valid values only
            r.execute_command('TS.ADD', key, 210, 40)
            r.execute_command('TS.ADD', key, 220, 50)
            
            # Test 1: Aggregations should ignore NaN values
            # sum of valid values in bucket 0-99: 10 + 20 + 30 = 60
            result = r.execute_command('TS.RANGE', key, 0, 99, 'AGGREGATION', 'sum', 100)
            assert len(result) == 1
            assert result[0][0] == 0
            assert float(result[0][1]) == 60.0
            
            # count should only count valid (non-NaN) samples
            result = r.execute_command('TS.RANGE', key, 0, 99, 'AGGREGATION', 'count', 100)
            assert len(result) == 1
            assert float(result[0][1]) == 3.0  # 3 valid samples
            
            result = r.execute_command('TS.RANGE', key, 0, 99, 'AGGREGATION', 'min', 100)
            assert len(result) == 1
            assert float(result[0][1]) == 10.0
            
            result = r.execute_command('TS.RANGE', key, 0, 99, 'AGGREGATION', 'max', 100)
            assert len(result) == 1
            assert float(result[0][1]) == 30.0
            
            result = r.execute_command('TS.RANGE', key, 0, 99, 'AGGREGATION', 'avg', 100)
            assert len(result) == 1
            assert float(result[0][1]) == 20.0
            
            result = r.execute_command('TS.RANGE', key, 0, 99, 'AGGREGATION', 'first', 100)
            assert len(result) == 1
            assert float(result[0][1]) == 10.0
            
            result = r.execute_command('TS.RANGE', key, 0, 99, 'AGGREGATION', 'last', 100)
            assert len(result) == 1
            assert float(result[0][1]) == 30.0
            
            result = r.execute_command('TS.RANGE', key, 0, 99, 'AGGREGATION', 'range', 100)
            assert len(result) == 1
            assert float(result[0][1]) == 20.0
            
            # Test 2: Bucket with only NaN should be skipped (no EMPTY flag)
            # Query all three buckets - bucket 100-199 should be missing
            result = r.execute_command('TS.RANGE', key, 0, 299, 'AGGREGATION', 'sum', 100)
            assert len(result) == 2  # Only 2 buckets (0 and 200), bucket 100 is skipped
            assert result[0][0] == 0    # First bucket
            assert result[1][0] == 200  # Third bucket (second is skipped)
            assert float(result[0][1]) == 60.0   # sum of bucket 0
            assert float(result[1][1]) == 90.0   # sum of bucket 200: 40 + 50
            
            # Test 3: Bucket with only NaN should return NaN with EMPTY flag
            result = r.execute_command('TS.RANGE', key, 0, 299, 'AGGREGATION', 'sum', 100, 'EMPTY')
            assert len(result) == 3  # All 3 buckets
            assert result[0][0] == 0
            assert result[1][0] == 100
            assert result[2][0] == 200
            assert float(result[0][1]) == 60.0
            assert float(result[1][1]) == 0 # empty bucket
            assert float(result[2][1]) == 90.0
            
            # Test 4: REVRANGE should also handle NaN correctly
            result = r.execute_command('TS.REVRANGE', key, 0, 299, 'AGGREGATION', 'sum', 100)
            assert len(result) == 2  # Only 2 buckets, reversed
            assert result[0][0] == 200
            assert result[1][0] == 0
            
            # Test 5: count aggregation - empty bucket should return 0 with EMPTY flag
            result = r.execute_command('TS.RANGE', key, 0, 299, 'AGGREGATION', 'count', 100, 'EMPTY')
            assert len(result) == 3
            assert float(result[0][1]) == 3.0   # 3 valid samples in bucket 0
            assert float(result[1][1]) == 0.0   # 0 valid samples in bucket 100
            assert float(result[2][1]) == 2.0   # 2 valid samples in bucket 200
            
            # Test 6: Test aggregations with EMPTY flag on NaN-only bucket (100-199)
            # Expected values: NaN for most, 0 for sum/count, 0 for last (last valid before bucket),
            # interpolated for twa (has samples before and after)
            expected_empty_bucket_values = {
                'sum': 0.0,
                'count': 0.0,
                'min': float('nan'),
                'max': float('nan'),
                'avg': float('nan'),
                'first': float('nan'),
                'last': 30.0,  # last valid value before bucket (from ts=50)
                'range': float('nan'),
                'std.p': float('nan'),
                'std.s': float('nan'),
                'var.p': float('nan'),
                'var.s': float('nan'),
                'twa': 'interpolated',  # special case: has surrounding samples, should not be NaN
            }
            
            for agg_func, expected in expected_empty_bucket_values.items():
                result = r.execute_command('TS.RANGE', key, 0, 199, 'AGGREGATION', agg_func, 100, 'EMPTY')
                assert len(result) == 2, f"{agg_func}: expected 2 results"
                actual = float(result[1][1])
                if expected == 'interpolated':
                    assert not math.isnan(actual), f"{agg_func}: expected interpolated value, got NaN"
                elif math.isnan(expected):
                    assert math.isnan(actual), f"{agg_func}: expected NaN, got {actual}"
                else:
                    assert actual == expected, f"{agg_func}: expected {expected}, got {actual}"
            
            # Test 7: Query only the NaN-only bucket without EMPTY - should return empty
            result = r.execute_command('TS.RANGE', key, 100, 199, 'AGGREGATION', 'sum', 100)
            assert len(result) == 0  # No results - bucket is effectively empty
            
            # Test 8: 'last' and 'twa' return NaN when NO sample exists before the bucket
            key_no_before = 'ts_nan_no_before{a}'
            r.execute_command('TS.CREATE', key_no_before, 'ENCODING', encoding)
            r.execute_command('TS.ADD', key_no_before, 100, 'nan')  # NaN in bucket 100-199
            r.execute_command('TS.ADD', key_no_before, 200, 50)     # Valid sample after
            
            for agg_func in ['last', 'twa']:
                result = r.execute_command('TS.RANGE', key_no_before, 100, 199, 'AGGREGATION', agg_func, 100, 'EMPTY')
                assert len(result) == 1
                assert math.isnan(float(result[0][1])), f"{agg_func}: expected NaN when no sample before bucket, got {result[0][1]}"


def test_twa_nan_interpolation():
    """
    Test that TWA aggregation correctly skips NaN samples when fetching samples
    before/after the query range for interpolation.    
    """
    with Env().getClusterConnectionIfNeeded() as r:
        # Test case 1: NaN sample immediately before query range should be skipped
        # The valid sample before NaN should be used for interpolation instead
        key1 = 'twa_nan_before{tag}'
        r.execute_command('TS.CREATE', key1)
        r.execute_command('TS.ADD', key1, 5, 5)    # Valid sample - should be used for interpolation
        r.execute_command('TS.ADD', key1, 8, 'nan') # NaN right before range - should be skipped
        r.execute_command('TS.ADD', key1, 12, 12)  # Sample in query range
        r.execute_command('TS.ADD', key1, 18, 18)  # Sample in query range
        r.execute_command('TS.ADD', key1, 25, 25)  # Sample after query range
        
        # Query range 10-20, bucket size 10
        # TWA should interpolate from sample at ts=5 (value=5), not from NaN at ts=8
        result = r.execute_command('TS.RANGE', key1, 10, 20, 'AGGREGATION', 'twa', 10)
        assert len(result) == 1
        # The result should be a valid number (not NaN)
        value = float(result[0][1])
        assert not math.isnan(value), f"TWA should not use NaN for interpolation, got {value}"
        
        # Test case 2: Multiple NaN samples before query range - all should be skipped
        key2 = 'twa_nan_multi_before{tag}'
        r.execute_command('TS.CREATE', key2)
        r.execute_command('TS.ADD', key2, 3, 3)    # Valid sample - should be used
        r.execute_command('TS.ADD', key2, 5, 'nan')
        r.execute_command('TS.ADD', key2, 7, 'nan')
        r.execute_command('TS.ADD', key2, 9, 'nan') # Multiple NaNs before range
        r.execute_command('TS.ADD', key2, 12, 12)  # In query range
        r.execute_command('TS.ADD', key2, 18, 18)  # In query range
        r.execute_command('TS.ADD', key2, 25, 25)  # After range
        
        result = r.execute_command('TS.RANGE', key2, 10, 20, 'AGGREGATION', 'twa', 10)
        assert len(result) == 1
        value = float(result[0][1])
        assert not math.isnan(value), f"TWA should skip multiple NaN samples, got {value}"
        
        # Test case 3: NaN samples after query range should also be skipped
        key3 = 'twa_nan_after{tag}'
        r.execute_command('TS.CREATE', key3)
        r.execute_command('TS.ADD', key3, 5, 5)
        r.execute_command('TS.ADD', key3, 12, 12)
        r.execute_command('TS.ADD', key3, 18, 18)
        r.execute_command('TS.ADD', key3, 22, 'nan')  # NaN right after range
        r.execute_command('TS.ADD', key3, 25, 25)     # Valid sample after NaN
        
        result = r.execute_command('TS.RANGE', key3, 10, 20, 'AGGREGATION', 'twa', 10)
        assert len(result) == 1
        value = float(result[0][1])
        assert not math.isnan(value), f"TWA should skip NaN after range, got {value}"
        
        # Test case 4: REVRANGE should also handle NaN interpolation correctly
        result = r.execute_command('TS.REVRANGE', key1, 10, 20, 'AGGREGATION', 'twa', 10)
        assert len(result) == 1
        value = float(result[0][1])
        assert not math.isnan(value), f"REVRANGE TWA should not use NaN for interpolation, got {value}"


def test_twa_nan_only_bucket():
    """
    Test that TWA correctly handles buckets containing only NaN samples.
    When a bucket has only NaN samples, TWA should not use uninitialized values
    for interpolation in subsequent buckets.
    """
    with Env().getClusterConnectionIfNeeded() as r:
        # Test case 1: TS.RANGE with NaN-only bucket in the middle
        key1 = 'twa_nan_only_bucket_range{tag}'
        r.execute_command('TS.CREATE', key1)
        r.execute_command('TS.ADD', key1, 5, 10)     # Bucket 0-10: valid sample
        r.execute_command('TS.ADD', key1, 15, 'nan') # Bucket 10-20: only NaN
        r.execute_command('TS.ADD', key1, 25, 30)    # Bucket 20-30: valid sample
        r.execute_command('TS.ADD', key1, 35, 40)    # Bucket 30-40: valid sample
        
        # Query across all buckets with EMPTY to include NaN-only bucket
        result = r.execute_command('TS.RANGE', key1, 0, 40, 'AGGREGATION', 'twa', 10, 'EMPTY')
        assert len(result) == 4
        
        # Bucket 0 (0-10): has sample at 5 with value 10 - should be valid
        bucket0_value = float(result[0][1])
        assert not math.isnan(bucket0_value), "Bucket 0 should have valid TWA value"
        
        # Bucket 1 (10-20): only NaN sample - TWA with EMPTY interpolates from surrounding samples
        # (sample at ts=5 value=10 before, sample at ts=25 value=30 after)
        bucket1_value = float(result[1][1])
        assert not math.isnan(bucket1_value), "TWA+EMPTY interpolates NaN-only bucket from neighbors"
        # Key test: the interpolated value should be reasonable (between 10 and 30)
        assert bucket1_value > 0, f"TWA should not use timestamp=0, got {bucket1_value}"
        
        # Bucket 2 (20-30): has sample at 25 with value 30 - should be valid
        # This is the key test: interpolation should work correctly, NOT from bogus (0,0) values
        bucket2_value = float(result[2][1])
        assert not math.isnan(bucket2_value), "Bucket after NaN-only bucket should have valid TWA"
        assert bucket2_value > 0, f"TWA should not use timestamp=0 for interpolation, got {bucket2_value}"
        
        # Bucket 3 (30-40): should interpolate correctly from bucket 2
        bucket3_value = float(result[3][1])
        assert not math.isnan(bucket3_value), "Bucket 3 should have valid TWA value"
        
        # Test case 2: REVRANGE with NaN-only bucket
        result_rev = r.execute_command('TS.REVRANGE', key1, 0, 40, 'AGGREGATION', 'twa', 10, 'EMPTY')
        assert len(result_rev) == 4
        # Results should be same as RANGE but reversed
        bucket2_rev = float(result_rev[1][1])  # Bucket 20-30 is second from end
        assert not math.isnan(bucket2_rev), "REVRANGE: bucket after NaN-only should have valid TWA"
        assert bucket2_rev > 0, f"REVRANGE: TWA should not use timestamp=0, got {bucket2_rev}"
        
        # Test case 3: First bucket is NaN-only (edge case - tests initial state)
        # No sample before first bucket, so TWA cannot interpolate backward
        key2 = 'twa_nan_first_bucket{tag}'
        r.execute_command('TS.CREATE', key2)
        r.execute_command('TS.ADD', key2, 5, 'nan')  # Bucket 0-10: only NaN
        r.execute_command('TS.ADD', key2, 15, 20)    # Bucket 10-20: valid sample
        r.execute_command('TS.ADD', key2, 25, 30)    # Bucket 20-30: valid sample
        
        result = r.execute_command('TS.RANGE', key2, 0, 30, 'AGGREGATION', 'twa', 10, 'EMPTY')
        assert len(result) == 3
        
        # First bucket is NaN-only with no sample before - may be NaN or interpolated forward
        # The key test is that subsequent buckets are NOT corrupted
        
        # Second bucket should NOT be corrupted by first bucket's uninitialized values
        bucket1_value = float(result[1][1])
        assert not math.isnan(bucket1_value), "Second bucket should have valid TWA"
        # The value should be reasonable (not affected by bogus timestamp=0)
        assert bucket1_value > 0, f"TWA should not interpolate from timestamp=0, got {bucket1_value}"
        
        # Test case 4: Compaction rule with NaN-only bucket
        src_key = 'twa_compaction_src{tag}'
        dst_key = 'twa_compaction_dst{tag}'
        r.execute_command('TS.CREATE', src_key)
        r.execute_command('TS.CREATE', dst_key)
        r.execute_command('TS.CREATERULE', src_key, dst_key, 'AGGREGATION', 'twa', 10)
        
        # Add samples: bucket 0-10 valid, bucket 10-20 NaN only, bucket 20-30 valid
        r.execute_command('TS.ADD', src_key, 5, 100)   # Bucket 0-10
        r.execute_command('TS.ADD', src_key, 15, 'nan') # Bucket 10-20: only NaN
        r.execute_command('TS.ADD', src_key, 25, 200)   # Bucket 20-30
        r.execute_command('TS.ADD', src_key, 35, 300)   # Trigger compaction for bucket 20-30
        
        # Check destination series
        result = r.execute_command('TS.RANGE', dst_key, 0, '+')
        
        # Should have bucket 0 (finalized when bucket 1 started) and bucket 2 
        # (finalized when bucket 3 started). Bucket 1 had only NaN so no output.
        assert len(result) >= 1, "Compaction should produce at least one bucket"
        
        for ts, val in result:
            value = float(val)
            assert not math.isnan(value), f"Compacted TWA at {ts} should not be NaN"
            # Key check: value should be reasonable, not corrupted by (0,0) interpolation
            assert value > 0, f"Compacted TWA at {ts} should be > 0, got {value}"


def test_ts_range_countNaN():
    """
    Validate COUNTNAN aggregation function
    """
    with Env().getClusterConnectionIfNeeded() as r:
        for encoding in ['compressed', 'uncompressed']:
            Env().flush()
            key = 'ts_countNaN_test{a}'
            r.execute_command('TS.CREATE', key, 'ENCODING', encoding)
            r.execute_command('TS.ADD', key, 10, 10)
            r.execute_command('TS.ADD', key, 20, 'nan')
            r.execute_command('TS.ADD', key, 30, 20)
            r.execute_command('TS.ADD', key, 40, 'nan')
            r.execute_command('TS.ADD', key, 50, 30)
            result = r.execute_command('TS.RANGE', key, 0, 99, 'AGGREGATION', 'countnan', 100)
            assert len(result) == 1
            assert float(result[0][1]) == 2.0
            result = r.execute_command('TS.REVRANGE', key, 0, 99, 'AGGREGATION', 'countnan', 10, 'EMPTY')
            assert len(result) == 5
            assert float(result[0][1]) == 0.0
            assert float(result[1][1]) == 1.0
            assert float(result[2][1]) == 0.0
            assert float(result[3][1]) == 1.0
            assert float(result[4][1]) == 0.0

            result = r.execute_command('TS.RANGE', key, 0, 99, 'AGGREGATION', 'countnan', 50)
            assert len(result) == 1
            assert float(result[0][1]) == 2.0

def test_ts_range_countAll():
    """
    Validate COUNTALL aggregation function
    """
    with Env().getClusterConnectionIfNeeded() as r:
        for encoding in ['compressed', 'uncompressed']:
            Env().flush()
            key = 'ts_countAll_test{a}'
            r.execute_command('TS.CREATE', key, 'ENCODING', encoding)
            r.execute_command('TS.ADD', key, 10, 10)
            r.execute_command('TS.ADD', key, 20, 'nan')
            r.execute_command('TS.ADD', key, 30, 20)
            r.execute_command('TS.ADD', key, 40, 'nan')
            r.execute_command('TS.ADD', key, 50, 30)
            result = r.execute_command('TS.RANGE', key, 0, 99, 'AGGREGATION', 'countall', 100)
            assert len(result) == 1
            assert float(result[0][1]) == 5.0
            result = r.execute_command('TS.REVRANGE', key, 0, 99, 'AGGREGATION', 'countall', 10, 'EMPTY')
            assert len(result) == 5
            assert float(result[0][1]) == 1.0
            assert float(result[1][1]) == 1.0
            assert float(result[2][1]) == 1.0
            assert float(result[3][1]) == 1.0
            assert float(result[4][1]) == 1.0


# ---------------------------------------------------------------------------
# Regression: TS.RANGE / TS.REVRANGE with AGGREGATION ... EMPTY must produce
# the same set of bucket timestamps in both directions (one is the reverse of
# the other), and carry the chronologically-correct value for LOCF-style
# aggregations such as LAST.
#
# This guards against two bugs that previously affected reverse iteration:
#   Bug A) extra empty buckets emitted past the data span.
#   Bug B) wrong carry-forward value for empty buckets between samples.
#
# To extend coverage to a new aggregation, just add it to AGGREGATIONS below.
# To additionally check exact values for that aggregation, add an entry to
# PER_AGG_VALUE_CHECKS. Without an entry, only timestamp-symmetry is verified.
# ---------------------------------------------------------------------------

# Aggregations to validate. Add more here with zero other code changes
# (provided the aggregation behaves like one of the cases handled by
# _agg_sample_value / _agg_empty_value below).
#
# Intentionally excluded: 'twa' (bucket-edge interpolation), 'std.p' / 'std.s' /
# 'var.p' / 'var.s' (single-sample value depends on sample/population denominator).
AGGREGATIONS = ['last', 'first', 'max', 'min', 'count', 'sum', 'avg', 'range']


def _agg_sample_value(agg, v):
    """Value the aggregation produces for a bucket that contains exactly one sample."""
    if agg == 'count':
        return '1'
    if agg == 'range':
        return '0'                    # max - min over a single sample
    # last, first, max, min, sum, avg → that single sample's value
    return str(v)


def _agg_empty_value(agg, prev_v, next_v):
    """Value the aggregation produces for an empty bucket that lies BETWEEN two samples
    (i.e. has both a left and a right neighbor — should_skip_empty_gap keeps it).

    Empty-bucket finalizers in src/compaction.c map as follows:
      - finalize_empty_last_value  → 'last'
      - finalize_empty_with_ZERO   → 'count', 'sum'
      - finalize_empty_with_NAN    → everything else (max, min, avg, range, first, ...)
    """
    if agg == 'last':
        return str(prev_v)            # LOCF: carry forward chronologically previous value
    if agg in ('count', 'sum'):
        return '0'
    return 'NaN'                      # max, min, avg, first, range, ...


def _build_expected_forward(samples, bucket_size, window, agg):
    """
    Build the expected forward TS.RANGE result for `agg` over `samples`,
    `window` and `bucket_size`. Each sample at t produces:
        - bucket t                   → _agg_sample_value(agg, v)
        - bucket (t + bucket_size)   → _agg_empty_value(agg, v, next_v)
    The trailing carry bucket is dropped when there is no next sample
    (matches should_skip_empty_gap).
    """
    win_lo, win_hi = window
    out = []
    for i, (t, v) in enumerate(samples):
        if win_lo <= t <= win_hi:
            out.append((t, _agg_sample_value(agg, v)))
        carry_t = t + bucket_size
        if carry_t <= win_hi and i + 1 < len(samples):
            next_v = samples[i + 1][1]
            out.append((carry_t, _agg_empty_value(agg, v, next_v)))
    return out


def _decode_pair(row):
    ts = int(row[0])
    val = row[1].decode() if isinstance(row[1], bytes) else row[1]
    return ts, val


def _format_redis_cli(pairs):
    """Format a list of (ts, value) pairs the way redis-cli prints them."""
    if not pairs:
        return '(empty array)'
    lines = []
    for i, (ts, val) in enumerate(pairs, start=1):
        lines.append(f'{i}) 1) (integer) {ts}')
        lines.append(f'   2) {val}')
    return '\n'.join(lines)


def _print_block(title, pairs):
    print(f'\n--- {title} ---')
    print(_format_redis_cli(pairs))


def _format_samples(samples):
    """Format input samples like ([t=10, v=100], [t=20, v=110], ...)."""
    inner = ', '.join(f'[t={t}, v={v}]' for t, v in samples)
    return f'({inner})'


def _print_samples(title, samples):
    print(f'\n--- {title} ({len(samples)} samples) ---')
    print(_format_samples(samples))


def test_range_revrange_empty_symmetry():
    """
    Forward and reverse aggregated EMPTY queries must be mirror images of
    each other (same bucket timestamps, reversed order). For aggregations
    listed in PER_AGG_VALUE_CHECKS the bucket values are also asserted.
    """
    samples = [(18, 100), (20, 110)]
    window  = (18, 22)
    bucket  = 1
    with Env().getClusterConnectionIfNeeded() as r:
        for agg in AGGREGATIONS:
            key = 'ts_sym_' + agg + '{a}'
            r.execute_command('DEL', key)
            r.execute_command('TS.CREATE', key)
            for t, v in samples:
                r.execute_command('TS.ADD', key, t, v)

            _print_samples(f'{agg} input samples', samples)

            win_lo, win_hi = window
            fwd = r.execute_command('TS.RANGE',    key, win_lo, win_hi,
                                    'AGGREGATION', agg, bucket, 'EMPTY')
            rev = r.execute_command('TS.REVRANGE', key, win_lo, win_hi,
                                    'AGGREGATION', agg, bucket, 'EMPTY')

            got_fwd = [_decode_pair(row) for row in fwd]
            got_rev = [_decode_pair(row) for row in rev]

            exp_fwd = _build_expected_forward(samples, bucket, window, agg)
            exp_rev = list(reversed(exp_fwd))

            _print_block(f'{agg} TS.RANGE actual',      got_fwd)
            _print_block(f'{agg} TS.REVRANGE actual',   got_rev)
            _print_block(f'{agg} TS.RANGE expected',    exp_fwd)
            _print_block(f'{agg} TS.REVRANGE expected', exp_rev)

            assert len(fwd) == len(rev), (
                f'{agg}: bucket count mismatch; '
                f'fwd={len(fwd)} ({fwd}), rev={len(rev)} ({rev})')

            fwd_ts = [ts for ts, _ in got_fwd]
            rev_ts = [ts for ts, _ in got_rev]
            assert fwd_ts == list(reversed(rev_ts)), (
                f'{agg}: timestamps not mirrored; '
                f'fwd={fwd_ts}, rev={rev_ts}')

            assert got_fwd == exp_fwd, (
                f'{agg} forward values mismatch.\n'
                f'expected:\n{_format_redis_cli(exp_fwd)}\n'
                f'actual:\n{_format_redis_cli(got_fwd)}')
            assert got_rev == exp_rev, (
                f'{agg} reverse values mismatch.\n'
                f'expected:\n{_format_redis_cli(exp_rev)}\n'
                f'actual:\n{_format_redis_cli(got_rev)}')


# ---------------------------------------------------------------------------
# Regression: same EMPTY-aggregation symmetry, but on a series whose samples
# span MANY chunks. The aggregation iterator's GetNext() loop must walk
# multiple chunks while still emitting the correct empty buckets in both
# directions and with correct carry values.
#
# We force tiny chunks (CHUNK_SIZE=48 bytes, uncompressed → 3 samples / chunk)
# and sparse samples so the empty buckets between samples land on chunk
# boundaries.
#
# Generic over AGGREGATIONS like the previous test; LAST also gets exact
# value-level assertions via MULTICHUNK_VALUE_CHECKS.
# ---------------------------------------------------------------------------

# Sample layout:  t in {10, 20, 30, ..., 300}, value = ts * 10.
# 30 samples / 3-per-chunk = 10 chunks.
_MC_SAMPLE_TS    = list(range(10, 301, 10))
_MC_SAMPLES      = [(t, t * 10) for t in _MC_SAMPLE_TS]
_MC_WINDOW       = (10, 300)
_MC_BUCKET_SIZE  = 5
_MC_CHUNK_BYTES  = 48     # uncompressed: 16 B/sample → 3 samples / chunk

def test_range_revrange_empty_symmetry_multichunk():
    """
    Same forward/reverse EMPTY-aggregation symmetry as the previous test,
    but the underlying series is split across ~10 chunks so the
    AggregationIterator GetNext loop is exercised across chunk boundaries.
    """
    with Env().getClusterConnectionIfNeeded() as r:
        for agg in AGGREGATIONS:
            key = 'ts_sym_mc_' + agg + '{a}'
            r.execute_command('DEL', key)
            r.execute_command('TS.CREATE', key,
                              'ENCODING', 'UNCOMPRESSED',
                              'CHUNK_SIZE', _MC_CHUNK_BYTES)
            for t, v in _MC_SAMPLES:
                r.execute_command('TS.ADD', key, t, v)

            # Sanity-check that we really got multiple chunks.
            info = r.execute_command('TS.INFO', key)
            info_dict = {info[i]: info[i + 1] for i in range(0, len(info), 2)}
            chunk_count_key = b'chunkCount' if b'chunkCount' in info_dict else 'chunkCount'
            chunk_count = int(info_dict[chunk_count_key])
            assert chunk_count > 1, (
                f'expected the series to span multiple chunks, got '
                f'chunkCount={chunk_count}; sample/chunk math may have changed')

            win_lo, win_hi = _MC_WINDOW
            fwd = r.execute_command('TS.RANGE',    key, win_lo, win_hi,
                                    'AGGREGATION', agg, _MC_BUCKET_SIZE, 'EMPTY')
            rev = r.execute_command('TS.REVRANGE', key, win_lo, win_hi,
                                    'AGGREGATION', agg, _MC_BUCKET_SIZE, 'EMPTY')

            got_fwd = [_decode_pair(row) for row in fwd]
            got_rev = [_decode_pair(row) for row in rev]

            exp_fwd = _build_expected_forward(
                _MC_SAMPLES, _MC_BUCKET_SIZE, _MC_WINDOW, agg)
            exp_rev = list(reversed(exp_fwd))

            print(f'\n--- {agg} multichunk: chunkCount={chunk_count}, '
                  f'samples={len(_MC_SAMPLES)}, buckets={len(exp_fwd)} ---')

            assert len(fwd) == len(rev), (
                f'{agg}: bucket count mismatch; '
                f'fwd={len(fwd)}, rev={len(rev)}')

            fwd_ts = [ts for ts, _ in got_fwd]
            rev_ts = [ts for ts, _ in got_rev]
            assert fwd_ts == list(reversed(rev_ts)), (
                f'{agg}: timestamps not mirrored across chunks; '
                f'fwd={fwd_ts}, rev={rev_ts}')

            assert got_fwd == exp_fwd, (
                f'{agg} forward values mismatch (multichunk).\n'
                f'expected:\n{_format_redis_cli(exp_fwd)}\n'
                f'actual:\n{_format_redis_cli(got_fwd)}')
            assert got_rev == exp_rev, (
                f'{agg} reverse values mismatch (multichunk).\n'
                f'expected:\n{_format_redis_cli(exp_rev)}\n'
                f'actual:\n{_format_redis_cli(got_rev)}')


# ---------------------------------------------------------------------------
# Regression: plain TS.RANGE / TS.REVRANGE (no AGGREGATION) must be exact
# mirror images of each other. Exercises the reply-time backward-index walk
# in ReplySeriesRange (which replaced the in-place buffer reverse).
# ---------------------------------------------------------------------------
def test_range_revrange_plain_symmetry():
    """
    Without any aggregation, TS.REVRANGE over the same window must return
    exactly the same samples as TS.RANGE, in reverse order.
    """
    samples = [(10, 1), (20, 2), (30, 3), (40, 4), (50, 5)]
    with Env().getClusterConnectionIfNeeded() as r:
        key = 'ts_plain_sym{a}'
        r.execute_command('DEL', key)
        r.execute_command('TS.CREATE', key)
        for t, v in samples:
            r.execute_command('TS.ADD', key, t, v)

        fwd = r.execute_command('TS.RANGE',    key, 10, 50)
        rev = r.execute_command('TS.REVRANGE', key, 10, 50)

        got_fwd = [_decode_pair(row) for row in fwd]
        got_rev = [_decode_pair(row) for row in rev]

        assert len(got_fwd) == len(samples), (
            f'fwd length wrong: got {got_fwd}')
        assert got_fwd == list(reversed(got_rev)), (
            f'plain mirror failed:\n'
            f'fwd={_format_redis_cli(got_fwd)}\n'
            f'rev={_format_redis_cli(got_rev)}')


# ---------------------------------------------------------------------------
# Regression: TS.REVRANGE ... COUNT N must return the LATEST N samples in
# reverse chronological order — i.e. it is the mirror image of TS.RANGE on
# the LAST N samples, not on the FIRST N. This pins the documented COUNT
# semantics for the reverse direction.
# ---------------------------------------------------------------------------
def test_range_revrange_plain_count_symmetry():
    """
    For samples [(10,1), (20,2), ..., (100,10)]:
      TS.RANGE    ... COUNT N → first N forward         = samples[:N]
      TS.REVRANGE ... COUNT N → latest N in reverse     = reversed(samples[-N:])
    """
    samples = [(t, t) for t in range(10, 101, 10)]
    with Env().getClusterConnectionIfNeeded() as r:
        key = 'ts_plain_cnt{a}'
        r.execute_command('DEL', key)
        r.execute_command('TS.CREATE', key)
        for t, v in samples:
            r.execute_command('TS.ADD', key, t, v)

        for n in (1, 3, 5, len(samples), len(samples) + 5):
            fwd = r.execute_command('TS.RANGE',    key, 10, 100, 'COUNT', n)
            rev = r.execute_command('TS.REVRANGE', key, 10, 100, 'COUNT', n)

            got_fwd = [_decode_pair(row) for row in fwd]
            got_rev = [_decode_pair(row) for row in rev]

            expected_fwd = [(t, str(v)) for t, v in samples[:n]]
            expected_rev = list(reversed([(t, str(v)) for t, v in samples[-n:]]))

            assert got_fwd == expected_fwd, (
                f'COUNT={n} fwd mismatch:\n'
                f'expected:\n{_format_redis_cli(expected_fwd)}\n'
                f'actual:\n{_format_redis_cli(got_fwd)}')
            assert got_rev == expected_rev, (
                f'COUNT={n} rev mismatch:\n'
                f'expected:\n{_format_redis_cli(expected_rev)}\n'
                f'actual:\n{_format_redis_cli(got_rev)}')


# ---------------------------------------------------------------------------
# Regression: aggregated TS.RANGE / TS.REVRANGE WITHOUT the EMPTY flag must
# still be mirror images of each other (same non-empty buckets, reversed
# order). Complements the EMPTY-flag tests above.
# ---------------------------------------------------------------------------
def test_range_revrange_no_empty_symmetry():
    """
    Sparse samples spaced beyond bucket_size so naturally-empty buckets
    exist; without EMPTY, neither direction should emit them.
    """
    samples = [(10, 1), (50, 5), (90, 9)]
    bucket  = 10
    with Env().getClusterConnectionIfNeeded() as r:
        for agg in AGGREGATIONS:
            key = 'ts_no_empty_' + agg + '{a}'
            r.execute_command('DEL', key)
            r.execute_command('TS.CREATE', key)
            for t, v in samples:
                r.execute_command('TS.ADD', key, t, v)

            fwd = r.execute_command('TS.RANGE',    key, 0, 100,
                                    'AGGREGATION', agg, bucket)
            rev = r.execute_command('TS.REVRANGE', key, 0, 100,
                                    'AGGREGATION', agg, bucket)

            got_fwd = [_decode_pair(row) for row in fwd]
            got_rev = [_decode_pair(row) for row in rev]

            assert len(got_fwd) == len(samples), (
                f'{agg}: expected one bucket per sample (no EMPTY flag), '
                f'got {got_fwd}')
            assert got_fwd == list(reversed(got_rev)), (
                f'{agg} no-EMPTY mirror failed:\n'
                f'fwd={_format_redis_cli(got_fwd)}\n'
                f'rev={_format_redis_cli(got_rev)}')


# ---------------------------------------------------------------------------
# Regression: TS.MRANGE / TS.MREVRANGE per-series mirror image. The set of
# returned series and their labels must be the same; the samples list per
# series must be reversed between the two directions.
# ---------------------------------------------------------------------------
def _decode_mrange_entry(entry):
    """Return (key_name_str, samples_as_list_of_(ts,val)_pairs) for one MRANGE entry.
    Tolerates the [key, labels, samples] layout regardless of byte vs str."""
    key = entry[0].decode() if isinstance(entry[0], bytes) else entry[0]
    samples = [_decode_pair(row) for row in entry[2]]
    return key, samples


def test_mrange_mrevrange_symmetry():
    """
    Two series with a shared label; MRANGE and MREVRANGE filtered on that
    label must return the same set of keys, each with its samples reversed.
    """
    samples = [(10, 1), (20, 2), (30, 3), (40, 4)]
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        keys = ['ts_m_a{a}', 'ts_m_b{a}']
        for i, key in enumerate(keys):
            r.execute_command('DEL', key)
            r.execute_command('TS.CREATE', key,
                              'LABELS', 'tag', 'mirror_sym', 'idx', str(i))
            for t, v in samples:
                r.execute_command('TS.ADD', key, t, v + i * 100)

        # MRANGE / MREVRANGE go through a specific shard connection — see docstring.
        shard_conn = env.getConnection(1)
        fwd = shard_conn.execute_command('TS.MRANGE',    '10', '40',
                                         'FILTER', 'tag=mirror_sym')
        rev = shard_conn.execute_command('TS.MREVRANGE', '10', '40',
                                         'FILTER', 'tag=mirror_sym')

        fwd_by_key = dict(_decode_mrange_entry(e) for e in fwd)
        rev_by_key = dict(_decode_mrange_entry(e) for e in rev)

        assert set(fwd_by_key) == set(rev_by_key) == set(keys), (
            f'key sets differ: fwd={set(fwd_by_key)}, '
            f'rev={set(rev_by_key)}, expected={set(keys)}')

        for key in keys:
            f_samples = fwd_by_key[key]
            r_samples = rev_by_key[key]
            assert len(f_samples) == len(samples), (
                f'{key}: fwd sample count wrong: {f_samples}')
            assert f_samples == list(reversed(r_samples)), (
                f'{key}: per-series mirror failed:\n'
                f'fwd={_format_redis_cli(f_samples)}\n'
                f'rev={_format_redis_cli(r_samples)}')


# ---------------------------------------------------------------------------
# TS.RANGE AGGREGATION coverage — keep aligned with the public docs at:
#   https://redis.io/docs/latest/commands/ts.range/
#
# The per-aggregator tests above (test_agg_*, test_ts_range_count{NaN,All})
# verify exact values for each aggregator on hand-tuned inputs. The four
# tests below add coverage that scales automatically to every aggregator
# listed in TS_RANGE_AGGREGATORS:
#
#   1. test_ts_range_all_aggregators_exact_values (positive)
#      Every aggregator returns the CORRECT value for a single bucket with
#      mixed numeric + NaN samples. Also verifies TS.REVRANGE mirrors
#      TS.RANGE (parser/aggregator share the path; this catches direction-
#      specific regressions).
#
#   2. test_ts_range_all_aggregators_empty_flag (positive, EMPTY flag)
#      Asserts the exact value reported for an EMPTY bucket sandwiched
#      between two samples, for every aggregator. The expected values
#      come from the public docs (the EMPTY-flag table on
#      https://redis.io/docs/latest/commands/ts.range/) plus the
#      standard mathematical definitions for aggregators that the docs
#      do not list explicitly (variance of an empty set → NaN, etc.).
#      twa's empty-bucket value is the time-weighted average of the
#      linear interpolation between the surrounding samples (also docs).
#
#   3. test_ts_range_invalid_aggregator_bucket_duration (negative)
#      Every aggregator rejects malformed `bucketDuration`. Catches new
#      aggregators that forget the shared validation. Also probes
#      TS.REVRANGE to make sure both directions agree.
#
#   4. test_ts_range_unknown_aggregator (negative)
#      Unknown / typo aggregator names are rejected. Includes a check for
#      `AGGREGATION` with no aggregator name and `AGGREGATION x dur EMPTY`
#      with an unknown name (EMPTY must not mask the unknown-name error).
# ---------------------------------------------------------------------------

# Full list per https://redis.io/docs/latest/commands/ts.range/ (AGGREGATION
# block). Adding a new aggregator? Add it here and AGG_SINGLE_BUCKET_EXPECTED
# + AGG_EMPTY_BUCKET_EXPECTED below, then both the positive and the negative
# tests will cover it automatically.
TS_RANGE_AGGREGATORS = [
    'avg', 'sum', 'min', 'max', 'range', 'count', 'countNaN', 'countAll',
    'first', 'last', 'std.p', 'std.s', 'var.p', 'var.s', 'twa',
]

# Expected values for AGGREGATION <agg> 100 over a single bucket [0, 100)
# containing samples: (10, 10), (20, NaN), (30, 20).
# Non-NaN values = [10, 20], mean = 15, n = 2.
#   var_p = ((10-15)^2 + (20-15)^2) / 2 = 25
#   var_s = ((10-15)^2 + (20-15)^2) / 1 = 50
# twa is excluded — single-bucket twa depends on extrapolation policy at the
# bucket edges and is exercised by the dedicated test_agg_twa above.
AGG_SINGLE_BUCKET_EXPECTED = {
    'avg':      15.0,
    'sum':      30.0,
    'min':      10.0,
    'max':      20.0,
    'range':    10.0,                        # max - min (non-NaN)
    'count':    2.0,                         # non-NaN count
    'countNaN': 1.0,
    'countAll': 3.0,
    'first':    10.0,                        # value at lowest ts (non-NaN)
    'last':     20.0,                        # value at highest ts (non-NaN)
    'std.p':    5.0,                         # sqrt(var_p)
    'std.s':    math.sqrt(50.0),             # sqrt(var_s)
    'var.p':    25.0,
    'var.s':    50.0,
}

# Expected empty-bucket value for an EMPTY bucket sandwiched between two
# samples (10, 100) and (30, 200), bucketDuration=10, ALIGN 0 → the empty
# bucket is [20, 30). Values are STRINGS so we can assert 'NaN' literally.
#
# These expectations come from the public docs AND from the mathematical
# definition of each aggregator over the empty set:
#   - sum, count, countNaN, countAll: aggregator counts/sums over zero
#     samples → 0 (docs explicitly state this for sum/count; countNaN
#     and countAll are counts of subsets of an empty set → also 0).
#   - last:  docs say "value of the last sample BEFORE the bucket's
#     start" → 100 (the sample at t=10).
#   - min, max, range, avg, first, std.p, std.s: docs explicitly say NaN.
#   - var.p, var.s: variance over the empty set is mathematically
#     undefined → NaN (docs don't list these; the math/sense answer is
#     NaN, consistent with the std.p/std.s row from the docs).
#   - twa: docs say "average value over the bucket's timeframe based on
#     linear interpolation". With surrounding samples (10, 100) and
#     (30, 200), the line value at t=20 is 150 and at t=30 is 200, so
#     the time-weighted average over [20, 30) is (150 + 200) / 2 = 175.
AGG_EMPTY_BUCKET_EXPECTED = {
    'sum':      '0',
    'count':    '0',
    'countNaN': '0',
    'countAll': '0',
    'last':     '100',                       # previous sample's value
    'min':      'NaN',
    'max':      'NaN',
    'range':    'NaN',
    'avg':      'NaN',
    'first':    'NaN',
    'std.p':    'NaN',
    'std.s':    'NaN',
    'var.p':    'NaN',
    'var.s':    'NaN',
    # 'twa' handled separately — expected value 175 derived above.
}


def _agg_value_to_float(val):
    """Aggregator response value → Python float (handles bytes/str/NaN)."""
    s = val.decode() if isinstance(val, bytes) else val
    return float(s)


def test_ts_range_all_aggregators_exact_values():
    """
    Positive: for every aggregator from the TS.RANGE docs, the value
    returned over a single bucket containing mixed numeric + NaN samples
    must match the hand-computed expected value.

    Also verifies TS.REVRANGE returns the same single bucket (direction
    must not change the per-bucket value).

    Coverage rationale (vs the existing test_agg_* tests):
      - This is a single source-of-truth table for the per-aggregator
        contract; new aggregators wire themselves in via the dict.
      - Adds the NaN dimension to the table — only countNaN / countAll /
        the dedicated test_ts_range_NaN_values exercise this today.
    """
    with Env(decodeResponses=True).getClusterConnectionIfNeeded() as r:
        key = 'agg_exact{a}'
        r.execute_command('TS.CREATE', key)
        r.execute_command('TS.MADD',
                          key, 10, 10,
                          key, 20, 'nan',
                          key, 30, 20)

        # Sanity: every aggregator in the docs list has an expected value
        # (or, for twa, an explicit exemption documented in the table).
        covered = set(AGG_SINGLE_BUCKET_EXPECTED.keys()) | {'twa'}
        missing = set(TS_RANGE_AGGREGATORS) - covered
        assert not missing, \
            f'AGG_SINGLE_BUCKET_EXPECTED missing aggregators: {sorted(missing)}'

        TOL = 1e-6
        for agg, exp in AGG_SINGLE_BUCKET_EXPECTED.items():
            fwd = r.execute_command(
                'TS.RANGE', key, 0, '+', 'AGGREGATION', agg, 100)
            rev = r.execute_command(
                'TS.REVRANGE', key, 0, '+', 'AGGREGATION', agg, 100)
            assert len(fwd) == 1, \
                f'AGGREGATION {agg}: expected 1 bucket fwd, got {fwd!r}'
            assert len(rev) == 1, \
                f'AGGREGATION {agg}: expected 1 bucket rev, got {rev!r}'
            assert fwd[0][0] == 0 == rev[0][0], (
                f'AGGREGATION {agg}: bucket ts mismatch '
                f'fwd={fwd[0][0]} rev={rev[0][0]}')

            got_fwd = _agg_value_to_float(fwd[0][1])
            got_rev = _agg_value_to_float(rev[0][1])
            assert abs(got_fwd - exp) < TOL, (
                f'AGGREGATION {agg} TS.RANGE: expected {exp}, got {got_fwd}')
            assert abs(got_rev - exp) < TOL, (
                f'AGGREGATION {agg} TS.REVRANGE: expected {exp}, got {got_rev}')

        # twa: single-bucket value depends on the edge-extrapolation policy.
        # Just assert the call succeeds and returns a finite number for this
        # input (two non-NaN samples in the same bucket).
        fwd = r.execute_command(
            'TS.RANGE', key, 0, '+', 'AGGREGATION', 'twa', 100)
        assert len(fwd) == 1, \
            f'AGGREGATION twa: expected 1 bucket, got {fwd!r}'
        twa = _agg_value_to_float(fwd[0][1])
        assert math.isfinite(twa), \
            f'AGGREGATION twa: expected finite value, got {twa}'


def test_ts_range_all_aggregators_empty_flag():
    """
    Positive (EMPTY flag): for every aggregator from the TS.RANGE docs,
    assert the exact value reported for an EMPTY bucket sandwiched between
    two samples.

    Setup: samples at (10, 100) and (30, 200), bucketDuration=10, ALIGN 0.
    Window [0, 39] yields buckets [10,20), [20,30), [30,40). The bucket at
    ts=20 is EMPTY (between samples). The bucket at ts=0 is dropped because
    no sample precedes it (should_skip_empty_gap).

    Also verifies TS.REVRANGE returns the same set of buckets in reverse.
    """
    with Env(decodeResponses=True).getClusterConnectionIfNeeded() as r:
        key = 'agg_empty{a}'
        r.execute_command('TS.CREATE', key)
        r.execute_command('TS.MADD',
                          key, 10, 100,
                          key, 30, 200)

        # Sanity: every aggregator in the docs list has an expected value
        # (or, for twa, an explicit exemption documented in the table).
        covered = set(AGG_EMPTY_BUCKET_EXPECTED.keys()) | {'twa'}
        missing = set(TS_RANGE_AGGREGATORS) - covered
        assert not missing, \
            f'AGG_EMPTY_BUCKET_EXPECTED missing aggregators: {sorted(missing)}'

        for agg, exp_str in AGG_EMPTY_BUCKET_EXPECTED.items():
            fwd = r.execute_command(
                'TS.RANGE', key, 0, 39, 'ALIGN', 0,
                'AGGREGATION', agg, 10, 'EMPTY')
            rev = r.execute_command(
                'TS.REVRANGE', key, 0, 39, 'ALIGN', 0,
                'AGGREGATION', agg, 10, 'EMPTY')

            # Direction symmetry: same buckets, reverse order.
            assert fwd == list(reversed(rev)), (
                f'AGGREGATION {agg} EMPTY: TS.RANGE / TS.REVRANGE are not '
                f'mirror images\n  fwd={fwd!r}\n  rev={rev!r}')

            fwd_by_ts = {row[0]: row[1] for row in fwd}
            assert set(fwd_by_ts) == {10, 20, 30}, (
                f'AGGREGATION {agg} EMPTY: expected buckets at 10/20/30, '
                f'got {sorted(fwd_by_ts)} (full: {fwd!r})')
            assert fwd_by_ts[20] == exp_str, (
                f'AGGREGATION {agg} EMPTY bucket: expected {exp_str!r}, '
                f'got {fwd_by_ts[20]!r}')

        # twa empty bucket: linear interpolation between (10, 100) and
        # (30, 200). Bucket [20, 30): interp at t=20 = 150, at t=30 = 200,
        # time-weighted avg over [20, 30) = (150 + 200) / 2 = 175.
        fwd = r.execute_command(
            'TS.RANGE', key, 0, 39, 'ALIGN', 0,
            'AGGREGATION', 'twa', 10, 'EMPTY')
        twa_by_ts = {row[0]: row[1] for row in fwd}
        assert 20 in twa_by_ts, \
            f'AGGREGATION twa EMPTY: bucket ts=20 missing from {fwd!r}'
        twa_empty = _agg_value_to_float(twa_by_ts[20])
        assert abs(twa_empty - 175.0) < 1e-6, (
            f'AGGREGATION twa EMPTY bucket: expected 175.0, got {twa_empty}')


def test_ts_range_invalid_aggregator_bucket_duration():
    """
    Negative: for every aggregator from the TS.RANGE docs, malformed
    `bucketDuration` must be rejected with a ResponseError, for BOTH
    TS.RANGE and TS.REVRANGE (the parser path is shared but worth
    cross-checking). Covers the full set of parser failure modes:
      - missing entirely
      - non-numeric ('foo')
      - empty string ('')
      - lone whitespace (' ')
      - decimal ('1.5')           — bucketDuration is integer-only
      - NaN / inf string literals
      - massive overflow that won't fit in a long long
      - negative integer (-1)
    """
    bad_bucket_durations = [
        'foo',
        '',
        ' ',
        '1.5',
        'nan',
        'inf',
        '99999999999999999999999999999',
        -1,
    ]

    with Env().getClusterConnectionIfNeeded() as r:
        key = 'agg_neg_bucket{a}'
        r.execute_command('TS.CREATE', key)
        r.execute_command('TS.ADD', key, 10, 1)

        for cmd in ('TS.RANGE', 'TS.REVRANGE'):
            for agg in TS_RANGE_AGGREGATORS:
                # bucketDuration missing entirely
                with pytest.raises(redis.ResponseError):
                    r.execute_command(cmd, key, 0, '+', 'AGGREGATION', agg)
                for bd in bad_bucket_durations:
                    with pytest.raises(redis.ResponseError):
                        r.execute_command(
                            cmd, key, 0, '+', 'AGGREGATION', agg, bd)


def test_ts_range_unknown_aggregator():
    """
    Negative: unknown aggregator names must be rejected. Includes the empty
    string, an obvious garbage token, and a typo of a real aggregator —
    guards against the parser silently accepting close-misses.

    Also exercises a few structural negatives that aren't tied to a name:
      - `AGGREGATION` with no aggregator and no bucketDuration at all
      - `AGGREGATION <unknown> 100 EMPTY` — EMPTY must not mask the
        unknown-name error
    """
    bad_names = [
        '',                          # empty string
        'foo',                       # never been an aggregator
        'avgg',                      # typo of 'avg'
        'aggregation',               # the keyword itself
        'not_aggregation_function',  # historical placeholder
    ]
    with Env().getClusterConnectionIfNeeded() as r:
        key = 'agg_unknown{a}'
        r.execute_command('TS.CREATE', key)
        r.execute_command('TS.ADD', key, 10, 1)

        for name in bad_names:
            with pytest.raises(redis.ResponseError):
                r.execute_command(
                    'TS.RANGE', key, 0, '+', 'AGGREGATION', name, 100)
            with pytest.raises(redis.ResponseError):
                r.execute_command(
                    'TS.REVRANGE', key, 0, '+', 'AGGREGATION', name, 100)
            # EMPTY must not mask the unknown-aggregator error.
            with pytest.raises(redis.ResponseError):
                r.execute_command(
                    'TS.RANGE', key, 0, '+',
                    'AGGREGATION', name, 100, 'EMPTY')

        # `AGGREGATION` with nothing after it must fail.
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.RANGE', key, 0, '+', 'AGGREGATION')


# ---------------------------------------------------------------------------
# Edge-case tests for TS.RANGE AGGREGATION.
#
# Each of these tests targets a corner case that the broader exact-values /
# EMPTY tests above don't reach. Expected values are derived from the
# public docs and from standard mathematical definitions. Where the docs
# leave a corner case undefined (single-sample twa, single-sample sample
# variance, etc.) the test either asserts the math-correct answer (so a
# code bug surfaces) or leaves the value unpinned — never silently
# encodes what the current implementation happens to return.
# ---------------------------------------------------------------------------

# Bucket [0, 100) with one sample (50, 42). Expectations are derived
# strictly from per-aggregator docs definitions + standard math:
#
#   sum, avg, min, max, first, last over {42}         → 42
#   range = max - min over {42}                       → 0
#   count (non-NaN), countAll over {42}               → 1
#   countNaN over {42}                                → bucket is "empty for
#       countNaN" (zero NaN samples passed countNaN's isValueValid filter),
#       so per the drop rule the bucket is OMITTED without EMPTY and emits
#       0 with EMPTY. This is the symmetric twin of "count over an all-NaN
#       bucket → bucket dropped without EMPTY" already pinned by
#       test_ts_range_count_family_on_all_nan_bucket. countNaN therefore
#       cannot live in the "always emits one bucket" dict below and is
#       asserted separately at the end of the test function.
#   std.p, var.p (population) over {42}:
#       variance = E[(X-μ)²] with μ=42, X=42          → 0
#       std      = sqrt(0)                            → 0
#   std.s, var.s (sample) over {42}:
#       Formal math: SS / (n-1) = 0 / 0 → NaN (undefined).
#       Implementation choice (compaction.c VarSamplesFinalize):
#       explicit `if (count == 1) *value = 0;`. The docs do not
#       specify n=1, so the shipping behaviour is "n=1 → 0" and
#       has been for years. We pin that here. If the implementation
#       ever switches to NaN, update both this expectation and the
#       public docs to call out the change.
#   twa over {(50, 42)} in bucket [0, 100):
#       docs say "time-weighted average over the bucket's timeframe".
#       A single sample defines a constant function (→ 42) under one
#       reading and is undefined (→ NaN) under another. The docs do
#       not disambiguate, so this test does NOT pin an exact value
#       for twa — it only asserts the call succeeds and returns a
#       number (NaN or finite). The dedicated test_agg_twa above
#       exercises the multi-sample twa math.
AGG_SINGLE_SAMPLE_BUCKET_EXPECTED = {
    'avg':      42.0,
    'sum':      42.0,
    'min':      42.0,
    'max':      42.0,
    'range':    0.0,
    'count':    1.0,
    # countNaN is intentionally absent — see comment above and the dedicated
    # block at the end of test_ts_range_all_aggregators_single_sample_bucket.
    'countAll': 1.0,
    'first':    42.0,
    'last':     42.0,
    'std.p':    0.0,
    'var.p':    0.0,
    'std.s':    0.0,                          # n=1 special-cased to 0 by impl
    'var.s':    0.0,                          # n=1 special-cased to 0 by impl
}


def test_ts_range_all_aggregators_single_sample_bucket():
    """
    Edge case (positive): bucket containing a single sample.

    Expected values are derived from each aggregator's documented
    semantics, standard mathematical definitions, and — where the docs
    leave a corner case open — the shipping implementation's choice:
      - Population variance/std over {x} is 0 by definition.
      - Sample variance/std over {x} is mathematically undefined
        (SS / (n-1) = 0 / 0), but compaction.c::VarSamplesFinalize
        explicitly returns 0 for n=1. We pin that here; if the impl
        ever switches to NaN, update this expectation and the public
        docs together.
      - twa over a single sample is ambiguous per docs and is therefore
        only verified to return SOMETHING parseable (not pinned to a
        specific value).
    """
    with Env(decodeResponses=True).getClusterConnectionIfNeeded() as r:
        key = 'agg_single{a}'
        r.execute_command('TS.CREATE', key)
        r.execute_command('TS.ADD', key, 50, 42)

        # Coverage guard. countNaN is asserted separately (drop-without-EMPTY
        # behavior) and twa is asserted separately (docs don't pin n=1).
        covered = set(AGG_SINGLE_SAMPLE_BUCKET_EXPECTED) | {'twa', 'countNaN'}
        missing = set(TS_RANGE_AGGREGATORS) - covered
        assert not missing, \
            f'AGG_SINGLE_SAMPLE_BUCKET_EXPECTED missing: {sorted(missing)}'

        TOL = 1e-6
        for agg, exp in AGG_SINGLE_SAMPLE_BUCKET_EXPECTED.items():
            fwd = r.execute_command(
                'TS.RANGE', key, 0, '+', 'AGGREGATION', agg, 100)
            rev = r.execute_command(
                'TS.REVRANGE', key, 0, '+', 'AGGREGATION', agg, 100)
            assert len(fwd) == 1, \
                f'AGGREGATION {agg} n=1: expected 1 bucket fwd, got {fwd!r}'
            assert len(rev) == 1, \
                f'AGGREGATION {agg} n=1: expected 1 bucket rev, got {rev!r}'
            for label, res in (('TS.RANGE', fwd), ('TS.REVRANGE', rev)):
                got = _agg_value_to_float(res[0][1])
                if math.isnan(exp):
                    assert math.isnan(got), (
                        f'AGGREGATION {agg} n=1 {label}: expected NaN '
                        f'(sample variance undefined for n=1), got {got}')
                else:
                    assert abs(got - exp) < TOL, (
                        f'AGGREGATION {agg} n=1 {label}: '
                        f'expected {exp}, got {got}')

        # twa: docs don't specify single-sample behaviour → only assert
        # the call succeeds and returns a parseable number.
        for cmd in ('TS.RANGE', 'TS.REVRANGE'):
            res = r.execute_command(
                cmd, key, 0, '+', 'AGGREGATION', 'twa', 100)
            assert len(res) == 1, \
                f'AGGREGATION twa n=1 {cmd}: expected 1 bucket, got {res!r}'
            # Will raise if not parseable; NaN is fine.
            _agg_value_to_float(res[0][1])

        # countNaN: zero NaN samples passed countNaN's isValueValid filter,
        # so the bucket is "empty for countNaN" and is dropped without
        # EMPTY (symmetric twin of count-on-all-NaN in
        # test_ts_range_count_family_on_all_nan_bucket). With EMPTY, the
        # bucket is emitted with value 0.
        for cmd in ('TS.RANGE', 'TS.REVRANGE'):
            res = r.execute_command(
                cmd, key, 0, '+', 'AGGREGATION', 'countNaN', 100)
            assert res == [], (
                f'AGGREGATION countNaN n=1 {cmd} without EMPTY: '
                f'expected [] (bucket dropped, mirror of count-on-all-NaN), '
                f'got {res!r}')
            res = r.execute_command(
                cmd, key, 0, '+', 'AGGREGATION', 'countNaN', 100, 'EMPTY')
            assert res == [[0, '0']], (
                f'AGGREGATION countNaN n=1 {cmd} with EMPTY: '
                f'expected [[0, "0"]], got {res!r}')


def test_ts_range_count_family_on_all_nan_bucket():
    """
    Edge case (positive): countNaN and countAll on an all-NaN bucket.

    Fills the gap left by test_ts_range_NaN_values (which covers all
    non-count aggregators on all-NaN buckets, but only the legacy
    `count` from the count family). Expectations are derived from the
    per-aggregator docs definitions:
      - count    = "Number of non-NaN values"               → 0 (bucket
                   has zero non-NaN values, so the bucket is "empty"
                   for count and is dropped without the EMPTY flag).
      - countNaN = "Number of NaN values"                   → N (the
                   bucket has N NaN samples, so it is NOT empty for
                   countNaN and must be reported even without EMPTY).
      - countAll = "Number of values, including NaN and non-NaN" → N
                   (same reasoning as countNaN).

    Setup: three NaN samples (10, 20, 30), one non-NaN sample (200, 5)
    in a different bucket so we can also check that the count family
    reports the all-NaN bucket distinctly from the populated one.
    """
    with Env(decodeResponses=True).getClusterConnectionIfNeeded() as r:
        key = 'agg_allnan{a}'
        r.execute_command('TS.CREATE', key)
        r.execute_command('TS.MADD',
                          key, 10, 'nan',
                          key, 20, 'nan',
                          key, 30, 'nan',
                          key, 200, 5)

        # All-NaN bucket [0, 100): query just that bucket.
        # No EMPTY flag. Per docs, countNaN counts the NaN samples and
        # countAll counts all samples — both see 3 valid items, so the
        # bucket is NOT empty for them and must be reported even without
        # the EMPTY flag.
        for agg, expected in (('countNaN', '3'), ('countAll', '3')):
            res = r.execute_command(
                'TS.RANGE', key, 0, 99, 'AGGREGATION', agg, 100)
            assert res == [[0, expected]], \
                f'AGGREGATION {agg} all-NaN bucket: got {res!r}'

        # By contrast, `count` is defined per docs as "Number of non-NaN
        # values" → 0 for an all-NaN bucket → the bucket is empty for
        # count and must be omitted without the EMPTY flag. This locks
        # in the per-aggregator asymmetry implied by the docs.
        res = r.execute_command(
            'TS.RANGE', key, 0, 99, 'AGGREGATION', 'count', 100)
        assert res == [], \
            f'AGGREGATION count all-NaN bucket without EMPTY: expected [], got {res!r}'

        # With EMPTY, count emits the bucket with value 0.
        res = r.execute_command(
            'TS.RANGE', key, 0, 99, 'AGGREGATION', 'count', 100, 'EMPTY')
        assert res == [[0, '0']], \
            f'AGGREGATION count all-NaN bucket with EMPTY: got {res!r}'

        # Across both buckets, countAll should report 3 NaN + 1 non-NaN.
        res = r.execute_command(
            'TS.RANGE', key, 0, 299, 'AGGREGATION', 'countAll', 100)
        # Bucket [0,100): 3 NaN samples → 3. Bucket [200,300): 1 → 1.
        # Bucket [100,200) has no samples → dropped (no EMPTY).
        assert res == [[0, '3'], [200, '1']], \
            f'AGGREGATION countAll multi-bucket: got {res!r}'


def test_ts_range_aggregator_name_case_insensitivity():
    """
    Edge case (parser): aggregator names are case-insensitive. For every
    aggregator from the docs, the lowercase, UPPERCASE and MiXeD-CaSe
    forms must all produce the same response.

    Catches drift in the parser's keyword table — e.g. if a new aggregator
    is added with a case-sensitive comparison instead of the shared
    case-insensitive matcher.
    """
    def mixed_case(s):
        # Toggle case per character; leaves '.' and digits untouched.
        return ''.join(c.upper() if i % 2 == 0 else c.lower()
                       for i, c in enumerate(s))

    with Env(decodeResponses=True).getClusterConnectionIfNeeded() as r:
        key = 'agg_case{a}'
        r.execute_command('TS.CREATE', key)
        r.execute_command('TS.MADD',
                          key, 10, 10,
                          key, 20, 'nan',
                          key, 30, 20)

        for agg in TS_RANGE_AGGREGATORS:
            forms = [agg.lower(), agg.upper(), mixed_case(agg)]
            results = [
                r.execute_command(
                    'TS.RANGE', key, 0, '+', 'AGGREGATION', form, 100)
                for form in forms
            ]
            # All three forms must produce IDENTICAL responses.
            assert results[0] == results[1] == results[2], (
                f'AGGREGATION case-insensitivity broken for {agg!r}:\n'
                f'  lower={forms[0]!r} → {results[0]!r}\n'
                f'  UPPER={forms[1]!r} → {results[1]!r}\n'
                f'  Mixed={forms[2]!r} → {results[2]!r}'
            )
            # And must not be an empty response (sanity: we set up data).
            assert len(results[0]) >= 1, \
                f'AGGREGATION {agg}: empty response for all forms'


# Multi-bucket composition: AGGREGATION + ALIGN + FILTER_BY_VALUE + COUNT.
# Per docs:
#   - FILTER_BY_VALUE filters samples BEFORE aggregation.
#   - AGGREGATION buckets the surviving samples.
#   - COUNT limits the number of REPORTED BUCKETS (not samples).
#   - TS.RANGE iterates ascending; TS.REVRANGE iterates descending.
#     "COUNT" in REVRANGE therefore yields the buckets with the LARGEST
#     timestamps (the first N in reverse-iteration order).
#
# Series: ten samples at t=5,15,25,...,95 with values equal to their ts.
# FILTER_BY_VALUE 20 80 keeps t=25,35,45,55,65,75. bucketDuration=20,
# ALIGN 0 → surviving buckets after the filter pipeline:
#   [20,40) holds (25, 35)   ← values [25, 35]
#   [40,60) holds (45, 55)   ← values [45, 55]
#   [60,80) holds (65, 75)   ← values [65, 75]
# Buckets [0,20) and [80,100) have no surviving samples and are dropped.
#
# Per-aggregator per-bucket values are computed from the surviving samples
# of each bucket using the standard definitions cited in the TS.RANGE docs:
#   sum=Σv, avg=Σv/n, min/max/first/last=obvious, range=max-min,
#   count=count_all=2 (no NaN),
#   var_p=Σ(v-μ)²/n, var_s=Σ(v-μ)²/(n-1), std=√var.
# countNaN is asserted separately below — zero NaN samples per bucket means
# every bucket is "empty for countNaN" and dropped without EMPTY (same drop
# rule that test_ts_range_count_family_on_all_nan_bucket pins for count on
# the symmetric all-NaN case).
# Each bucket here has values [x, x+10] with mean=x+5, so SS=50 in every
# bucket → var.p=25, var.s=50, std.p=5, std.s=√50 across the board.
AGG_COMPOSITION_BUCKET_VALUES = {
    'avg':      {20: 30.0, 40: 50.0,  60: 70.0},
    'sum':      {20: 60.0, 40: 100.0, 60: 140.0},
    'min':      {20: 25.0, 40: 45.0,  60: 65.0},
    'max':      {20: 35.0, 40: 55.0,  60: 75.0},
    'range':    {20: 10.0, 40: 10.0,  60: 10.0},
    'count':    {20:  2.0, 40:  2.0,  60:  2.0},
    # countNaN is intentionally absent — see comment above; asserted
    # separately at the end of the test function.
    'countAll': {20:  2.0, 40:  2.0,  60:  2.0},
    'first':    {20: 25.0, 40: 45.0,  60: 65.0},
    'last':     {20: 35.0, 40: 55.0,  60: 75.0},
    'std.p':    {20:  5.0, 40:  5.0,  60:  5.0},
    'std.s':    {20: math.sqrt(50.0), 40: math.sqrt(50.0), 60: math.sqrt(50.0)},
    'var.p':    {20: 25.0, 40: 25.0,  60: 25.0},
    'var.s':    {20: 50.0, 40: 50.0,  60: 50.0},
}


def test_ts_range_aggregator_composition_align_filter_count():
    """
    Edge case (composition): AGGREGATION pipelined with ALIGN +
    FILTER_BY_VALUE + COUNT. Asserts EXACT per-aggregator per-bucket
    values for both TS.RANGE and TS.REVRANGE.

    Per the docs ("samples are filtered before being aggregated" and
    "COUNT ... limits the number of reported buckets"):
      - TS.RANGE with COUNT 2 yields the two SMALLEST-ts surviving
        buckets, in ascending order: ts=20 then ts=40.
      - TS.REVRANGE with COUNT 2 yields the two LARGEST-ts surviving
        buckets, in descending order: ts=60 then ts=40.

    Twa is excluded from the per-value table: the docs say twa
    interpolates based on "the last sample before the bucket's start"
    and "the first sample after the bucket's end", and the docs do not
    specify whether samples removed by FILTER_BY_VALUE remain visible
    to that interpolation. This test should not pin a value that the
    docs leave open.
    """
    with Env(decodeResponses=True).getClusterConnectionIfNeeded() as r:
        key = 'agg_compose{a}'
        r.execute_command('TS.CREATE', key)
        for t in range(5, 100, 10):              # 5, 15, 25, ..., 95
            r.execute_command('TS.ADD', key, t, t)

        # Coverage guard. countNaN is asserted separately (drop-without-EMPTY
        # behavior — zero NaN samples per bucket); twa is documented as
        # out-of-scope for this composition test (interpolation visibility
        # past FILTER_BY_VALUE is unspecified).
        covered = set(AGG_COMPOSITION_BUCKET_VALUES) | {'twa', 'countNaN'}
        missing = set(TS_RANGE_AGGREGATORS) - covered
        assert not missing, \
            f'AGG_COMPOSITION_BUCKET_VALUES missing: {sorted(missing)}'

        TOL = 1e-6

        def _check(label, command, expected_ts_in_order, agg, per_bucket):
            res = r.execute_command(
                command, key, 0, '+',
                'FILTER_BY_VALUE', 20, 80,
                'COUNT', 2,
                'ALIGN', 0, 'AGGREGATION', agg, 20)
            assert len(res) == 2, (
                f'AGGREGATION {agg} {label}: expected 2 buckets, '
                f'got {res!r}')
            for exp_ts, row in zip(expected_ts_in_order, res):
                assert row[0] == exp_ts, (
                    f'AGGREGATION {agg} {label}: '
                    f'bucket ts {row[0]} != {exp_ts}')
                got = float(row[1])
                exp_val = per_bucket[exp_ts]
                assert abs(got - exp_val) < TOL, (
                    f'AGGREGATION {agg} {label} bucket {exp_ts}: '
                    f'expected {exp_val}, got {got}')

        for agg, per_bucket in AGG_COMPOSITION_BUCKET_VALUES.items():
            _check('TS.RANGE',    'TS.RANGE',    [20, 40], agg, per_bucket)
            _check('TS.REVRANGE', 'TS.REVRANGE', [60, 40], agg, per_bucket)

        # countNaN: every surviving bucket has only non-NaN samples → each
        # bucket is "empty for countNaN" → dropped without EMPTY (symmetric
        # twin of count-on-all-NaN pinned by
        # test_ts_range_count_family_on_all_nan_bucket). Just lock in the
        # drop here; the exact-value flavor is pinned by
        # test_ts_range_all_aggregators_single_sample_bucket.
        for label, command in (('TS.RANGE', 'TS.RANGE'),
                               ('TS.REVRANGE', 'TS.REVRANGE')):
            res = r.execute_command(
                command, key, 0, '+',
                'FILTER_BY_VALUE', 20, 80,
                'COUNT', 2,
                'ALIGN', 0, 'AGGREGATION', 'countNaN', 20)
            assert res == [], (
                f'AGGREGATION countNaN {label} without EMPTY: '
                f'expected [] (every bucket has 0 NaN samples), '
                f'got {res!r}')


def test_ts_range_aggregator_empty_no_preceding_sample():
    """
    Edge case (EMPTY flag): when an EMPTY bucket has no PRECEDING non-NaN
    sample, LAST and TWA must report NaN per docs ("NaN when no such
    sample"). This complements test_ts_range_all_aggregators_empty_flag
    above, where every EMPTY bucket DID have a preceding sample.

    Setup: a NaN sample at t=100 (so the bucket [100, 200) is non-empty
    from the storage layer's perspective and won't be skipped by
    should_skip_empty_gap, but LAST/TWA see no valid prior sample),
    followed by a valid sample at t=200.
    """
    with Env(decodeResponses=True).getClusterConnectionIfNeeded() as r:
        for agg in ('last', 'twa'):
            key = f'agg_emp_no_prev_{agg.replace(".", "_")}{{a}}'
            r.execute_command('TS.CREATE', key)
            r.execute_command('TS.ADD', key, 100, 'nan')
            r.execute_command('TS.ADD', key, 200, 50)

            res = r.execute_command(
                'TS.RANGE', key, 100, 199,
                'AGGREGATION', agg, 100, 'EMPTY')
            assert len(res) == 1, \
                f'AGGREGATION {agg}: expected 1 bucket, got {res!r}'
            got = float(res[0][1])
            assert math.isnan(got), (
                f'AGGREGATION {agg} EMPTY no-prior-sample: '
                f'expected NaN, got {got}')


def test_revrange_empty_count_matches_range_tail():
    """
    TS.REVRANGE + EMPTY + COUNT N must return the LAST N emitted buckets in
    reverse chronological order. The ground truth comes from TS.RANGE without
    COUNT (untouched, well-established forward path): the expected REVRANGE+COUNT
    result is the last N entries of that forward result, reversed.

    Covered:
      - N strictly less than total emitted buckets
      - N == 1 (smallest meaningful slice)
      - N exactly equal to total emitted bucket count
      - N larger than total emitted bucket count (must return all, no padding)
      - Sparse-leading data: empty buckets at the start of the window are
        "outside data span" and dropped by the EMPTY rule; the count must apply
        AFTER that drop so the response always reflects real, emitted buckets.
      - Mid-series gap: empty buckets that ARE inside the data span are emitted
        and must be eligible to appear in the COUNT-N tail.

    Every assertion is derived from TS.RANGE on the same key/window, so the
    test stays a true black-box check of the REVRANGE+EMPTY+COUNT semantics.
    """
    SCENARIOS = (
        # (label, samples, window, bucket_duration)
        ('dense',         [(t, t) for t in range(0, 100, 10)],   (0, 99),   10),
        ('sparse-lead',   [(50, 50), (60, 60), (70, 70),
                           (80, 80), (90, 90)],                  (0, 99),   10),
        ('mid-gap',       [(0, 0),  (10, 10), (20, 20),
                           (70, 70), (80, 80), (90, 90)],        (0, 99),   10),
        ('single-sample', [(42, 42)],                            (0, 99),   10),
    )
    AGGS = ('last', 'first', 'max', 'min', 'count', 'sum', 'avg', 'range')

    with Env(decodeResponses=True).getClusterConnectionIfNeeded() as r:
        for label, samples, (win_lo, win_hi), bd in SCENARIOS:
            key = f'rev_empty_count_{label}{{a}}'
            r.execute_command('DEL', key)
            r.execute_command('TS.CREATE', key)
            for t, v in samples:
                r.execute_command('TS.ADD', key, t, v)

            for agg in AGGS:
                fwd_all = r.execute_command(
                    'TS.RANGE', key, win_lo, win_hi,
                    'AGGREGATION', agg, bd, 'EMPTY')
                total = len(fwd_all)

                # Sanity guard: the scenarios are designed so EMPTY emits at
                # least one bucket. If this ever fires, the scenario is wrong,
                # not the SUT.
                assert total >= 1, (
                    f'[{label}/{agg}] TS.RANGE EMPTY returned no buckets; '
                    f'scenario invalid')

                counts_to_try = sorted({1, max(1, total // 2), total, total + 5})
                for n in counts_to_try:
                    rev = r.execute_command(
                        'TS.REVRANGE', key, win_lo, win_hi,
                        'AGGREGATION', agg, bd, 'EMPTY',
                        'COUNT', n)
                    expected = list(reversed(fwd_all[-n:]))
                    assert rev == expected, (
                        f'[{label}/{agg}] REVRANGE EMPTY COUNT {n}: '
                        f'expected {expected!r}, got {rev!r}; '
                        f'(fwd_all={fwd_all!r})')


def test_empty_gap_fill_twa_last_multi_agg():
    """
    TWA+LAST multi-agg gap-filling.

    Bug: with TWA and LAST in the same query, LAST contexts were not seeded for
    gap-filled buckets in fillEmptyBuckets(), so LAST produced wrong values for
    empty buckets.
    Fix: always seed LAST contexts before filling (seed_locf_for_empty_gap moved
    outside the if/else in fillEmptyBuckets).

    Verifies an interior gap with TWA+LAST: LAST carries LOCF while TWA
    interpolates, asserting exact deterministic values in both directions
    (reverse exercises the always-overwrite seeding branch).
    """
    with Env(decodeResponses=True).getClusterConnectionIfNeeded() as r:
        r.execute_command('DEL', 'gap_twa_last_test')
        assert r.execute_command('TS.CREATE', 'gap_twa_last_test')

        # Samples t=5 (=100), t=45 (=300); bucket size 10 over [0,50].
        # [0,10) and [40,50) hold real samples; [10,20) [20,30) [30,40) are an interior gap.
        r.execute_command('TS.ADD', 'gap_twa_last_test', 5, 100)
        r.execute_command('TS.ADD', 'gap_twa_last_test', 45, 300)

        # Per bucket: [ts, twa, last]. LAST is LOCF (100 until the t=45 bucket); TWA interpolates.
        expected = [
            [0,  '112.5', '100'],
            [10, '150',   '100'],
            [20, '200',   '100'],
            [30, '250',   '100'],
            [40, '287.5', '300'],
        ]
        fwd = decode_if_needed(r.execute_command(
            'TS.RANGE', 'gap_twa_last_test', 0, 50, 'AGGREGATION', 'twa,last', 10, 'EMPTY'))
        assert fwd == expected, f'fwd: {fwd!r} != {expected!r}'

        rev = decode_if_needed(r.execute_command(
            'TS.REVRANGE', 'gap_twa_last_test', 0, 50, 'AGGREGATION', 'twa,last', 10, 'EMPTY'))
        assert rev == list(reversed(expected)), f'rev: {rev!r} != {list(reversed(expected))!r}'


def test_empty_gap_fill_interior_no_edge_scan():
    """
    Guards the check_edge_gaps optimization in fillEmptyBuckets().

    Interior gap fills (the main aggregation loop and the single-agg MAX forward
    fast path) now pass check_edge_gaps=False and skip the two edge-detection
    series scans, because a sample provably exists on both sides. The
    prefix/suffix/whole-range callers still pass True and must still DROP edge
    gaps.

    This test packs several interior gaps AND real edge gaps into one EMPTY query
    so both behaviors run together, for every aggregator and in both directions.
    If the optimization ever wrongly skipped or mis-classified a gap, the emitted
    bucket set or values would change here.

    Layout (bucket size 10, ALIGN 0), samples at t=25, 55, 85:
        [0,10) [10,20)     -> before first sample  -> dropped (prefix edge)
        [20,30)            -> sample 25 (=100)
        [30,40) [40,50)    -> interior gap         -> filled (check_edge_gaps=False)
        [50,60)            -> sample 55 (=200)
        [60,70) [70,80)    -> interior gap         -> filled (check_edge_gaps=False)
        [80,90)            -> sample 85 (=300)
        [90,100) [100,110) -> after last sample    -> dropped (suffix edge)
    => exactly 7 emitted buckets: ts 20, 30, 40, 50, 60, 70, 80
    """
    with Env(decodeResponses=True).getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 'g')
        assert r.execute_command('TS.MADD', 'g', 25, 100, 'g', 55, 200, 'g', 85, 300) == [25, 55, 85]

        def rng(agg, start=0, end=109):
            return decode_if_needed(r.execute_command(
                'TS.range', 'g', start, end, 'ALIGN', '0', 'AGGREGATION', agg, 10, 'EMPTY'))

        def revrng(agg, start=0, end=109):
            return decode_if_needed(r.execute_command(
                'TS.revrange', 'g', start, end, 'ALIGN', '0', 'AGGREGATION', agg, 10, 'EMPTY'))

        ts = [20, 30, 40, 50, 60, 70, 80]

        def expected(by_ts):
            return [[t, by_ts[t]] for t in ts]

        # LAST: LOCF carried across every interior gap (the seeded path).
        last_vals = {20: '100', 30: '100', 40: '100', 50: '200', 60: '200', 70: '200', 80: '300'}
        # SUM / COUNT: documented empty-bucket value 0 in gaps.
        sum_vals = {20: '100', 30: '0', 40: '0', 50: '200', 60: '0', 70: '0', 80: '300'}
        count_vals = {20: '1', 30: '0', 40: '0', 50: '1', 60: '0', 70: '0', 80: '1'}
        # MAX/MIN/FIRST/AVG: NaN in gaps, sample value in real buckets.
        nan_vals = {20: '100', 30: 'NaN', 40: 'NaN', 50: '200', 60: 'NaN', 70: 'NaN', 80: '300'}

        # 'max' specifically exercises the single-agg forward FAST PATH (the second
        # interior caller that now skips the edge scan).
        cases = (
            ('last', last_vals),
            ('sum', sum_vals),
            ('count', count_vals),
            ('max', nan_vals),
            ('min', nan_vals),
            ('first', nan_vals),
            ('avg', nan_vals),
        )
        for agg, vals in cases:
            exp = expected(vals)
            assert rng(agg) == exp, f'{agg} fwd: {rng(agg)!r} != {exp!r}'
            assert revrng(agg) == list(reversed(exp)), \
                f'{agg} rev: {revrng(agg)!r} != {list(reversed(exp))!r}'

        # Ranges entirely before the first / after the last sample have no neighbor on one
        # side and must stay empty for every aggregator. This is the edge path that still
        # scans & drops - proving the optimization didn't disable edge handling.
        for agg in ('last', 'sum', 'count', 'max', 'min', 'first', 'avg', 'twa'):
            assert rng(agg, 0, 19) == [], f'{agg}: prefix-only range should be empty'
            assert rng(agg, 90, 109) == [], f'{agg}: suffix-only range should be empty'
            assert revrng(agg, 0, 19) == [], f'{agg}: prefix-only revrange should be empty'
            assert revrng(agg, 90, 109) == [], f'{agg}: suffix-only revrange should be empty'

        # TWA interior multi-gap fill also goes through the check_edge_gaps=False after-finalize
        # path. Use a constant value so the interpolated gap buckets are exactly 100 (deterministic,
        # no float fragility): samples 5,35,65 (=100) over [0,69], bucket 10 -> 7 buckets all 100,
        # interior gaps filled (not dropped) in both directions.
        assert r.execute_command('TS.CREATE', 'c')
        assert r.execute_command('TS.MADD', 'c', 5, 100, 'c', 35, 100, 'c', 65, 100) == [5, 35, 65]
        twa_const = [[t, '100'] for t in (0, 10, 20, 30, 40, 50, 60)]
        twa_fwd = decode_if_needed(r.execute_command(
            'TS.range', 'c', 0, 69, 'ALIGN', '0', 'AGGREGATION', 'twa', 10, 'EMPTY'))
        twa_rev = decode_if_needed(r.execute_command(
            'TS.revrange', 'c', 0, 69, 'ALIGN', '0', 'AGGREGATION', 'twa', 10, 'EMPTY'))
        assert twa_fwd == twa_const, f'twa interior fwd: {twa_fwd!r} != {twa_const!r}'
        assert twa_rev == list(reversed(twa_const)), f'twa interior rev: {twa_rev!r}'
