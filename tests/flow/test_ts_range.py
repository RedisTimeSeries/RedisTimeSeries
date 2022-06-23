import math

import pytest
import redis
from RLTest import Env
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
        wsum = (10*1.5 + 13*2.0 + 14*5 + 23*1.5)
        avgw = (1.5 + 2.0 + 5 + 1.5)/4
        res = (wsum/avgw)/4.0
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts1', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts1', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 2:
        assert r.execute_command('TS.CREATE', 'ts2')
        assert r.execute_command('TS.ADD', 'ts2', 8, 8)
        assert r.execute_command('TS.ADD', 'ts2', 9, 9)
        assert r.execute_command('TS.ADD', 'ts2', 13, 13)
        assert r.execute_command('TS.ADD', 'ts2', 14, 14)
        assert r.execute_command('TS.ADD', 'ts2', 23, 23)
        wsum = (9*1 + 13*2.5 + 14*5 + 23*1.5)
        avgw = (1 + 2.5 + 5 + 1.5)/4.0
        res = (wsum/avgw)/4.0
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts2', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts2', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 3:
        assert r.execute_command('TS.CREATE', 'ts3')
        assert r.execute_command('TS.ADD', 'ts3', 8, 8)
        assert r.execute_command('TS.ADD', 'ts3', 9, 9)
        assert r.execute_command('TS.ADD', 'ts3', 13, 13)
        assert r.execute_command('TS.ADD', 'ts3', 14, 14)
        assert r.execute_command('TS.ADD', 'ts3', 26, 26)
        wsum = (9*1 + 13*2.5 + 14*6.5)
        avgw = (1 + 2.5 + 6.5)/3.0
        res = (wsum/avgw)/3.0
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts3', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts3', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 4:
        assert r.execute_command('TS.CREATE', 'ts4')
        assert r.execute_command('TS.ADD', 'ts4', 8, 8)
        assert r.execute_command('TS.ADD', 'ts4', 9, 9)
        assert r.execute_command('TS.ADD', 'ts4', 13, 13)
        assert r.execute_command('TS.ADD', 'ts4', 14, 14)
        assert r.execute_command('TS.ADD', 'ts4', 27, 27)

        actual_result = r.execute_command('TS.RANGE', 'ts4', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts4', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 5:
        assert r.execute_command('TS.CREATE', 'ts5')
        assert r.execute_command('TS.ADD', 'ts5', 3, 3)
        assert r.execute_command('TS.ADD', 'ts5', 7, 7)
        assert r.execute_command('TS.ADD', 'ts5', 13, 13)
        assert r.execute_command('TS.ADD', 'ts5', 14, 14)
        assert r.execute_command('TS.ADD', 'ts5', 27, 27)
        wsum = (13*3.5 + 14*6.5)
        avgw = (3.5 + 6.5)/2.0
        res = (wsum/avgw)/2.0
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts5', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts5', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 6:
        assert r.execute_command('TS.CREATE', 'ts6')
        assert r.execute_command('TS.ADD', 'ts6', 3, 3)
        assert r.execute_command('TS.ADD', 'ts6', 6, 6)
        assert r.execute_command('TS.ADD', 'ts6', 13, 13)
        assert r.execute_command('TS.ADD', 'ts6', 14, 14)
        assert r.execute_command('TS.ADD', 'ts6', 27, 27)

        actual_result = r.execute_command('TS.RANGE', 'ts6', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts6', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 7:
        assert r.execute_command('TS.CREATE', 'ts7')
        assert r.execute_command('TS.ADD', 'ts7', 3, 3)
        assert r.execute_command('TS.ADD', 'ts7', 9, 9)
        assert r.execute_command('TS.ADD', 'ts7', 13, 13)
        assert r.execute_command('TS.ADD', 'ts7', 22, 22)
        wsum = (9*1.0 + 13*6.5 + 22*2.5)
        avgw = (1 + 6.5 + 2.5)/3.0
        res = (wsum/avgw)/3.0
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts7', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts7', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 8:
        assert r.execute_command('TS.CREATE', 'ts8')
        assert r.execute_command('TS.ADD', 'ts8', 3, 3)
        assert r.execute_command('TS.ADD', 'ts8', 13, 13)
        assert r.execute_command('TS.ADD', 'ts8', 28, 28)
        wsum = 13*10
        avgw = 10
        res = (wsum//avgw)//1
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts8', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts8', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 9:
        assert r.execute_command('TS.CREATE', 'ts9')
        assert r.execute_command('TS.ADD', 'ts9', 13, 13)
        assert r.execute_command('TS.ADD', 'ts9', 28, 28)
        wsum = 13*10
        avgw = 10
        res = (wsum//avgw)//1
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts9', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts9', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 10:
        assert r.execute_command('TS.CREATE', 'ts10')
        assert r.execute_command('TS.ADD', 'ts10', 13, 13)
        assert r.execute_command('TS.ADD', 'ts10', 21, 21)
        wsum = (13*7.0 + 21*3.0)
        avgw = (7.0 + 3.0)/2
        res = (wsum/avgw)/2
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts10', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts10', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 11:
        assert r.execute_command('TS.CREATE', 'ts11')
        assert r.execute_command('TS.ADD', 'ts11', 17, 17)
        assert r.execute_command('TS.ADD', 'ts11', 21, 21)
        wsum = (17*4.0 + 21*1.0)
        avgw = (4.0 + 1.0)/2
        res = (wsum/avgw)/2
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts11', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts11', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 12:
        assert r.execute_command('TS.CREATE', 'ts12')
        assert r.execute_command('TS.ADD', 'ts12', 3, 3)
        assert r.execute_command('TS.ADD', 'ts12', 17, 17)
        wsum = (17*10)
        avgw = (10)//1
        res = (wsum//avgw)//1
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts12', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts12', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 13:
        assert r.execute_command('TS.CREATE', 'ts13')
        assert r.execute_command('TS.ADD', 'ts13', 3, 3)
        assert r.execute_command('TS.ADD', 'ts13', 17, 17)
        wsum = (17*10)
        avgw = (10)//1
        res = (wsum//avgw)//1
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts13', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts13', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 14:
        assert r.execute_command('TS.CREATE', 'ts14')
        assert r.execute_command('TS.ADD', 'ts14', 5, 5)
        assert r.execute_command('TS.ADD', 'ts14', 12, 12)
        res = 12
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts14', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts14', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 15:
        assert r.execute_command('TS.CREATE', 'ts15')
        assert r.execute_command('TS.ADD', 'ts15', 7, 7)
        assert r.execute_command('TS.ADD', 'ts15', 15, 15)
        wsum = (7*1 + 15*8)
        avgw = (1 + 8)/2
        res = (wsum/avgw)/2
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts15', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts15', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 16:
        assert r.execute_command('TS.CREATE', 'ts16')
        assert r.execute_command('TS.ADD', 'ts16', 15, 15)
        res = 15
        expected_result = [10, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts16', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts16', 10, 19, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

        #case 17:
        assert r.execute_command('TS.CREATE', 'ts17')
        assert r.execute_command('TS.ADD', 'ts17', 0, 5) == 0
        assert r.execute_command('TS.ADD', 'ts17', 9, 12)
        wsum = (5*4.5 + 12*5.5)
        avgw = (4.5 + 5.5)/2
        res = (wsum/avgw)/2
        expected_result = [0, str(res).encode('ascii')]

        actual_result = r.execute_command('TS.RANGE', 'ts17', 0, 9, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result
        actual_result = r.execute_command('TS.REVRANGE', 'ts17', 0, 9, 'AGGREGATION', 'twa', 10)
        assert actual_result[0] == expected_result

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

def test_large_compressed_range():
    seed = datetime.now()
    random.seed(seed)
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
    seed = datetime.now()
    random.seed(seed)
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
    seed = datetime.now()
    random.seed(seed)
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
        expected_data = [[10, '4'], [20, 'NaN'], [30, 'NaN'], [40, 'NaN'], [50, '3'], [60, 'NaN'], [70, '5']]
        assert expected_data == \
        decode_if_needed(r.execute_command('TS.range', 't1', '0', '100', 'ALIGN', '0', 'AGGREGATION', 'max', agg_size, 'EMPTY'))
        expected_data.reverse()
        assert expected_data == \
        decode_if_needed(r.execute_command('TS.revrange', 't1', '0', '100', 'ALIGN', '0', 'AGGREGATION', 'max', agg_size, 'EMPTY'))

        expected_data = [[10, '5'], [20, '0'], [30, '0'], [40, '0'], [50, '3'], [60, '0'], [70, '8']]
        assert expected_data == \
        decode_if_needed(r.execute_command('TS.range', 't1', '0', '100', 'ALIGN', '0', 'AGGREGATION', 'sum', agg_size, 'EMPTY'))
        expected_data.reverse()
        assert expected_data == \
        decode_if_needed(r.execute_command('TS.revrange', 't1', '0', '100', 'ALIGN', '0', 'AGGREGATION', 'sum', agg_size, 'EMPTY'))

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
        assert res == [[1, '1'], [2, '3'], [11, '7'], [13, '1']]
        res = r.execute_command('TS.range', key2, 0, 10)
        assert res == [[0, '4']]
        res = r.execute_command('TS.range', key2, 0, 10, "LATEST")
        assert res == [[0, '4'], [10, '8']]
        res = r.execute_command('TS.range', key2, 0, 9, "LATEST")
        assert res == [[0, '4']]

        # make sure LATEST haven't changed anything in the keys
        res = r.execute_command('TS.range', key2, 0, 10)
        assert res == [[0, '4']]
        res = r.execute_command('TS.range', key1, 0, 20)
        assert res == [[1, '1'], [2, '3'], [11, '7'], [13, '1']]

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
        assert res == [[1, '1'], [2, '3'], [11, '7'], [13, '1']]
        res = r.execute_command('TS.revrange', key2, 0, 10)
        assert res == [[0, '4']]
        res = r.execute_command('TS.revrange', key2, 0, 10, "LATEST")
        assert res == [[10, '8'], [0, '4']]
        res = r.execute_command('TS.revrange', key2, 0, 9, "LATEST")
        assert res == [[0, '4']]

        # make sure LATEST haven't changed anything in the keys
        res = r.execute_command('TS.revrange', key2, 0, 10)
        assert res == [[0, '4']]
        res = r.execute_command('TS.range', key1, 0, 20)
        assert res == [[1, '1'], [2, '3'], [11, '7'], [13, '1']]