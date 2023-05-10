from collections import defaultdict

# from utils import Env
import pytest
import redis
import create_test_rdb_file
from includes import *
import statistics

def test_groupby_reduce_errors():
    env = Env()
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        assert r.execute_command('TS.ADD', 's1', 1, 100, 'LABELS', 'metric_family', 'cpu', 'metric_name', 'user')
        assert r.execute_command('TS.ADD', 's2', 2, 55, 'LABELS', 'metric_family', 'cpu', 'metric_name', 'user')
        assert r.execute_command('TS.ADD', 's3', 2, 40, 'LABELS', 'metric_family', 'cpu', 'metric_name', 'system')
        assert r.execute_command('TS.ADD', 's1', 2, 95)

        # test wrong arity
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r1.execute_command('TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY')

        with pytest.raises(redis.ResponseError) as excinfo:
            assert r1.execute_command('TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name')

        with pytest.raises(redis.ResponseError) as excinfo:
            assert r1.execute_command('TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'abc', 'abc')

        with pytest.raises(redis.ResponseError) as excinfo:
            assert r1.execute_command('TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'REDUCE', 'bla')

    with pytest.raises(redis.ResponseError) as excinfo:
        assert r1.execute_command('TS.MRANGE', 0, 100, 'WITHLABELS', 'GROUPBY', 'metric_name', 'REDUCE', 'max', 'FILTER', 'metric=cpu')

def test_groupby_reduce():
    env = Env()
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        assert r.execute_command('TS.ADD', 's1', 1, 100, 'LABELS', 'metric_family', 'cpu', 'metric_name', 'user')
        assert r.execute_command('TS.ADD', 's2', 2, 55, 'LABELS', 'metric_family', 'cpu', 'metric_name', 'user')
        assert r.execute_command('TS.ADD', 's3', 2, 40, 'LABELS', 'metric_family', 'cpu', 'metric_name', 'system')
        assert r.execute_command('TS.ADD', 's1', 2, 95)

        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'REDUCE', 'max')
        serie1 = actual_result[0]
        serie1_name = serie1[0]
        serie1_labels = serie1[1]
        serie1_values = serie1[2]
        env.assertEqual(serie1_values, [[2, b'40']])
        env.assertEqual(serie1_name, b'metric_name=system')
        env.assertEqual(serie1_labels[0][0], b'metric_name')
        env.assertEqual(serie1_labels[0][1], b'system')
        serie2 = actual_result[1]
        serie2_name = serie2[0]
        serie2_labels = serie2[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_name, b'metric_name=user')
        env.assertEqual(serie2_labels[0][0], b'metric_name')
        env.assertEqual(serie2_labels[0][1], b'user')
        env.assertEqual(serie2_labels[1][0], b'__reducer__')
        env.assertEqual(serie2_labels[1][1], b'max')
        env.assertEqual(serie2_labels[2][0], b'__source__')
        env.assertEqual(sorted(serie2_labels[2][1].decode("ascii").split(",")), ['s1', 's2'])
        env.assertEqual(serie2_values, [[1, b'100'], [2, b'95']])

        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'REDUCE', 'sum')
        serie2 = actual_result[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_values, [[1, b'100'], [2, b'150']])

        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'REDUCE', 'min')
        serie2 = actual_result[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_values, [[1, b'100'], [2, b'55']])

        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'COUNT', 1, 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'REDUCE', 'min')
        serie2 = actual_result[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_values, [[1, b'100']])

        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'COUNT', 1, 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'REDUCE', 'min')
        serie2 = actual_result[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_values, [[1, b'100']])

        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'REDUCE', 'avg')
        serie2 = actual_result[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_values, [[1, b'100'], [2, b'75']])

        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'REDUCE', 'count')
        serie2 = actual_result[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_values, [[1, b'1'], [2, b'2']])

        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'REDUCE', 'range')
        serie2 = actual_result[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_values, [[1, b'0'], [2, b'40']])

        expected_res = [[1, b'0'], [2, str(statistics.pvariance([55, 95])).encode('ascii')]]
        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'REDUCE', 'var.p')
        serie2 = actual_result[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_values, expected_res)

        expected_res = [[1, b'0'], [2, str(statistics.variance([55, 95])).encode('ascii')]]
        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'REDUCE', 'var.s')
        serie2 = actual_result[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_values, expected_res)

        expected_res = [[1, b'0'], [2, str(int(statistics.pstdev([55, 95]))).encode('ascii')]]
        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'REDUCE', 'std.p')
        serie2 = actual_result[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_values, expected_res)

        expected_res = [[1, b'0'], [2, str(statistics.stdev([55, 95])).encode('ascii')]]
        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'REDUCE', 'std.s')
        serie2 = actual_result[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_values, expected_res)

def test_groupby_reduce_empty():
    env = Env()
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        assert r.execute_command('TS.ADD', 's1', 1, 100, 'LABELS', 'metric_family', 'cpu', 'metric_name', 'user')
        assert r.execute_command('TS.ADD', 's2', 2, 55, 'LABELS', 'metric_family', 'cpu', 'metric_name', 'user')
        assert r.execute_command('TS.ADD', 's3', 2, 40, 'LABELS', 'metric_family', 'cpu', 'metric_name', 'system')
        assert r.execute_command('TS.ADD', 's1', 2, 95)

        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'labelX', 'REDUCE', 'max')
        env.assertEqual(actual_result, [])

def test_groupby_reduce_multiple_groups():
    env = Env()
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        assert r.execute_command('TS.ADD', 's1', 1, 100, 'LABELS', 'HOST', 'A', 'REGION', 'EU', 'PROVIDER', 'AWS')
        assert r.execute_command('TS.ADD', 's2', 1, 55, 'LABELS', 'HOST', 'B', 'REGION', 'EU', 'PROVIDER', 'AWS')
        assert r.execute_command('TS.ADD', 's2', 2, 90, 'LABELS', 'HOST', 'B', 'REGION', 'EU', 'PROVIDER', 'AWS')
        assert r.execute_command('TS.ADD', 's3', 2, 40, 'LABELS', 'HOST', 'C', 'REGION', 'US', 'PROVIDER', 'AWS')

        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'PROVIDER=AWS', 'GROUPBY', 'REGION', 'REDUCE', 'max')
        serie1 = actual_result[0]
        serie1_name = serie1[0]
        serie1_labels = serie1[1]
        serie1_values = serie1[2]
        env.assertEqual(serie1_values, [[1, b'100'],[2, b'90']])
        env.assertEqual(serie1_name, b'REGION=EU')
        env.assertEqual(serie1_labels[0][0], b'REGION')
        env.assertEqual(serie1_labels[0][1], b'EU')
        env.assertEqual(serie1_labels[1][0], b'__reducer__')
        env.assertEqual(serie1_labels[1][1], b'max')
        env.assertEqual(serie1_labels[2][0], b'__source__')
        env.assertEqual(sorted(serie1_labels[2][1].decode("ascii").split(",")), ['s1', 's2'])
        serie2 = actual_result[1]
        serie2_name = serie2[0]
        serie2_labels = serie2[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_values, [[2, b'40']])
        env.assertEqual(serie2_name, b'REGION=US')
        env.assertEqual(serie2_labels[0][0], b'REGION')
        env.assertEqual(serie2_labels[0][1], b'US')
        env.assertEqual(serie2_labels[1][0], b'__reducer__')
        env.assertEqual(serie2_labels[1][1], b'max')
        env.assertEqual(serie2_labels[2][0], b'__source__')
        env.assertEqual(serie2_labels[2][1], b's3')

def truncate_month(date):
    return "-".join(date.split("-")[0:2])

def test_filterby():
    env = Env()
    high_temps = defaultdict(lambda : defaultdict(lambda: 0))
    specific_days = defaultdict(lambda : defaultdict(lambda: 0))
    days = [1335830400000, 1338508800000]
    for row in create_test_rdb_file.read_from_disk():
        timestamp = create_test_rdb_file.parse_timestamp(row[0])
        country = row[create_test_rdb_file.Country].replace('(', '[').replace(')', ']')
        if timestamp in days:
            specific_days[country][timestamp] += 1

        if row[1] and float(row[1]) >= 30:
            if timestamp > 0:
                high_temps[country][timestamp] += 1

    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        create_test_rdb_file.load_into_redis(r)

        def assert_results(results, expected_results):
            for row in results:
                country = row[1][0][1].decode()
                points = dict([(point[0], int(point[1])) for point in row[2]])
                for k in points:
                    env.assertEqual(points[k], expected_results[country][k], message="timestamp {} not equal".format(k))
                env.assertEqual(points, expected_results[country], message="country {} not eq".format(country))

        results = r1.execute_command("TS.MRANGE", "-", "+",
                          "withlabels", "FILTER_BY_VALUE", 30, 100,
                          "AGGREGATION", "count", 3600000,
                          "filter", "metric=temperature", "groupby", "country", "reduce", "sum")
        assert_results(results, high_temps)

        results = r1.execute_command("TS.MRANGE", "-", "+",
                                    "withlabels", "FILTER_BY_TS", 1335830400000, 1338508800000,
                                    "AGGREGATION", "count", 3600000,
                                    "filter", "metric=temperature", "groupby", "country", "reduce", "sum")
        assert_results(results, specific_days)

def test_empty():
    agg_size = 10
    env = Env(decodeResponses=True)
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        assert r.execute_command('TS.CREATE', 't1', 'LABELS', 'metric_family', 'cpu', 'metric_name', 'user')
        assert r.execute_command('TS.add', 't1', 15, 1)
        assert r.execute_command('TS.add', 't1', 17, 4)
        assert r.execute_command('TS.add', 't1', 51, 3)
        assert r.execute_command('TS.add', 't1', 73, 5)
        assert r.execute_command('TS.add', 't1', 75, 3)
        assert r.execute_command('TS.CREATE', 't2', 'LABELS', 'metric_family', 'cpu', 'metric_name', 'user')
        assert r.execute_command('TS.add', 't2', 3, 1)
        assert r.execute_command('TS.add', 't2', 4, 2)
        assert r.execute_command('TS.add', 't2', 51, 3)
        assert r.execute_command('TS.add', 't2', 60, 9)
        #expected_agg_t1 = [[10, '4'], [20, 'NaN'], [30, 'NaN'], [40, 'NaN'], [50, '3'], [60, 'NaN'], [70, '5']]
        #expected_agg_t2 = [[0, '2'], [10, 'NaN'], [20, 'NaN'] [30, 'NaN'], [40, 'NaN'], [50, '3'], [60, '9']]
        exp_samples = [[0, '2'], [10, '4'], [20, 'NaN'], [30, 'NaN'], [40, 'NaN'], [50, '6'], [60, '9'], [70, '5']]
        expected_data = [['metric_name=user', [], exp_samples]]
        assert expected_data == \
        decode_if_needed(r1.execute_command("TS.MRANGE", "0", "100", "AGGREGATION", "max", agg_size, 'EMPTY', "filter", "metric_family=cpu", "groupby", "metric_name", "reduce", "sum"))
        exp_samples.reverse()
        expected_data = [['metric_name=user', [], exp_samples]]
        assert expected_data == \
        decode_if_needed(r1.execute_command('TS.MREVRANGE', "0", "100", "AGGREGATION", "max", agg_size, 'EMPTY', "filter", "metric_family=cpu", "groupby", "metric_name", "reduce", "sum"))

        # test LAST + EMPTY
        #expected_agg_t1 = [[10, '4'], [20, '4'], [30, '4'], [40, '4'], [50, '3'], [60, '3'], [70, '3']]
        #expected_agg_t2 = [[0, '2'], [10, '2'], [20, '2'], [30, '2'], [40, '2'], [50, '3'], [60, '9']]
        exp_samples = [[0, '2'], [10, '6'], [20, '6'], [30, '6'], [40, '6'], [50, '6'], [60, '12'], [70, '3']]
        expected_data = [['metric_name=user', [], exp_samples]]
        assert expected_data == \
        decode_if_needed(r1.execute_command("TS.MRANGE", "0", "100", "AGGREGATION", "LAST", agg_size, 'EMPTY', "filter", "metric_family=cpu", "groupby", "metric_name", "reduce", "sum"))
        exp_samples.reverse()
        expected_data = [['metric_name=user', [], exp_samples]]
        assert expected_data == \
        decode_if_needed(r1.execute_command('TS.MREVRANGE', "0", "100", "AGGREGATION", "LAST", agg_size, 'EMPTY', "filter", "metric_family=cpu", "groupby", "metric_name", "reduce", "sum"))



def test_bucket_timestamp():
    agg_size = 10
    env = Env(decodeResponses=True)
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        assert r.execute_command('TS.CREATE', 't1', 'LABELS', 'metric_family', 'cpu', 'metric_name', 'user')
        assert r.execute_command('TS.add', 't1', 15, 1)
        assert r.execute_command('TS.add', 't1', 17, 4)
        assert r.execute_command('TS.add', 't1', 51, 3)
        assert r.execute_command('TS.add', 't1', 73, 5)
        assert r.execute_command('TS.add', 't1', 75, 3)
        assert r.execute_command('TS.CREATE', 't2', 'LABELS', 'metric_family', 'cpu', 'metric_name', 'user')
        assert r.execute_command('TS.add', 't2', 3, 1)
        assert r.execute_command('TS.add', 't2', 4, 2)
        assert r.execute_command('TS.add', 't2', 51, 3)
        assert r.execute_command('TS.add', 't2', 60, 9)

        #expected_agg_t1 = [[10, '4'], [50, '3'], [70, '5']]
        #expected_agg_t2 = [[0, '2'], [50, '3'], [60, '9']]
        exp_samples = [[0, '2'], [10, '4'], [50, '6'], [60, '9'], [70, '5']]
        expected_data = [['metric_name=user', [], exp_samples]]
        assert expected_data == \
        decode_if_needed(r1.execute_command("TS.MRANGE", "0", "100", "AGGREGATION", "max", agg_size, 'BUCKETTIMESTAMP', '-', "filter", "metric_family=cpu", "groupby", "metric_name", "reduce", "sum"))
        exp_samples.reverse()
        expected_data = [['metric_name=user', [], exp_samples]]
        assert expected_data == \
        decode_if_needed(r1.execute_command('TS.MREVRANGE', "0", "100", "AGGREGATION", "max", agg_size, 'BUCKETTIMESTAMP', '-', "filter", "metric_family=cpu", "groupby", "metric_name", "reduce", "sum"))

        #expected_agg_t1 = [[15, '4'], [55, '3'], [75, '5']]
        #expected_agg_t2 = [[5, '2'], [55, '3'], [65, '9']]
        exp_samples = [[5, '2'], [15, '4'], [55, '6'], [65, '9'], [75, '5']]
        expected_data = [['metric_name=user', [], exp_samples]]
        assert expected_data == \
        decode_if_needed(r1.execute_command("TS.MRANGE", "0", "73", "AGGREGATION", "max", agg_size, 'BUCKETTIMESTAMP', '~', "filter", "metric_family=cpu", "groupby", "metric_name", "reduce", "sum"))
        exp_samples.reverse()
        expected_data = [['metric_name=user', [], exp_samples]]
        assert expected_data == \
        decode_if_needed(r1.execute_command('TS.MREVRANGE', "0", "73", "AGGREGATION", "max", agg_size, 'BUCKETTIMESTAMP', '~', "filter", "metric_family=cpu", "groupby", "metric_name", "reduce", "sum"))

        #expected_agg_t1 = [[20, '4'], [60, '3'], [80, '5']]
        #expected_agg_t2 = [[10, '2'], [60, '3'], [70, '9']]
        exp_samples = [[10, '2'], [20, '4'], [60, '6'], [70, '9'], [80, '5']]
        expected_data = [['metric_name=user', [], exp_samples]]
        assert expected_data == \
        decode_if_needed(r1.execute_command("TS.MRANGE", "0", "73", "AGGREGATION", "max", agg_size, 'BUCKETTIMESTAMP', '+', "filter", "metric_family=cpu", "groupby", "metric_name", "reduce", "sum"))
        exp_samples.reverse()
        expected_data = [['metric_name=user', [], exp_samples]]
        assert expected_data == \
        decode_if_needed(r1.execute_command('TS.MREVRANGE', "0", "73", "AGGREGATION", "max", agg_size, 'BUCKETTIMESTAMP', '+', "filter", "metric_family=cpu", "groupby", "metric_name", "reduce", "sum"))
