from collections import defaultdict

from utils import Env
import pytest
import redis
import create_test_rdb_file
from includes import *

def test_groupby_reduce_errors(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.ADD', 's1', 1, 100, 'LABELS', 'metric_family', 'cpu', 'metric_name', 'user', conn=r).noError()
        env.expect('TS.ADD', 's2', 2, 55, 'LABELS', 'metric_family', 'cpu', 'metric_name', 'user', conn=r).noError()
        env.expect('TS.ADD', 's3', 2, 40, 'LABELS', 'metric_family', 'cpu', 'metric_name', 'system', conn=r).noError()
        env.expect('TS.ADD', 's1', 2, 95, conn=r).noError()

        # test wrong arity
        env.expect('TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', conn=r).error()
        env.expect('TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', conn=r).error()
        env.expect('TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'abc', 'abc', conn=r).error()
        env.expect('TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'REDUCE', 'bla', conn=r).error()
        env.expect('TS.MRANGE', 0, 100, 'WITHLABELS', 'GROUPBY', 'metric_name', 'REDUCE', 'max', 'FILTER', 'metric=cpu', conn=r).error()

def test_groupby_reduce(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.ADD', 's1', 1, 100, 'LABELS', 'metric_family', 'cpu', 'metric_name', 'user', conn=r).noError()
        env.expect('TS.ADD', 's2', 2, 55, 'LABELS', 'metric_family', 'cpu', 'metric_name', 'user', conn=r).noError()
        env.expect('TS.ADD', 's3', 2, 40, 'LABELS', 'metric_family', 'cpu', 'metric_name', 'system', conn=r).noError()
        env.expect('TS.ADD', 's1', 2, 95, conn=r).noError()

        actual_result = r.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'REDUCE', 'max')
        serie1 = actual_result[0]
        serie1_name = serie1[0]
        serie1_labels = serie1[1]
        serie1_values = serie1[2]
        env.assertEqual(serie1_values, [[2, '40']])
        env.assertEqual(serie1_name, 'metric_name=system')
        env.assertEqual(serie1_labels[0][0], 'metric_name')
        env.assertEqual(serie1_labels[0][1], 'system')
        serie2 = actual_result[1]
        serie2_name = serie2[0]
        serie2_labels = serie2[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_name, 'metric_name=user')
        env.assertEqual(serie2_labels[0][0], 'metric_name')
        env.assertEqual(serie2_labels[0][1], 'user')
        env.assertEqual(serie2_labels[1][0], '__reducer__')
        env.assertEqual(serie2_labels[1][1], 'max')
        env.assertEqual(serie2_labels[2][0], '__source__')
        env.assertEqual(sorted(serie2_labels[2][1].split(",")), ['s1', 's2'])
        env.assertEqual(serie2_values, [[1, '100'], [2, '95']])

        actual_result = r.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'REDUCE', 'sum')
        serie2 = actual_result[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_values, [[1, '100'], [2, '150']])

        actual_result = r.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'REDUCE', 'min')
        serie2 = actual_result[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_values, [[1, '100'], [2, '55']])

        actual_result = r.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'COUNT', 1, 'FILTER', 'metric_family=cpu', 'GROUPBY', 'metric_name', 'REDUCE', 'min')
        serie2 = actual_result[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_values, [[1, '100']])

def test_groupby_reduce_empty(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.ADD', 's1', 1, 100, 'LABELS', 'metric_family', 'cpu', 'metric_name', 'user', conn=r).noError()
        env.expect('TS.ADD', 's2', 2, 55, 'LABELS', 'metric_family', 'cpu', 'metric_name', 'user', conn=r).noError()
        env.expect('TS.ADD', 's3', 2, 40, 'LABELS', 'metric_family', 'cpu', 'metric_name', 'system', conn=r).noError()
        env.expect('TS.ADD', 's1', 2, 95, conn=r).noError()

        actual_result = r.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'metric_family=cpu', 'GROUPBY', 'labelX', 'REDUCE', 'max')
        env.assertEqual(actual_result, [])

def test_groupby_reduce_multiple_groups(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.ADD', 's1', 1, 100, 'LABELS', 'HOST', 'A', 'REGION', 'EU', 'PROVIDER', 'AWS', conn=r).noError()
        env.expect('TS.ADD', 's2', 1, 55, 'LABELS', 'HOST', 'B', 'REGION', 'EU', 'PROVIDER', 'AWS', conn=r).noError()
        env.expect('TS.ADD', 's2', 2, 90, 'LABELS', 'HOST', 'B', 'REGION', 'EU', 'PROVIDER', 'AWS', conn=r).noError()
        env.expect('TS.ADD', 's3', 2, 40, 'LABELS', 'HOST', 'C', 'REGION', 'US', 'PROVIDER', 'AWS', conn=r).noError()

        actual_result = r.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'PROVIDER=AWS', 'GROUPBY', 'REGION', 'REDUCE', 'max')
        serie1 = actual_result[0]
        serie1_name = serie1[0]
        serie1_labels = serie1[1]
        serie1_values = serie1[2]
        env.assertEqual(serie1_values, [[1, '100'],[2, '90']])
        env.assertEqual(serie1_name, 'REGION=EU')
        env.assertEqual(serie1_labels[0][0], 'REGION')
        env.assertEqual(serie1_labels[0][1], 'EU')
        env.assertEqual(serie1_labels[1][0], '__reducer__')
        env.assertEqual(serie1_labels[1][1], 'max')
        env.assertEqual(serie1_labels[2][0], '__source__')
        env.assertEqual(sorted(serie1_labels[2][1].split(",")), ['s1', 's2'])
        serie2 = actual_result[1]
        serie2_name = serie2[0]
        serie2_labels = serie2[1]
        serie2_values = serie2[2]
        env.assertEqual(serie2_values, [[2, '40']])
        env.assertEqual(serie2_name, 'REGION=US')
        env.assertEqual(serie2_labels[0][0], 'REGION')
        env.assertEqual(serie2_labels[0][1], 'US')
        env.assertEqual(serie2_labels[1][0], '__reducer__')
        env.assertEqual(serie2_labels[1][1], 'max')
        env.assertEqual(serie2_labels[2][0], '__source__')
        env.assertEqual(serie2_labels[2][1], 's3')

def truncate_month(date):
    return "-".join(date.split("-")[0:2])

def test_filterby(env):
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

    with env.getClusterConnectionIfNeeded() as r:
        create_test_rdb_file.load_into_redis(r)

        def assert_results(results, expected_results):
            for row in results:
                country = row[1][0][1]
                points = dict([(point[0], int(point[1])) for point in row[2]])
                for k in points:
                    env.assertEqual(points[k], expected_results[country][k], message="timestamp {} not equal".format(k))
                env.assertEqual(points, expected_results[country], message="country {} not eq".format(country))

        results = r.execute_command("TS.MRANGE", "-", "+",
                          "withlabels", "FILTER_BY_VALUE", 30, 100,
                          "AGGREGATION", "count", 3600000,
                          "filter", "metric=temperature", "groupby", "country", "reduce", "sum")
        assert_results(results, high_temps)

        results = r.execute_command("TS.MRANGE", "-", "+",
                                    "withlabels", "FILTER_BY_TS", 1335830400000, 1338508800000,
                                    "AGGREGATION", "count", 3600000,
                                    "filter", "metric=temperature", "groupby", "country", "reduce", "sum")
        assert_results(results, specific_days)
