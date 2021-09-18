from utils import Env, set_hertz
from test_helper_classes import _insert_data
from includes import *


def test_mrevrange(env):
    start_ts = 1511885909
    samples_count = 50
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'tester1', 'LABELS', 'name', 'bo', 'class', 'middle', 'generation', 'x', conn=r).noError()
        env.expect('TS.CREATE', 'tester2', 'LABELS', 'name', 'rudy', 'class', 'junior', 'generation', 'x', conn=r).noError()
        env.expect('TS.CREATE', 'tester3', 'LABELS', 'name', 'fabi', 'class', 'top', 'generation', 'x', conn=r).noError()
        _insert_data(env, r, 'tester1', start_ts, samples_count, 5)
        _insert_data(env, r, 'tester2', start_ts, samples_count, 15)
        _insert_data(env, r, 'tester3', start_ts, samples_count, 25)

        expected_result = [[start_ts + i, str(5)] for i in range(samples_count)]
        expected_result.reverse()
        env.expect('TS.mrevrange', start_ts, start_ts + samples_count, 'FILTER', 'name=bo', conn=r).equal([['tester1', [], expected_result]])

        expected_result = [['tester1', [],
                            [[1511885958, '5'], [1511885957, '5'], [1511885956, '5'], [1511885955, '5'],
                             [1511885954, '5']]],
                           ['tester2', [],
                            [[1511885958, '15'], [1511885957, '15'], [1511885956, '15'], [1511885955, '15'],
                             [1511885954, '15']]],
                           ['tester3', [],
                            [[1511885958, '25'], [1511885957, '25'], [1511885956, '25'], [1511885955, '25'],
                             [1511885954, '25']]]]
        actual_result = r.execute_command('TS.mrevrange', start_ts, start_ts + samples_count, 'COUNT', '5', 'FILTER', 'generation=x')
        actual_result.sort(key=lambda x: x[0])
        env.assertEqual(actual_result, expected_result)

        agg_result = r.execute_command('TS.mrange', 0, '+', 'AGGREGATION', 'sum', 50, 'FILTER', 'name=bo')[0][2]
        rev_agg_result = r.execute_command('TS.mrevrange', 0, '+', 'AGGREGATION', 'sum', 50, 'FILTER', 'name=bo')[0][2]
        rev_agg_result.reverse()
        env.assertEqual(rev_agg_result, agg_result)
        last_results = list(agg_result)
        last_results.reverse()
        last_results = last_results[0:3]
        res = r.execute_command('TS.mrevrange', 0, '+', 'AGGREGATION', 'sum', 50, 'COUNT', 3, 'FILTER', 'name=bo')[0][2]
        env.assertEqual(res, last_results)
