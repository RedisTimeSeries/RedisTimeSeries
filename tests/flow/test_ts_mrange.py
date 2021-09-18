import pytest
import redis
import time
from collections import defaultdict
from utils import Env, set_hertz
from test_helper_classes import _insert_data
from test_ts_range import build_expected_aligned_data
from includes import *


def test_mrange_with_expire_cmd(env):
    set_hertz(env)

    with env.getClusterConnectionIfNeeded() as r:
        env.expect("TS.ADD", "X" ,"*" ,"1" ,"LABELS", "type", "DELAYED", conn=r).noError()
        env.expect("TS.ADD", "Y" ,"*" ,"1" ,"LABELS", "type", "DELAYED", conn=r).noError()
        env.expect("TS.ADD", "Z" ,"*" ,"1" ,"LABELS", "type", "DELAYED", conn=r).noError()
        current_ts = time.time()
        env.expect("EXPIRE","X", 5, conn=r).noError()
        env.expect("EXPIRE","Y", 6, conn=r).noError()
        env.expect("EXPIRE","Z", 7, conn=r).noError()
        while time.time() < (current_ts+10):
            reply = r.execute_command('TS.mrange', '-', '+', 'FILTER', 'type=DELAYED')
            assert(len(reply)>=0 and len(reply)<=3)
        env.expect("PING", conn=r).noError()

def test_mrange_expire_issue549(env):
    env.skipOnDebugger()
    set_hertz(env)
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('ts.add', 'k1', 1, 10, 'LABELS', 'l', '1', conn=r).equal(1)
        env.expect('ts.add', 'k2', 2, 20, 'LABELS', 'l', '1', conn=r).equal(2)
        env.expect('expire', 'k1', '1', conn=r).equal(1)
        for i in range(0, 5000):
            assert env.getConnection().execute_command('ts.mrange - + aggregation avg 10 withlabels filter l=1') is not None


def test_range_by_labels(env):
    start_ts = 1511885909
    samples_count = 50

    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'tester1', 'LABELS', 'name', 'bob', 'class', 'middle', 'generation', 'x', conn=r).noError()
        env.expect('TS.CREATE', 'tester2', 'LABELS', 'name', 'rudy', 'class', 'junior', 'generation', 'x', conn=r).noError()
        env.expect('TS.CREATE', 'tester3', 'LABELS', 'name', 'fabi', 'class', 'top', 'generation', 'x', conn=r).noError()
        _insert_data(r, 'tester1', start_ts, samples_count, 5)
        _insert_data(r, 'tester2', start_ts, samples_count, 15)
        _insert_data(r, 'tester3', start_ts, samples_count, 25)

        expected_result = [[start_ts + i, str(5)] for i in range(samples_count)]
        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'FILTER', 'name=bob', conn=r).equal([['tester1', [], expected_result]])

        expected_result.reverse()
        env.expect('TS.mrevrange', start_ts, start_ts + samples_count, 'FILTER', 'name=bob', conn=r).equal([['tester1', [], expected_result]])

        def build_expected(val, time_bucket):
            return [[int(i - i % time_bucket), str(val)] for i in
                    range(start_ts, start_ts + samples_count + 1, time_bucket)]

        expected_result = [['tester1', [], build_expected(5, 5)],
                           ['tester2', [], build_expected(15, 5)],
                           ['tester3', [], build_expected(25, 5)],
                           ]
        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'AGGREGATION', 'LAST', 5,
                   'FILTER', 'generation=x', conn=r).apply(sorted).equal(sorted(expected_result))
        env.expect('TS.mrange', start_ts, start_ts + samples_count,
                   'AGGREGATION', 'LAST', 5, 'FILTER', 'generation=x',
                   'class!=middle', conn=r).apply(lambda x: sorted(x, key=lambda x: x[0])).equal(expected_result[1:])
        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'COUNT', 3, 'AGGREGATION',
                   'LAST', 5, 'FILTER', 'generation=x', conn=r).\
                   apply(lambda x: sorted(x, key=lambda x:x[0])[0][2]).equal(expected_result[0][2][:3])
        actual_result = r.execute_command('TS.mrange', start_ts, start_ts + samples_count, 'AGGREGATION', 'COUNT', 5, 'FILTER', 'generation=x')
        env.assertEqual([[1511885905, '1']], actual_result[0][2][:1])
        env.assertEqual(expected_result[0][2][1:9], actual_result[0][2][1:9])
        
        # checking that agg count before count works
        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'AGGREGATION', 'COUNT', 3, 'COUNT', 3, 
                   'FILTER', 'generation=x', conn=r).apply(lambda x: len(x[0][2])).equal(3) 
        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'COUNT', 3, 'AGGREGATION', 'COUNT', 3, 
                   'FILTER', 'generation=x', conn=r).apply(labmda x: len(x[0][2])).equal(3)
        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'AGGREGATION', 'COUNT', 3, 
                   'FILTER', 'generation=x', conn=r).apply(labmda x: len(x[0][2])).equal(18)

        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'AGGREGATION', 'invalid', 3, 'FILTER', 'generation=x', conn=r).error()
        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'AGGREGATION', 'AVG', 'string', 'FILTER', 'generation=x', conn=r).error()
        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'COUNT', 'string', 'FILTER', 'generation=x', conn=r).error()
        env.expect('TS.mrange', '-', '+' ,'FILTER', conn=r).error()  # missing args
        env.expect('TS.mrange', '-', '+', 'RETLIF', conn=r).error()  # no filter word
        env.expect('TS.mrange', 'string', start_ts + samples_count, 'FILTER', 'generation=x', conn=r).error()
        env.expect('TS.mrange', start_ts, 'string', 'FILTER', 'generation=x', conn=r).error()
        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'FILTER', 'generation+x', conn=r).error()
        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'FILTER', 'generation!=x', conn=r).error()

        # issue 414
        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'FILTER', 'name=(bob,rudy,)', conn=r).error()
        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'FILTER', 'name=(bob,,rudy)', conn=r).error()

        # test SELECTED_LABELS
        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'SELECTED_LABELS', 'filter', 'k!=5', conn=r).error()
        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'SELECTED_LABELS', 'filter', 'k!=5', conn=r).error()
        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'SELECTED_LABELS', 'WITHLABELS', 'filter', 'k!=5', conn=r).error()
        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'WITHLABELS', 'SELECTED_LABELS', 'filter', 'k!=5', conn=r).error()


def test_mrange_filterby(env):
    start_ts = 1511885909
    samples_count = 50

    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'tester1', 'LABELS', 'name', 'bob', 'class', 'middle', 'generation', 'x', conn=r).noError()
        env.expect('TS.CREATE', 'tester2', 'LABELS', 'name', 'rudy', 'class', 'junior', 'generation', 'x', conn=r).noError()
        env.expect('TS.CREATE', 'tester3', 'LABELS', 'name', 'fabi', 'class', 'top', 'generation', 'x', conn=r).noError()
        _insert_data(r, 'tester1', start_ts, samples_count, 5)
        _insert_data(r, 'tester2', start_ts, samples_count, 15)
        _insert_data(r, 'tester3', start_ts, samples_count, 25)


        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'FILTER_BY_VALUE', "a", 1 ,'FILTER', 'name=bob', conn=r).error()
        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'FILTER_BY_VALUE', "a", "a" ,'FILTER', 'name=bob', conn=r).error()
        env.expect('TS.mrange', start_ts, start_ts + samples_count, 'FILTER_BY_VALUE', 1, "a" ,'FILTER', 'name=bob', conn=r).error()

        expected_result = [['tester1', [], []],
                           ['tester2', [], [[start_ts + i, str(15)] for i in range(samples_count)]],
                           ['tester3', [], []],
                           ]
        actual_result = r.execute_command('TS.mrange', start_ts, start_ts + samples_count, 'FILTER_BY_VALUE', 10, 20,'FILTER', 'generation=x')
        env.assertEqual(sorted(actual_result), sorted(expected_result))

        expected_result = [['tester1', [], []],
                           ['tester2', [], [[start_ts + i, str(15)] for i in range(9, 12)]],
                           ['tester3', [], []],
                           ]
        actual_result = r.execute_command('TS.mrange', start_ts, start_ts + samples_count, 'FILTER_BY_TS', start_ts+9, start_ts+10, start_ts+11, 'FILTER_BY_VALUE', 10, 20,'FILTER', 'generation=x')
        env.assertEqual(sorted(actual_result), sorted(expected_result))

def test_mrange_withlabels(env):
    start_ts = 1511885909
    samples_count = 50

    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'tester1', 'LABELS', 'name', 'bob', 'class', 'middle', 'generation', 'x', conn=r).noError()
        env.expect('TS.CREATE', 'tester2', 'LABELS', 'name', 'rudy', 'class', 'junior', 'generation', 'x', conn=r).noError()
        env.expect('TS.CREATE', 'tester3', 'LABELS', 'name', 'fabi', 'class', 'top', 'generation', 'x', conn=r).noError()
        _insert_data(r, 'tester1', start_ts, samples_count, 5)
        _insert_data(r, 'tester2', start_ts, samples_count, 15)
        _insert_data(r, 'tester3', start_ts, samples_count, 25)

        expected_result = [[start_ts + i, str(5)] for i in range(samples_count)]
        actual_result = r.execute_command('TS.mrange', start_ts, start_ts + samples_count, 'WITHLABELS', 'FILTER',
                                          'name=bob')
        assert [['tester1', [['name', 'bob'], ['class', 'middle'], ['generation', 'x']],
                 expected_result]] == actual_result

        actual_result = r.execute_command('TS.mrange', start_ts, start_ts + samples_count, 'SELECTED_LABELS', 'name', 'generation', 'FILTER',
                                          'name=bob')
        assert [['tester1', [['name', 'bob'], ['generation', 'x']],
                 expected_result]] == actual_result

        actual_result = r.execute_command('TS.mrange', start_ts + 1, start_ts + samples_count, 'WITHLABELS',
                                          'AGGREGATION', 'COUNT', 1, 'FILTER', 'generation=x')
        # assert the labels length is 3 (name,class,generation) for each of the returned time-series
        assert len(actual_result[0][1]) == 3
        assert len(actual_result[1][1]) == 3
        assert len(actual_result[2][1]) == 3


def test_multilabel_filter(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'tester1', 'LABELS', 'name', 'bob', 'class', 'middle', 'generation', 'x', conn=r).noError()
        env.expect('TS.CREATE', 'tester2', 'LABELS', 'name', 'rudy', 'class', 'junior', 'generation', 'x', conn=r).noError()
        env.expect('TS.CREATE', 'tester3', 'LABELS', 'name', 'fabi', 'class', 'top', 'generation', 'x', conn=r).noError()

        env.expect('TS.ADD', 'tester1', 0, 1, conn=r).equal(0)
        env.expect('TS.ADD', 'tester2', 0, 2, conn=r).equal(0)
        env.expect('TS.ADD', 'tester3', 0, 3, conn=r).equal(0)

        actual_result = r.execute_command('TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'name=(bob,rudy)')
        assert set(item[0] for item in actual_result) == set(['tester1', 'tester2'])

        actual_result = r.execute_command('TS.mrange', 0, '+', 'WITHLABELS', 'FILTER', 'name=(bob,rudy)',
                                          'class!=(middle,top)')
        assert actual_result[0][0] == 'tester2'

        actual_result = r.execute_command('TS.mget', 'WITHLABELS', 'FILTER', 'name=(bob,rudy)')
        assert set(item[0] for item in actual_result) == set(['tester1', 'tester2'])

        actual_result = r.execute_command('TS.mget', 'WITHLABELS', 'FILTER', 'name=(bob,rudy)', 'class!=(middle,top)')
        assert actual_result[0][0] == 'tester2'

def test_large_key_value_pairs(env):
    with env.getClusterConnectionIfNeeded() as r:
        number_series = 100
        for i in range(0,number_series):
            env.expect('TS.CREATE', 'ts-{}'.format(i), 'LABELS', 'baseAsset', '17049', 'counterAsset', '840', 'source', '1000', 'dataType', 'PRICE_TICK', conn=r).noError()

        kv_label1 = 'baseAsset=(13830,10249,16019,10135,17049,10777,10138,11036,11292,15778,11043,10025,11436,12207,13359,10807,12216,11833,10170,10811,12864,12738,10053,11334,12487,12619,12364,13266,11219,15827,12374,11223,10071,12249,11097,14430,13282,16226,13667,11365,12261,12646,12650,12397,12785,13941,10231,16254,12159,15103)'
        kv_label2 = 'counterAsset=(840)'
        kv_label3 = 'source=(1000)'
        kv_label4 = 'dataType=(PRICE_TICK)'
        kv_labels = [kv_label1, kv_label2, kv_label3, kv_label4]
        for kv_label in kv_labels:
            res = r.execute_command('TS.MRANGE', '-', '+', 'FILTER', kv_label1)
            assert len(res) == number_series

def ensure_replies_series_match(env,series_array_1, series_array_2):
    for ts in series_array_1:
        ts_name = ts[0]
        ts_labels =ts[1]
        ts_values =ts[2]
        for comparison_ts in series_array_2:
            comparison_ts_name = comparison_ts[0]
            comparison_ts_labels =comparison_ts[1]
            comparison_ts_values =comparison_ts[2]
            if ts_name == comparison_ts_name:
                env.assertEqual(ts_labels,comparison_ts_labels)
                env.assertEqual(ts_values,comparison_ts_values)

def test_non_local_data(env):
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.ADD', '{host1}_metric_1', 1 ,100, 'LABELS', 'metric', 'cpu')
        r.execute_command('TS.ADD', '{host1}_metric_2', 2 ,40, 'LABELS', 'metric', 'cpu')
        r.execute_command('TS.ADD', '{host1}_metric_1', 2, 95)
        r.execute_command('TS.ADD', '{host1}_metric_1', 10, 99)

    previous_results = []
    # ensure that initiating the query on different shards always replies with the same series
    for shard in range(0, env.shardsCount):
        shard_conn = env.getConnection(shard)
        actual_result = shard_conn.execute_command('TS.MRANGE - + FILTER metric=cpu')
        env.assertEqual(len(actual_result),2)
        for previous_result in previous_results:
            ensure_replies_series_match(env,previous_result,actual_result)
        previous_results.append(actual_result)

def test_non_local_filtered_data(env):
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.ADD', '{host1}_metric_1', 1 ,100, 'LABELS', 'metric', 'cpu')
        r.execute_command('TS.ADD', '{host1}_metric_2', 2 ,40, 'LABELS', 'metric', 'cpu')
        r.execute_command('TS.ADD', '{host1}_metric_1', 2, 95)
        r.execute_command('TS.ADD', '{host1}_metric_1', 10, 99)

    previous_results = []
    # ensure that initiating the query on different shards always replies with the same series
    for shard in range(0, env.shardsCount):
        shard_conn = env.getConnection(shard)
        # send undordered timestamps to test for sorting
        actual_result = shard_conn.execute_command('TS.MRANGE - + FILTER_BY_TS 11 5 25 55 101 18 9 1900 2 FILTER metric=cpu')
        env.assertEqual(len(actual_result),2)

        # ensure reply is properly filtered by TS
        for serie in actual_result:
            serie_ts = serie[2]
            # ensure only timestamp 2 is present on reply
            env.assertEqual(len(serie_ts),1)
            env.assertEqual(serie_ts[0][0],2)

        for previous_result in previous_results:
            ensure_replies_series_match(env,previous_result,actual_result)
        previous_results.append(actual_result)

def test_non_local_filtered_labels(env):
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.ADD', '{host1}_metric_1', 1 ,100, 'LABELS', 'metric', 'cpu', '')
        r.execute_command('TS.ADD', '{host1}_metric_2', 2 ,40, 'LABELS', 'metric', 'cpu')
        r.execute_command('TS.ADD', '{host1}_metric_1', 2, 95)
        r.execute_command('TS.ADD', '{host1}_metric_1', 10, 99)

    previous_results = []
    # ensure that initiating the query on different shards always replies with the same series
    for shard in range(0, env.shardsCount):
        shard_conn = env.getConnection(shard)
        actual_result = shard_conn.execute_command('TS.MRANGE - + FILTER_BY_TS 2 SELECTED_LABELS metric FILTER metric=cpu')
        env.assertEqual(len(actual_result),2)
        for previous_result in previous_results:
            ensure_replies_series_match(env,previous_result,actual_result)
        previous_results.append(actual_result)

def test_mrange_align(env):
    start_ts = 1511885909
    samples_count = 50

    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'tester1', 'LABELS', 'name', 'bob', 'class', 'middle', 'generation', 'x', conn=r).noError()
        env.expect('TS.CREATE', 'tester2', 'LABELS', 'name', 'rudy', 'class', 'junior', 'generation', 'x', conn=r).noError()
        env.expect('TS.CREATE', 'tester3', 'LABELS', 'name', 'fabi', 'class', 'top', 'generation', 'x', conn=r).noError()
        _insert_data(r, 'tester1', start_ts, samples_count, 5)
        _insert_data(r, 'tester2', start_ts, samples_count, 15)
        _insert_data(r, 'tester3', start_ts, samples_count, 25)

        end_ts = start_ts + samples_count
        agg_bucket_size = 15
        expected_start_result = [
            ['tester1', [], build_expected_aligned_data(start_ts, start_ts + samples_count, agg_bucket_size, start_ts)],
            ['tester2', [], build_expected_aligned_data(start_ts, start_ts + samples_count, agg_bucket_size, start_ts)],
            ['tester3', [], build_expected_aligned_data(start_ts, start_ts + samples_count, agg_bucket_size, start_ts)],
        ]
        expected_end_result = [
            ['tester1', [], build_expected_aligned_data(start_ts, start_ts + samples_count, agg_bucket_size, end_ts)],
            ['tester2', [], build_expected_aligned_data(start_ts, start_ts + samples_count, agg_bucket_size, end_ts)],
            ['tester3', [], build_expected_aligned_data(start_ts, start_ts + samples_count, agg_bucket_size, end_ts)],
        ]

        assert expected_start_result == sorted(r.execute_command('TS.mrange', start_ts, start_ts + samples_count, 'ALIGN', '-', 'AGGREGATION', 'COUNT', agg_bucket_size, 'FILTER', 'generation=x'))
        assert expected_end_result == sorted(r.execute_command('TS.mrange', start_ts, start_ts + samples_count, 'ALIGN', '+', 'AGGREGATION', 'COUNT', agg_bucket_size, 'FILTER', 'generation=x'))

        def groupby(data):
            result =  defaultdict(lambda: 0)
            for key, labels, samples in data:
                for sample in samples:
                    result[sample[0]] = max(result[sample[0]], int(sample[1]))
            return [[s[0], str(s[1])] for s in result.items()]

        expected_groupby_start_result = [['generation=x', [], groupby(expected_start_result)]]
        expected_groupby_end_result = [['generation=x', [], groupby(expected_end_result)]]

        assert expected_groupby_start_result == r.execute_command('TS.mrange', start_ts, start_ts + samples_count, 'ALIGN', '-', 'AGGREGATION', 'COUNT', agg_bucket_size, 'FILTER', 'generation=x', 'GROUPBY', 'generation', 'REDUCE', 'max')
        assert expected_groupby_end_result == r.execute_command('TS.mrange', start_ts, start_ts + samples_count, 'ALIGN', '+', 'AGGREGATION', 'COUNT', agg_bucket_size, 'FILTER', 'generation=x', 'GROUPBY', 'generation', 'REDUCE', 'max')
