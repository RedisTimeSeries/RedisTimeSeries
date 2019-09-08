import os
import redis
import pytest
import time
import __builtin__
import math
import random
import statistics 
from rmtest import ModuleTestCase

if os.environ['REDISTIMESERIES'] != '':
    REDISTIMESERIES = os.environ['REDISTIMESERIES']
else:
    REDISTIMESERIES = os.path.dirname(os.path.abspath(__file__)) + '/redistimeseries.so'

ALLOWED_ERROR = 0.001

def assertInitArgsFail(self):
    try:
        c, s = self.client, self.server
    except Exception:
        delattr(self, '_server')
        self.assertOk('OK')
    else:
        self.assertOk('NotOK')  

def assertInitArgsSuccess(self):
    c, s = self.client, self.server
    self.assertOk('OK', self.cmd('set', 'test', 'foo'))

class RedisTimeseriesTests(ModuleTestCase(REDISTIMESERIES)):
    def _get_ts_info(self, redis, key):
        info = redis.execute_command('TS.INFO', key)
        return dict([(info[i], info[i+1]) for i in range(0, len(info), 2)])

    def _assert_alter_cmd(self, r, key, start_ts, end_ts,
                          expected_data=None,
                          expected_retention=None,
                          expected_chunk_size=None,
                          expected_labels=None):
        """
        Test modifications didn't change the data
        :param r: Redis instance
        :param key: redis key
        :param start_ts:
        :param end_ts:
        :param expected_data:
        :return:
        """

        actual_result = self._get_ts_info(r, key)

        if expected_data:
            actual_data = r.execute_command('TS.range', key, start_ts, end_ts)
            assert expected_data == actual_data
        if expected_retention:
            assert expected_retention == actual_result['retentionTime']
        if expected_labels:
            assert expected_labels == actual_result['labels']
        if expected_chunk_size:
            assert expected_chunk_size == actual_result['maxSamplesPerChunk']

    @staticmethod
    def _ts_alter_cmd(r, key, set_retention=None, set_chunk_size=None, set_labels=None):
        """
        Assert that changed data is the same as expected, with or without modification of label or retention.
        :param r: Redis instance
        :param key: redis key to work on
        :param set_retention: If none will skip (and test that didn't change from expected result)
        :param set_labels: Format is list. If none will skip (and test that didn't change from expected result)
        :return:
        """
        cmd = ['TS.ALTER', key]
        if set_retention:
            cmd.extend(['RETENTION', set_retention])
        if set_chunk_size:
            cmd.extend(['CHUNK_SIZE', set_chunk_size])
        if set_labels:
            new_labels = [item for sublist in set_labels for item in sublist]
            cmd.extend(['LABELS'])
            cmd.extend(new_labels)
        r.execute_command(*cmd)

    @staticmethod
    def _insert_data(redis, key, start_ts, samples_count, value):
        """
        insert data to key, starting from start_ts, with 1 sec interval between them
        :param redis: redis connection
        :param key: name of time_series
        :param start_ts: beginning of time series
        :param samples_count: number of samples
        :param value: could be a list of samples_count values, or one value. if a list, insert the values in their
        order, if not, insert the single value for all the timestamps
        """
        for i in range(samples_count):
            value_to_insert = value[i] if type(value) == list else value
            actual_result = redis.execute_command('TS.ADD', key, start_ts + i, value_to_insert)
            if type(actual_result) == long:               
                assert actual_result == long(start_ts + i)
            else:
                assert actual_result

    def _insert_agg_data(self, redis, key, agg_type):
        agg_key = '%s_agg_%s_10' % (key, agg_type)

        assert redis.execute_command('TS.CREATE', key)
        assert redis.execute_command('TS.CREATE', agg_key)
        assert redis.execute_command('TS.CREATERULE', key, agg_key, "AGGREGATION", agg_type, 10)

        values = (31, 41, 59, 26, 53, 58, 97, 93, 23, 84)
        for i in range(10, 50):
            assert redis.execute_command('TS.ADD', key, i, i // 10 * 100 + values[i % 10])

        return agg_key

    @staticmethod
    def _get_series_value(ts_key_result):
        """
        Get only the values from the time stamp series
        :param ts_key_result: the output of ts.range command (pairs of timestamp and value)
        :return: float values of all the values in the series
        """
        return [float(value[1]) for value in ts_key_result]

    @staticmethod
    def _calc_downsampling_series(values, bucket_size, calc_func):
        """
        calculate the downsampling series given the wanted calc_func, by applying to calc_func to all the full buckets
        and to the remainder bucket
        :param values: values of original series
        :param bucket_size: bucket size for downsampling
        :param calc_func: function that calculates the wanted rule, for example min/sum/avg
        :return: the values of the series after downsampling
        """
        series = []
        for i in range(0, int(math.ceil(len(values) / float(bucket_size)))):
            curr_bucket_size = bucket_size
            # we don't have enough values for a full bucket anymore
            if (i + 1) * bucket_size > len(values):
                curr_bucket_size = len(values) - i
            series.append(calc_func(values[i * bucket_size: i * bucket_size + curr_bucket_size]))
        return series

    def calc_rule(self, rule, values, bucket_size):
        """
        Calculate the downsampling with the given rule
        :param rule: 'avg' / 'max' / 'min' / 'sum' / 'count'
        :param values: original series values
        :param bucket_size: bucket size for downsampling
        :return: the values of the series after downsampling
        """
        if rule == 'avg':
            return self._calc_downsampling_series(values, bucket_size, lambda x: float(sum(x)) / len(x))
        elif rule in ['sum', 'max', 'min']:
            return self._calc_downsampling_series(values, bucket_size, getattr(__builtin__, rule))
        elif rule == 'count':
            return self._calc_downsampling_series(values, bucket_size, len)

    def test_sanity(self):
        start_ts = 1511885909L
        samples_count = 1500
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester', 'RETENTION', '0', 'CHUNK_SIZE', '360', 'LABELS', 'name',
                                     'brown', 'color', 'pink')
            self._insert_data(r, 'tester', start_ts, samples_count, 5)

            expected_result = [[start_ts+i, str(5)] for i in range(samples_count)]
            actual_result = r.execute_command('TS.range', 'tester', start_ts, start_ts + samples_count)
            assert expected_result == actual_result

            expected_result = {'chunkCount': math.ceil((samples_count + 1) / 360.0),
              'labels': [['name', 'brown'], ['color', 'pink']],
              'lastTimestamp': start_ts + samples_count - 1,
              'maxSamplesPerChunk': 360L,
              'retentionTime': 0L,
              'sourceKey': None,
              'rules': []}
            actual_result = self._get_ts_info(r, 'tester')
            assert expected_result == actual_result


    def test_rdb(self):
        start_ts = 1511885909L
        samples_count = 1500
        data = None
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester', 'RETENTION', '0', 'CHUNK_SIZE', '360', 'LABELS', 'name', 'brown', 'color', 'pink')
            assert r.execute_command('TS.CREATE', 'tester_agg_avg_10')
            assert r.execute_command('TS.CREATE', 'tester_agg_max_10')
            assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_avg_10', 'AGGREGATION', 'AVG', 10)
            assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_max_10', 'AGGREGATION', 'MAX', 10)
            self._insert_data(r, 'tester', start_ts, samples_count, 5)
            data = r.execute_command('dump', 'tester')

        with self.redis() as r:
            r.execute_command('RESTORE', 'tester', 0, data)
            expected_result = [[start_ts+i, str(5)] for i in range(samples_count)]
            actual_result = r.execute_command('TS.range', 'tester', start_ts, start_ts + samples_count)
            assert expected_result == actual_result
            actual_result = r.execute_command('TS.range', 'tester', start_ts, start_ts + samples_count, 'count', 3)
            assert expected_result[:3] == actual_result

            expected_result = {'chunkCount': math.ceil((samples_count + 1) / 360.0),
                               'labels': [['name', 'brown'], ['color', 'pink']],
                               'lastTimestamp': 1511887408L,
                               'maxSamplesPerChunk': 360L,
                               'retentionTime': 0L,
                               'sourceKey': None,
                               'rules': [['tester_agg_avg_10', 10L, 'AVG'], ['tester_agg_max_10', 10L, 'MAX']]}
            actual_result = self._get_ts_info(r, 'tester')
            assert expected_result == actual_result

    def test_rdb_aggregation_context(self):
        """
        Check that the aggregation context of the rules is saved in rdb. Write data with not a full bucket,
        then save it and restore, add more data to the bucket and check the rules results considered the previous data
        that was in that bucket in their calculation. Check on avg and min, since all the other rules use the same
        context as min.
        """
        start_ts = 3
        samples_count = 4  # 1 full bucket and another one with 1 value
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            assert r.execute_command('TS.CREATE', 'tester_agg_avg_3')
            assert r.execute_command('TS.CREATE', 'tester_agg_min_3')
            assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_avg_3', 'AGGREGATION', 'AVG', 3)
            assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_min_3', 'AGGREGATION', 'MIN', 3)
            self._insert_data(r, 'tester', start_ts, samples_count, range(samples_count))
            data_tester = r.execute_command('dump', 'tester')
            data_avg_tester = r.execute_command('dump', 'tester_agg_avg_3')
            data_min_tester = r.execute_command('dump', 'tester_agg_min_3')

        with self.redis() as r:
            r.execute_command('RESTORE', 'tester', 0, data_tester)
            r.execute_command('RESTORE', 'tester_agg_avg_3', 0, data_avg_tester)
            r.execute_command('RESTORE', 'tester_agg_min_3', 0, data_min_tester)
            assert r.execute_command('TS.ADD', 'tester', start_ts + samples_count, samples_count)
            # if the aggregation context wasn't saved, the results were considering only the new value added
            expected_result_avg = [[start_ts, '1'], [start_ts + 3, '3.5']]
            expected_result_min = [[start_ts, '0'], [start_ts + 3, '3']]
            actual_result_avg = r.execute_command('TS.range', 'tester_agg_avg_3', start_ts, start_ts + samples_count)
            assert actual_result_avg == expected_result_avg
            actual_result_min = r.execute_command('TS.range', 'tester_agg_min_3', start_ts, start_ts + samples_count)
            assert actual_result_min == expected_result_min

    def test_sanity_pipeline(self):
        start_ts = 1488823384L
        samples_count = 1500
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            with r.pipeline(transaction=False) as p:
                p.set("name", "danni")
                self._insert_data(p, 'tester', start_ts, samples_count, 5)
                p.execute()
            expected_result = [[start_ts+i, str(5)] for i in range(samples_count)]
            actual_result = r.execute_command('TS.range', 'tester', start_ts, start_ts + samples_count)
            assert expected_result == actual_result

    def test_alter_cmd(self):
        start_ts = 1511885909L
        samples_count = 1500
        end_ts = start_ts + samples_count
        key = 'tester'

        with self.redis() as r:
            assert r.execute_command('TS.CREATE', key, 'CHUNK_SIZE', '360',
                                     'LABELS', 'name', 'brown', 'color', 'pink')
            self._insert_data(r, key, start_ts, samples_count, 5)

            expected_data = [[start_ts + i, str(5)] for i in range(samples_count)]

            # test alter retention, chunk size and labels
            expected_labels = [['A', '1'], ['B', '2'], ['C', '3']]
            expected_retention = 500
            expected_chunk_size = 100
            self._ts_alter_cmd(r, key, expected_retention, expected_chunk_size, expected_labels)
            self._assert_alter_cmd(r, key, end_ts-501, end_ts, expected_data[-501:], expected_retention,
                                   expected_chunk_size, expected_labels)
            
            # test alter retention
            expected_retention = 200
            self._ts_alter_cmd(r, key, set_retention=expected_retention)
            self._assert_alter_cmd(r, key, end_ts-201, end_ts, expected_data[-201:], expected_retention,
                                   expected_chunk_size, expected_labels)
            
            # test alter chunk size
            expected_chunk_size = 100
            expected_labels = [['A', '1'], ['B', '2'], ['C', '3']]
            self._ts_alter_cmd(r, key, set_chunk_size=expected_chunk_size)
            self._assert_alter_cmd(r, key, end_ts-201, end_ts, expected_data[-201:], expected_retention,
                                   expected_chunk_size, expected_labels)
            

            # test alter labels
            expected_labels = [['A', '1']]
            self._ts_alter_cmd(r, key, expected_retention, set_labels=expected_labels)
            self._assert_alter_cmd(r, key, end_ts-201, end_ts, expected_data[-201:], expected_retention,
                                   expected_chunk_size, expected_labels)

            # test indexer was updated
            assert r.execute_command('TS.QUERYINDEX', 'A=1') == [key]
            assert r.execute_command('TS.QUERYINDEX', 'name=brown') == []

    def test_mget_cmd(self):
        num_of_keys = 3
        time_stamp = 1511885909L
        keys = ['k1', 'k2', 'k3']
        labels = ['a', 'a', 'b']
        values = [100, 200, 300]

        with self.redis() as r:
            for i in range(num_of_keys):
                assert r.execute_command('TS.CREATE', keys[i], 'LABELS', labels[i], '1')

                assert r.execute_command('TS.ADD', keys[i], time_stamp - 1, values[i] - 1)
                assert r.execute_command('TS.ADD', keys[i], time_stamp, values[i])

            assert r.execute_command('TS.MGET', 'FILTER', 'a=1') == [
                [keys[0], [[labels[0], '1']], time_stamp, str(values[0])],
                [keys[1], [[labels[1], '1']], time_stamp, str(values[1])]
            ]

            # negative test
            assert not r.execute_command('TS.MGET', 'FILTER', 'a=100')
            assert not r.execute_command('TS.MGET', 'FILTER', 'k=1')

    def test_range_query(self):
        start_ts = 1488823384L
        samples_count = 1500
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester', 'RETENTION', samples_count-100)
            self._insert_data(r, 'tester', start_ts, samples_count, 5)

            expected_result = [[start_ts+i, str(5)] for i in range(99, 151)]
            actual_result = r.execute_command('TS.range', 'tester', start_ts+50, start_ts+150)
            assert expected_result == actual_result

    def test_range_with_agg_query(self):
        start_ts = 1488823384L
        samples_count = 1500
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            self._insert_data(r, 'tester', start_ts, samples_count, 5)

            expected_result = [[1488823000L, '116'], [1488823500L, '500'], [1488824000L, '500'], [1488824500L, '384']]
            actual_result = r.execute_command('TS.range', 'tester', start_ts, start_ts + samples_count, 'AGGREGATION',
                                              'count', 500)
            assert expected_result == actual_result

    def test_compaction_rules(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester', 'CHUNK_SIZE', '360')
            assert r.execute_command('TS.CREATE', 'tester_agg_max_10')
            assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_max_10', 'AGGREGATION', 'avg', 10)

            start_ts = 1488823384L
            samples_count = 1500
            self._insert_data(r, 'tester', start_ts, samples_count, 5)

            actual_result = r.execute_command('TS.RANGE', 'tester_agg_max_10', start_ts, start_ts + samples_count)

            assert len(actual_result) == samples_count/10

            info_dict = self._get_ts_info(r, 'tester')
            assert info_dict == {'chunkCount': math.ceil((samples_count + 1) / 360.0),
                                 'lastTimestamp': start_ts + samples_count -1,
                                 'maxSamplesPerChunk': 360L,
                                 'retentionTime': 0L,
                                 'labels': [],
                                 'sourceKey': None,
                                 'rules': [['tester_agg_max_10', 10L, 'AVG']]}          
            
    def test_delete_key(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester', 'CHUNK_SIZE', '360')
            assert r.execute_command('TS.CREATE', 'tester_agg_max_10')
            assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_max_10', 'AGGREGATION', 'avg', 10)
            assert r.delete('tester_agg_max_10')
            assert len(self._get_ts_info(r, 'tester')['rules']) == 0
            
            assert r.execute_command('TS.CREATE', 'tester_agg_max_10')
            assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_max_10', 'AGGREGATION', 'avg', 11)
            assert r.delete('tester')
            assert self._get_ts_info(r, 'tester_agg_max_10')['sourceKey'] == None
            
            assert r.execute_command('TS.CREATE', 'tester')
            assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_max_10', 'AGGREGATION', 'avg', 12)
            assert len(self._get_ts_info(r, 'tester')['rules']) == 1
    
    def test_create_retention(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester', 'RETENTION', 1000)
            
            assert r.execute_command('TS.ADD', 'tester', 500, 10)
            expected_result = [[500L, '10']]
            actual_result = r.execute_command('TS.range', 'tester', '-', '+')
            assert expected_result == actual_result
            
            assert r.execute_command('TS.ADD', 'tester', 1001, 20)
            expected_result = [[500L, '10'], [1001L, '20']]
            actual_result = r.execute_command('TS.range', 'tester', '-', '+')
            assert expected_result == actual_result            
            
            assert r.execute_command('TS.ADD', 'tester', 2000, 30)
            expected_result = [[1001L, '20'], [2000L, '30']]
            actual_result = r.execute_command('TS.range', 'tester', '-', '+')
            assert expected_result == actual_result   

            with pytest.raises(redis.ResponseError) as excinfo:
                assert r.execute_command('TS.CREATE', 'negative', 'RETENTION', -10)

    def test_create_with_negative_chunk_size(self):
        with self.redis() as r:
            with pytest.raises(redis.ResponseError) as excinfo:
                assert r.execute_command('TS.CREATE', 'tester', 'CHUNK_SIZE', -10)

    def test_check_retention_64bit(self):
        with self.redis() as r:
            huge_timestamp = 4000000000 # larger than uint32
            r.execute_command('TS.CREATE', 'tester', 'RETENTION', huge_timestamp)
            info = r.execute_command('TS.INFO', 'tester')
            assert info[3] == huge_timestamp

            r.execute_command('TS.ADD', 'tester', huge_timestamp, '1')
            assert r.execute_command('TS.RANGE', 'tester', 0, -1) == [[huge_timestamp, '1']]
            r.execute_command('TS.ADD', 'tester', huge_timestamp * 3, '2')
            assert r.execute_command('TS.RANGE', 'tester', 0, -1) == [[huge_timestamp * 3, '2']]

    def test_create_compaction_rule_with_wrong_aggregation(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            assert r.execute_command('TS.CREATE', 'tester_agg_max_10')
            with pytest.raises(redis.ResponseError) as excinfo:
                assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_max_10', 'AGGREGATION', 'MAXX', 10)

            with pytest.raises(redis.ResponseError) as excinfo:
                assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_max_10', 'AGGREGATION', 'MA', 10)

    def test_create_compaction_rule_without_dest_series(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            with pytest.raises(redis.ResponseError) as excinfo:
                assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_max_10', 'AGGREGATION', 'MAX', 10)

    def test_create_compaction_rule_twice(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            assert r.execute_command('TS.CREATE', 'tester_agg_max_10')
            assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_max_10', 'AGGREGATION', 'MAX', 10)
            with pytest.raises(redis.ResponseError) as excinfo:
                assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_max_10', 'AGGREGATION', 'MAX', 10)
                
    def test_create_compaction_rule_override_dest(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            assert r.execute_command('TS.CREATE', 'tester2')
            assert r.execute_command('TS.CREATE', 'tester_agg_max_10')
            assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_max_10', 'AGGREGATION', 'MAX', 10)
            with pytest.raises(redis.ResponseError) as excinfo:
                assert r.execute_command('TS.CREATERULE', 'tester2', 'tester_agg_max_10', 'AGGREGATION', 'MAX', 10)

    def test_create_compaction_rule_from_target(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            assert r.execute_command('TS.CREATE', 'tester2')
            assert r.execute_command('TS.CREATE', 'tester_agg_max_10')
            assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_max_10', 'AGGREGATION', 'MAX', 10)
            with pytest.raises(redis.ResponseError) as excinfo:
                assert r.execute_command('TS.CREATERULE', 'tester_agg_max_10', 'tester2', 'AGGREGATION', 'MAX', 10)

    def test_create_compaction_rule_own(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            with pytest.raises(redis.ResponseError) as excinfo:
                assert r.execute_command('TS.CREATERULE', 'tester', 'tester', 'AGGREGATION', 'MAX', 10)

    def test_create_compaction_rule_and_del_dest_series(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            assert r.execute_command('TS.CREATE', 'tester_agg_max_10')
            assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_max_10', 'AGGREGATION', 'AVG', 10)
            assert r.delete('tester_agg_max_10')

            start_ts = 1488823384L
            samples_count = 1500
            self._insert_data(r, 'tester', start_ts, samples_count, 5)

    def test_delete_rule(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            assert r.execute_command('TS.CREATE', 'tester_agg_max_10')
            assert r.execute_command('TS.CREATE', 'tester_agg_min_20')
            assert r.execute_command('TS.CREATE', 'tester_agg_avg_30')
            assert r.execute_command('TS.CREATE', 'tester_agg_last_40')
            assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_max_10', 'AGGREGATION', 'MAX', 10)
            assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_min_20', 'AGGREGATION', 'MIN', 20)
            assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_avg_30', 'AGGREGATION', 'AVG', 30)
            assert r.execute_command('TS.CREATERULE', 'tester', 'tester_agg_last_40', 'AGGREGATION', 'LAST', 40)

            with pytest.raises(redis.ResponseError) as excinfo:
                assert r.execute_command('TS.DELETERULE', 'tester', 'non_existent')

            assert len(self._get_ts_info(r, 'tester')['rules']) == 4
            assert r.execute_command('TS.DELETERULE', 'tester', 'tester_agg_avg_30')
            assert len(self._get_ts_info(r, 'tester')['rules']) == 3
            assert r.execute_command('TS.DELETERULE', 'tester', 'tester_agg_max_10')
            assert len(self._get_ts_info(r, 'tester')['rules']) == 2

    def test_empty_series(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            assert r.execute_command('DUMP', 'tester')

    def test_valid_labels(self):
        with self.redis() as r:
            with pytest.raises(redis.ResponseError) as excinfo:
                r.execute_command('TS.CREATE', 'tester', 'LABELS', 'name', '')
            with pytest.raises(redis.ResponseError) as excinfo:
                r.execute_command('TS.ADD', 'tester2', '*', 1, 'LABELS', 'name', 'myName', 'location', '')
            with pytest.raises(redis.ResponseError) as excinfo:
                r.execute_command('TS.ADD', 'tester2', '*', 1, 'LABELS', 'name', 'myName', 'location', 'list)')
            with pytest.raises(redis.ResponseError) as excinfo:
                r.execute_command('TS.ADD', 'tester2', '*', 1, 'LABELS', 'name', 'myName', 'location', 'li(st')
            with pytest.raises(redis.ResponseError) as excinfo:
                r.execute_command('TS.ADD', 'tester2', '*', 1, 'LABELS', 'name', 'myName', 'location', 'lis,t')

    def test_incrby_reset(self):
        with self.redis() as r:
            r.execute_command('ts.create', 'tester')

            time_bucket = 10*1000
            start_time = long(time.time()*1000)
            start_time = start_time - start_time % time_bucket
            for _ in range(1000):
                r.execute_command('ts.incrby', 'tester', '1', 'RESET', time_bucket)

            assert r.execute_command('TS.RANGE', 'tester', 0, int(time.time()*1000)) == [[start_time, '1000']]

    def test_incrby_reset_timestamp(self):
        with self.redis() as r:
            r.execute_command('ts.create', 'tester')

            time_bucket = 1000
            quantity = 100
            start_time = 0
            for _ in range(quantity):
                r.execute_command('ts.incrby', 'tester', '1', 'timestamp', start_time, 'RESET', time_bucket)
            for _ in range(quantity):
                r.execute_command('ts.incrby', 'tester', '1', 'timestamp', start_time + time_bucket, 'RESET', time_bucket)

            assert r.execute_command('TS.RANGE', 'tester', 0, int(2 * time_bucket)) == [[0, '100'], [1000, '200']]
        
    def test_incrby(self):
        with self.redis() as r:
            r.execute_command('ts.create', 'tester')

            start_incr_time = int(time.time()*1000)
            for i in range(20):
                r.execute_command('ts.incrby', 'tester', '5')

            start_decr_time = int(time.time()*1000)
            for i in range(20):
                r.execute_command('ts.decrby', 'tester', '1.5')

            now = int(time.time()*1000)
            result = r.execute_command('TS.RANGE', 'tester', 0, now)
            assert result[-1][1] == '70'
            assert result[-1][0] <= now
            assert result[0][0] >= start_incr_time
            assert len(result) <= 40

    def test_incrby_with_timestamp(self):
        with self.redis() as r:
            r.execute_command('ts.create', 'tester')

            for i in range(20):
                assert r.execute_command('ts.incrby', 'tester', '5', 'TIMESTAMP', i) == 'OK'
            result = r.execute_command('TS.RANGE', 'tester', 0, 20)
            assert len(result) == 20
            assert result[19][1] == '100'

            assert r.execute_command('ts.incrby', 'tester', '5', 'TIMESTAMP', '*') == 'OK'

    def test_agg_min(self):
        with self.redis() as r:
            agg_key = self._insert_agg_data(r, 'tester', 'min')

            expected_result = [[10, '123'], [20, '223'], [30, '323'], [40, '423']]
            actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
            assert expected_result == actual_result

    def test_agg_max(self):
        with self.redis() as r:
            agg_key = self._insert_agg_data(r, 'tester', 'max')

            expected_result = [[10, '197'], [20, '297'], [30, '397'], [40, '497']]
            actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
            assert expected_result == actual_result

    def test_agg_avg(self):
        with self.redis() as r:
            agg_key = self._insert_agg_data(r, 'tester', 'avg')

            expected_result = [[10, '156.5'], [20, '256.5'], [30, '356.5'], [40, '456.5']]
            actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
            assert expected_result == actual_result

    def test_std_var_func(self):
        with self.redis() as r:
            raw_key = 'raw'
            std_key = 'std_key'
            var_key = 'var_key'

            random_numbers = 100
            random.seed(0)
            items = random.sample(range(random_numbers), random_numbers)
        
            stdev = statistics.stdev(items)
            var = statistics.variance(items)
            assert r.execute_command('TS.CREATE', raw_key)
            assert r.execute_command('TS.CREATE', std_key)
            assert r.execute_command('TS.CREATE', var_key)
            assert r.execute_command('TS.CREATERULE', raw_key, std_key, "AGGREGATION", 'std.s', random_numbers)
            assert r.execute_command('TS.CREATERULE', raw_key, var_key, "AGGREGATION", 'var.s', random_numbers)
    
            for i in range(random_numbers):
                r.execute_command('TS.ADD', raw_key, i, items[i])
    
            assert abs(stdev - float(r.execute_command('TS.GET', std_key)[1])) < ALLOWED_ERROR
            assert abs(var - float(r.execute_command('TS.GET', var_key)[1])) < ALLOWED_ERROR        

    def test_agg_std_p(self):
        with self.redis() as r:
            agg_key = self._insert_agg_data(r, 'tester', 'std.p')

            expected_result = [[10, '25.869'], [20, '25.869'], [30, '25.869'], [40, '25.869']]
            actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
            for i in range(len(expected_result)):
                assert abs(float(expected_result[i][1]) - float(actual_result[i][1])) < ALLOWED_ERROR                

    def test_agg_std_s(self):
        with self.redis() as r:
            agg_key = self._insert_agg_data(r, 'tester', 'std.s')

            expected_result = [[10, '27.269'], [20, '27.269'], [30, '27.269'], [40, '27.269']]
            actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
            for i in range(len(expected_result)):
                assert abs(float(expected_result[i][1]) - float(actual_result[i][1])) < ALLOWED_ERROR                

    def test_agg_var_p(self):
        with self.redis() as r:
            agg_key = self._insert_agg_data(r, 'tester', 'var.p')

            expected_result = [[10, '669.25'], [20, '669.25'], [30, '669.25'], [40, '669.25']]
            actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
            for i in range(len(expected_result)):
                assert abs(float(expected_result[i][1]) - float(actual_result[i][1])) < ALLOWED_ERROR                

    def test_agg_var_s(self):
        with self.redis() as r:
            agg_key = self._insert_agg_data(r, 'tester', 'var.s')

            expected_result = [[10, '743.611'], [20, '743.611'], [30, '743.611'], [40, '743.611']]
            actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
            for i in range(len(expected_result)):
                assert abs(float(expected_result[i][1]) - float(actual_result[i][1])) < ALLOWED_ERROR                

    def test_agg_sum(self):
        with self.redis() as r:
            agg_key = self._insert_agg_data(r, 'tester', 'sum')

            expected_result = [[10, '1565'], [20, '2565'], [30, '3565'], [40, '4565']]
            actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
            assert expected_result == actual_result

    def test_agg_count(self):
        with self.redis() as r:
            agg_key = self._insert_agg_data(r, 'tester', 'count')

            expected_result = [[10, '10'], [20, '10'], [30, '10'], [40, '10']]
            actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
            assert expected_result == actual_result

    def test_agg_first(self):
        with self.redis() as r:
            agg_key = self._insert_agg_data(r, 'tester', 'first')

            expected_result = [[10, '131'], [20, '231'], [30, '331'], [40, '431']]
            actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
            assert expected_result == actual_result

    def test_agg_last(self):
        with self.redis() as r:
            agg_key = self._insert_agg_data(r, 'tester', 'last')

            expected_result = [[10, '184'], [20, '284'], [30, '384'], [40, '484']]
            actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
            assert expected_result == actual_result

    def test_agg_range(self):
        with self.redis() as r:
            agg_key = self._insert_agg_data(r, 'tester', 'range')

            expected_result = [[10, '74'], [20, '74'], [30, '74'], [40, '74']]
            actual_result = r.execute_command('TS.RANGE', agg_key, 10, 50)
            assert expected_result == actual_result

    def test_downsampling_rules(self):
        """
        Test downsmapling rules - avg,min,max,count,sum with 4 keys each.
        Downsample in resolution of:
        1sec (should be the same length as the original series),
        3sec (number of samples is divisible by 10),
        10s (number of samples is not divisible by 10),
        1000sec (series should be empty since there are not enough samples)
        Insert some data and check that the length, the values and the info of the downsample series are as expected.
        """
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            rules = ['avg', 'sum', 'count', 'max', 'min']
            resolutions = [1, 3, 10, 1000]
            for rule in rules:
                for resolution in resolutions:
                    assert r.execute_command('TS.CREATE', 'tester_{}_{}'.format(rule, resolution))
                    assert r.execute_command('TS.CREATERULE', 'tester', 'tester_{}_{}'.format(rule, resolution),
                                             'AGGREGATION', rule, resolution)

            start_ts = 0
            samples_count = 501
            end_ts = start_ts + samples_count
            values = range(samples_count)
            self._insert_data(r, 'tester', start_ts, samples_count, values)

            for rule in rules:
                for resolution in resolutions:
                    actual_result = r.execute_command('TS.RANGE', 'tester_{}_{}'.format(rule, resolution),
                                                      start_ts, end_ts)
                    assert len(actual_result) == math.ceil(samples_count / float(resolution))
                    expected_result = self.calc_rule(rule, values, resolution)
                    assert self._get_series_value(actual_result) == expected_result
                    # last time stamp should be the beginning of the last bucket
                    assert self._get_ts_info(r, 'tester_{}_{}'.format(rule, resolution))['lastTimestamp'] == \
                                            (samples_count - 1) - (samples_count - 1) % resolution

    def test_automatic_timestamp(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            curr_time = int(time.time()*1000)
            response_timestamp = r.execute_command('TS.ADD', 'tester', '*', 1)
            result = r.execute_command('TS.RANGE', 'tester', 0, int(time.time() * 1000))
            # test time difference is not more than 5 milliseconds
            assert result[0][0] - curr_time <= 5
            assert response_timestamp - curr_time <= 5

    def test_add_create_key(self):
        with self.redis() as r:
            ts = time.time()
            assert r.execute_command('TS.ADD', 'tester1', str(int(ts)), str(ts), 'RETENTION', '666', 'LABELS', 'name', 'blabla') == int(ts)
            assert r.execute_command('TS.INFO', 'tester1') == [
                'lastTimestamp',
                long(ts),
                'retentionTime',
                666L,
                'chunkCount',
                1L,
                'maxSamplesPerChunk',
                360L,
                'labels',
                [
                    ['name',
                    'blabla']
                ],
                'sourceKey',
                None,
                'rules',
                []
            ]

            assert r.execute_command('TS.ADD', 'tester2', str(int(ts)), str(ts), 'LABELS', 'name', 'blabla2', 'location', 'earth')
            assert r.execute_command('TS.INFO', 'tester2') == [
                'lastTimestamp',
                long(ts),
                'retentionTime',
                0L,
                'chunkCount',
                1L,
                'maxSamplesPerChunk',
                360L,
                'labels',
                [
                    [
                        'name',
                        'blabla2'
                     ],
                    [
                        'location',
                        'earth'
                    ]
                ],
                'sourceKey',
                None,
                'rules',
                []
            ]

    def test_range_by_labels(self):
        start_ts = 1511885909L
        samples_count = 50

        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester1', 'LABELS', 'name', 'bob', 'class', 'middle', 'generation', 'x')
            assert r.execute_command('TS.CREATE', 'tester2', 'LABELS', 'name', 'rudy', 'class', 'junior', 'generation', 'x')
            assert r.execute_command('TS.CREATE', 'tester3', 'LABELS', 'name', 'fabi', 'class', 'top', 'generation', 'x')
            self._insert_data(r, 'tester1', start_ts, samples_count, 5)
            self._insert_data(r, 'tester2', start_ts, samples_count, 15)
            self._insert_data(r, 'tester3', start_ts, samples_count, 25)


            expected_result = [[start_ts+i, str(5)] for i in range(samples_count)]
            actual_result = r.execute_command('TS.mrange', start_ts, start_ts + samples_count, 'FILTER', 'name=bob')
            assert [['tester1', [['name', 'bob'], ['class', 'middle'], ['generation', 'x']], expected_result]] == actual_result

            def build_expected(val, time_bucket):
                return [[long(i - i%time_bucket), str(val)] for i in range(start_ts, start_ts+samples_count+1, time_bucket)]
            actual_result = r.execute_command('TS.mrange', start_ts, start_ts + samples_count, 'AGGREGATION', 'LAST', 5, 'FILTER', 'generation=x')
            expected_result = [['tester1', [['name', 'bob'], ['class', 'middle'], ['generation', 'x']], build_expected(5, 5)],
                    ['tester2', [['name', 'rudy'], ['class', 'junior'], ['generation', 'x']], build_expected(15, 5)],
                    ['tester3', [['name', 'fabi'], ['class', 'top'], ['generation', 'x']], build_expected(25, 5)],
                    ]
            assert expected_result == actual_result
            assert expected_result[1:] == r.execute_command('TS.mrange', start_ts, start_ts + samples_count,
                                                            'AGGREGATION', 'LAST', 5, 'FILTER', 'generation=x', 'class!=middle')
            actual_result = r.execute_command('TS.mrange', start_ts, start_ts + samples_count, 'COUNT', 3, 'AGGREGATION', 'LAST', 5, 'FILTER', 'generation=x')
            assert expected_result[0][2][:3] == actual_result[0][2]
            actual_result = r.execute_command('TS.mrange', start_ts + 1, start_ts + samples_count, 'AGGREGATION', 'COUNT', 5, 'FILTER', 'generation=x')
            assert expected_result[0][2][1:9] == actual_result[0][2][:8]
            actual_result = r.execute_command('TS.mrange', start_ts, start_ts + samples_count, 'AGGREGATION', 'COUNT', 3, 'COUNT', 3, 'FILTER', 'generation=x')
            assert 3 == len(actual_result[0][2]) #just checking that agg count before count works
            actual_result = r.execute_command('TS.mrange', start_ts, start_ts + samples_count, 'COUNT', 3, 'AGGREGATION', 'COUNT', 3, 'FILTER', 'generation=x')
            assert 3 == len(actual_result[0][2]) #just checking that agg count before count works
            actual_result = r.execute_command('TS.mrange', start_ts, start_ts + samples_count, 'AGGREGATION', 'COUNT', 3, 'FILTER', 'generation=x')
            assert 18 == len(actual_result[0][2]) #just checking that agg count before count works

    def test_range_count(self):
        start_ts = 1511885908L
        samples_count = 50

        with self.redis() as r:
            r.execute_command('TS.CREATE', 'tester1')
            for i in range(samples_count):
                r.execute_command('TS.ADD', 'tester1', start_ts + i, i)
            full_results = r.execute_command('TS.RANGE', 'tester1', 0, -1)
            assert len(full_results) == samples_count
            count_results = r.execute_command('TS.RANGE', 'tester1', 0, -1, 'COUNT', 10)
            assert count_results == full_results[:10]
            count_results = r.execute_command('TS.RANGE', 'tester1', 0, -1, 'COUNT', 10, 'AGGREGATION', 'COUNT', 3)
            assert len(count_results) == 10
            count_results = r.execute_command('TS.RANGE', 'tester1', 0, -1, 'AGGREGATION', 'COUNT', 3, 'COUNT', 10)
            assert len(count_results) == 10
            count_results = r.execute_command('TS.RANGE', 'tester1', 0, -1, 'AGGREGATION', 'COUNT', 3)
            assert len(count_results) ==  math.ceil(samples_count / 3.0)

    def test_label_index(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester1', 'LABELS', 'name', 'bob', 'class', 'middle', 'generation', 'x')
            assert r.execute_command('TS.CREATE', 'tester2', 'LABELS', 'name', 'rudy', 'class', 'junior', 'generation', 'x')
            assert r.execute_command('TS.CREATE', 'tester3', 'LABELS', 'name', 'fabi', 'class', 'top', 'generation', 'x', 'x', '2')
            assert r.execute_command('TS.CREATE', 'tester4', 'LABELS', 'name', 'anybody', 'class', 'top', 'type', 'noone', 'x', '2', 'z', '3')

            assert ['tester1', 'tester2', 'tester3'] == r.execute_command('TS.QUERYINDEX', 'generation=x')
            assert ['tester1', 'tester2'] == r.execute_command('TS.QUERYINDEX', 'generation=x', 'x=')
            assert ['tester3'] == r.execute_command('TS.QUERYINDEX', 'generation=x', 'x=2')
            assert ['tester3', 'tester4'] == r.execute_command('TS.QUERYINDEX', 'x=2')
            assert ['tester1', 'tester2'] == r.execute_command('TS.QUERYINDEX', 'generation=x', 'class!=top')
            assert ['tester2'] == r.execute_command('TS.QUERYINDEX', 'generation=x', 'class!=middle', 'x=')
            assert [] == r.execute_command('TS.QUERYINDEX', 'generation=x', 'class=top', 'x=')
            assert ['tester3'] == r.execute_command('TS.QUERYINDEX', 'generation=x', 'class=top', 'z=')
            with pytest.raises(redis.ResponseError):
                r.execute_command('TS.QUERYINDEX', 'z=', 'x!=2')
            assert ['tester3'] == r.execute_command('TS.QUERYINDEX', 'z=', 'x=2')

            # Test filter list
            assert ['tester1', 'tester2'] == r.execute_command('TS.QUERYINDEX', 'generation=x', 'class=(middle,junior)')
            assert [] == r.execute_command('TS.QUERYINDEX', 'generation=x', 'class=(a,b,c)')
            assert r.execute_command('TS.QUERYINDEX', 'generation=x') == r.execute_command('TS.QUERYINDEX', 'generation=(x)')
            assert r.execute_command('TS.QUERYINDEX', 'generation=x', 'class=()') == []
            assert r.execute_command('TS.QUERYINDEX', 'class=(middle,junior,top)', 'name!=(bob,rudy,fabi)') == ['tester4']
            with pytest.raises(redis.ResponseError):
                assert r.execute_command('TS.QUERYINDEX', 'generation=x', 'class=(')
            with pytest.raises(redis.ResponseError):
                assert r.execute_command('TS.QUERYINDEX', 'generation=x', 'class=(ab')
            with pytest.raises(redis.ResponseError):
                assert r.execute_command('TS.QUERYINDEX', 'generation!=(x,y)')

    def test_series_ordering(self):
        with self.redis() as r:
            sample_len = 1024
            chunk_size = 4

            r.execute_command("ts.create", 'test_key', 0, chunk_size)
            for i in range(sample_len):
                r.execute_command("ts.add", 'test_key', i , i)

            res = r.execute_command('ts.range', 'test_key', 0, sample_len)
            i = 0
            for sample in res:
                assert sample == [i, str(i)]
                i += 1

    def test_madd(self):
        sample_len = 1024

        with self.redis() as r:
            r.execute_command("ts.create", 'test_key1')
            r.execute_command("ts.create", 'test_key2')
            r.execute_command("ts.create", 'test_key3')

            for i in range(sample_len):
                assert [i+1000L, i+3000L, i+6000L] == r.execute_command("ts.madd", 'test_key1', i+1000, i, 'test_key2', i+3000, i, 'test_key3', i+6000, i,)

            res = r.execute_command('ts.range', 'test_key1', 1000, 1000+sample_len)
            i = 0
            for sample in res:
                assert sample == [1000+i, str(i)]
                i += 1

            res = r.execute_command('ts.range', 'test_key2', 3000, 3000+sample_len)
            i = 0
            for sample in res:
                assert sample == [3000+i, str(i)]
                i += 1

            res = r.execute_command('ts.range', 'test_key3', 6000, 6000+sample_len)
            i = 0
            for sample in res:
                assert sample == [6000+i, str(i)]
                i += 1

    def test_partial_madd(self):
        with self.redis() as r:
            r.execute_command("ts.create", 'test_key1')
            r.execute_command("ts.create", 'test_key2')
            r.execute_command("ts.create", 'test_key3')

            now = int(time.time()*1000)
            res = r.execute_command("ts.madd", 'test_key1', "*", 10, 'test_key2', 2000, 20, 'test_key3', 3000, 30)
            assert now <= res[0] and now+2 >= res[0]
            assert 2000 == res[1]
            assert 3000 == res[2]

            res = r.execute_command("ts.madd", 'test_key1', now + 1000, 10, 'test_key2', 1000, 20, 'test_key3', 3001 , 30)
            assert (now + 1000, 3001) == (res[0], res[2])
            assert isinstance(res[1], redis.ResponseError)
            assert len(r.execute_command('ts.range', 'test_key1', "-", "+")) == 2
            assert len(r.execute_command('ts.range', 'test_key2', "-", "+")) == 1
            assert len(r.execute_command('ts.range', 'test_key3', "-", "+")) == 2
    
    def test_rule_timebucket_64bit(self):
        with self.redis() as r:
            BELOW_32BIT_LIMIT = 2147483647
            ABOVE_32BIT_LIMIT = 2147483648
            r.execute_command("ts.create", 'test_key', 'RETENTION', ABOVE_32BIT_LIMIT)
            r.execute_command("ts.create", 'below_32bit_limit')
            r.execute_command("ts.create", 'above_32bit_limit')
            r.execute_command("ts.createrule", 'test_key', 'below_32bit_limit', 'AGGREGATION', 'max', BELOW_32BIT_LIMIT)
            r.execute_command("ts.createrule", 'test_key', 'above_32bit_limit', 'AGGREGATION', 'max', ABOVE_32BIT_LIMIT)
            info = r.execute_command("ts.info", 'test_key')
            assert info[13][0][1] == BELOW_32BIT_LIMIT            
            assert info[13][1][1] == ABOVE_32BIT_LIMIT

########## Test init args ##########
class RedisTimeseriesInitTestRetSuccess(ModuleTestCase(REDISTIMESERIES, module_args=['RETENTION_POLICY', '100'])):
    def test_args(self):
        assertInitArgsSuccess(self)

class RedisTimeseriesInitTestRetFailStr(ModuleTestCase(REDISTIMESERIES, module_args=['RETENTION_POLICY', 'RTS'])):
    def test_args(self):
        assertInitArgsFail(self)

class RedisTimeseriesInitTestRetFailNeg(ModuleTestCase(REDISTIMESERIES, module_args=['RETENTION_POLICY', -1])):
    def test_args(self):
        assertInitArgsFail(self)

class RedisTimeseriesInitTestMaxSamplesSuccess(ModuleTestCase(REDISTIMESERIES, module_args=['MAX_SAMPLE_PER_CHUNK', '100'])):
    def test_args(self):
        assertInitArgsSuccess(self)

class RedisTimeseriesInitTestMaxSamplesFailStr(ModuleTestCase(REDISTIMESERIES, module_args=['MAX_SAMPLE_PER_CHUNK', 'RTS'])):
    def test_args(self):
        assertInitArgsFail(self)

class RedisTimeseriesInitTestMaxSamplesFailNeg(ModuleTestCase(REDISTIMESERIES, module_args=['MAX_SAMPLE_PER_CHUNK', -1])):
    def test_args(self):
        assertInitArgsFail(self)

class RedisTimeseriesInitTestPolicySuccess(ModuleTestCase(REDISTIMESERIES, module_args=['COMPACTION_POLICY', 'max:1m:1d;min:10s:1h;avg:2h:10d;avg:3d:100d'])):
    def test_args(self):
        assertInitArgsSuccess(self)

class RedisTimeseriesInitTestPolicyFail(ModuleTestCase(REDISTIMESERIES, module_args=['COMPACTION_POLICY', 'RTS'])):
    def test_args(self):
        assertInitArgsFail(self)