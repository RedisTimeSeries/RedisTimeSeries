import redis
import pytest
from rmtest import ModuleTestCase

class MyTestCase(ModuleTestCase('redis-tsdb-module.so')):
    def _get_ts_info(self, redis, key):
        info = redis.execute_command('TS.INFO', key)
        return dict([(info[i], info[i+1]) for i in range(0, len(info), 2)])

    def _insert_data(self, redis, key, start_ts, samples_count, value):
        for i in range(samples_count):
            assert redis.execute_command('TS.ADD', key, start_ts + i, 5)

    def test_sanity(self):
        start_ts = 1511885909L
        samples_count = 500
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            self._insert_data(r, 'tester', start_ts, samples_count, 5)

            expected_result = [[start_ts+i, str(5)] for i in range(samples_count)]
            actual_result = r.execute_command('TS.range', 'tester', start_ts, start_ts + samples_count)
            assert expected_result == actual_result

    def test_rdb(self):
        start_ts = 1511885909L
        samples_count = 500
        data = None
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            self._insert_data(r, 'tester', start_ts, samples_count, 5)
            data = r.execute_command('dump', 'tester')

        with self.redis() as r:
            r.execute_command('RESTORE', 'tester', 0, data)
            expected_result = [[start_ts+i, str(5)] for i in range(samples_count)]
            actual_result = r.execute_command('TS.range', 'tester', start_ts, start_ts + samples_count)
            assert expected_result == actual_result

    def test_sanity_pipeline(self):
        start_ts = 1488823384L
        samples_count = 500
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            with r.pipeline(transaction=False) as p:
                p.set("name", "danni")
                self._insert_data(p, 'tester', start_ts, samples_count, 5)
                p.execute()
            expected_result = [[start_ts+i, str(5)] for i in range(samples_count)]
            actual_result = r.execute_command('TS.range', 'tester', start_ts, start_ts + samples_count)
            assert expected_result == actual_result

    def test_range_query(self):
        start_ts = 1488823384L
        samples_count = 500
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            self._insert_data(r, 'tester', start_ts, samples_count, 5)

            expected_result = [[start_ts+i, str(5)] for i in range(100, 151)]
            actual_result = r.execute_command('TS.range', 'tester', start_ts+100, start_ts + 150)
            assert expected_result == actual_result

    def test_range_with_agg_query(self):
        start_ts = 1488823384L
        samples_count = 500
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            self._insert_data(r, 'tester', start_ts, samples_count, 5)

            expected_result = [[1488823000L, '116'], [1488823500L, '384']]
            actual_result = r.execute_command('TS.range', 'tester', start_ts, start_ts + 500, 'count', 500)
            assert expected_result == actual_result

    def test_compaction_rules(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            assert r.execute_command('TS.CREATE', 'tester_agg_max_10')
            assert r.execute_command('TS.CREATERULE', 'tester', 'avg', 10, 'tester_agg_max_10')

            start_ts = 1488823384L
            samples_count = 500
            self._insert_data(r, 'tester', start_ts, samples_count, 5)

            actual_result = r.execute_command('TS.RANGE', 'tester_agg_max_10', start_ts, start_ts + samples_count)

            assert len(actual_result) == samples_count/10

            info_dict = self._get_ts_info(r, 'tester')
            assert info_dict == {'chunkCount': 2L, 'lastTimestamp': start_ts + samples_count -1, 'maxSamplesPerChunk': 360L, 'retentionSecs': 0L, 'rules': [['tester_agg_max_10', 10L, 'AVG']]}
    
    def test_create_compaction_rule_without_dest_series(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            with pytest.raises(redis.ResponseError) as excinfo:
                assert r.execute_command('TS.CREATERULE', 'tester', 'MAX', 10, 'tester_agg_max_10')

    def test_create_compaction_rule_twice(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            assert r.execute_command('TS.CREATE', 'tester_agg_max_10')
            assert r.execute_command('TS.CREATERULE', 'tester', 'MAX', 10, 'tester_agg_max_10')
            with pytest.raises(redis.ResponseError) as excinfo:
                assert r.execute_command('TS.CREATERULE', 'tester', 'MAX', 10, 'tester_agg_max_10')

    def test_create_compaction_rule_and_del_dest_series(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            assert r.execute_command('TS.CREATE', 'tester_agg_max_10')
            assert r.execute_command('TS.CREATERULE', 'tester', 'AVG', 10, 'tester_agg_max_10')
            assert r.delete('tester_agg_max_10')

            start_ts = 1488823384L
            samples_count = 500
            self._insert_data(r, 'tester', start_ts, samples_count, 5)

    def test_delete_rule(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            assert r.execute_command('TS.CREATE', 'tester_agg_max_10')
            assert r.execute_command('TS.CREATERULE', 'tester', 'AVG', 10, 'tester_agg_max_10')

            with pytest.raises(redis.ResponseError) as excinfo:
                assert r.execute_command('TS.DELETERULE', 'tester', 'non_existent')

            assert len(self._get_ts_info(r, 'tester')['rules']) == 1
            assert r.execute_command('TS.DELETERULE', 'tester', 'tester_agg_max_10')
            assert len(self._get_ts_info(r, 'tester')['rules']) == 0

    def test_empty_series(self):
        with self.redis() as r:
            assert r.execute_command('TS.CREATE', 'tester')
            assert r.execute_command('DUMP', 'tester')
