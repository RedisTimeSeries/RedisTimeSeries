import pytest
import redis
from RLTest import Env
from test_helper_classes import _get_ts_info


def test_delete_rule(self):
    with Env().getConnection() as r:
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
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.DELETERULE', 'non_existent', 'tester')

        assert len(_get_ts_info(r, 'tester').rules) == 4
        assert r.execute_command('TS.DELETERULE', 'tester', 'tester_agg_avg_30')
        assert len(_get_ts_info(r, 'tester').rules) == 3
        assert r.execute_command('TS.DELETERULE', 'tester', 'tester_agg_max_10')
        assert len(_get_ts_info(r, 'tester').rules) == 2
