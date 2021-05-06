import pytest
import redis
from RLTest import Env
from test_helper_classes import _get_ts_info


def test_delete_rule(self):
    key_name = 'tester{abc}'
    with Env().getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', key_name)
        assert r.execute_command('TS.CREATE', '{}_agg_max_10'.format(key_name))
        assert r.execute_command('TS.CREATE', '{}_agg_min_20'.format(key_name))
        assert r.execute_command('TS.CREATE', '{}_agg_avg_30'.format(key_name))
        assert r.execute_command('TS.CREATE', '{}_agg_last_40'.format(key_name))
        assert r.execute_command('TS.CREATERULE', key_name, '{}_agg_max_10'.format(key_name), 'AGGREGATION', 'MAX', 10)
        assert r.execute_command('TS.CREATERULE', key_name, '{}_agg_min_20'.format(key_name), 'AGGREGATION', 'MIN', 20)
        assert r.execute_command('TS.CREATERULE', key_name, '{}_agg_avg_30'.format(key_name), 'AGGREGATION', 'AVG', 30)
        assert r.execute_command('TS.CREATERULE', key_name, '{}_agg_last_40'.format(key_name), 'AGGREGATION', 'LAST', 40)

        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.DELETERULE', key_name, 'non_existent')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.DELETERULE', 'non_existent', key_name)

        assert len(_get_ts_info(r, key_name).rules) == 4
        assert r.execute_command('TS.DELETERULE', key_name, '{}_agg_avg_30'.format(key_name))
        assert len(_get_ts_info(r, key_name).rules) == 3
        assert r.execute_command('TS.DELETERULE', key_name, '{}_agg_max_10'.format(key_name))
        assert len(_get_ts_info(r, key_name).rules) == 2
