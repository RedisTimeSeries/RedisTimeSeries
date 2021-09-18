# import pytest
import redis
from RLTest import Env
from test_helper_classes import _get_ts_info
from includes import *


def test_delete_rule(env):
    key_name = 'tester{abc}'
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', key_name, conn=r).ok()
        env.expect('TS.CREATE', '{}_agg_max_10'.format(key_name), conn=r).ok()
        env.expect('TS.CREATE', '{}_agg_min_20'.format(key_name), conn=r).ok()
        env.expect('TS.CREATE', '{}_agg_avg_30'.format(key_name), conn=r).ok()
        env.expect('TS.CREATE', '{}_agg_last_40'.format(key_name), conn=r).ok()
        env.expect('TS.CREATERULE', key_name, '{}_agg_max_10'.format(key_name), 'AGGREGATION', 'MAX', 10, conn=r).ok()
        env.expect('TS.CREATERULE', key_name, '{}_agg_min_20'.format(key_name), 'AGGREGATION', 'MIN', 20, conn=r).ok()
        env.expect('TS.CREATERULE', key_name, '{}_agg_avg_30'.format(key_name), 'AGGREGATION', 'AVG', 30, conn=r).ok()
        env.expect('TS.CREATERULE', key_name, '{}_agg_last_40'.format(key_name), 'AGGREGATION', 'LAST', 40, conn=r).ok()

        env.expect('TS.DELETERULE', key_name, 'non_existent', conn=r).error()
        env.expect('TS.DELETERULE', 'non_existent', key_name, conn=r).error()

        env.assertEqual(len(_get_ts_info(r, key_name).rules), 4)
        env.expect('TS.DELETERULE', key_name, '{}_agg_avg_30'.format(key_name), conn=r).noError()
        env.assertEqual(len(_get_ts_info(r, key_name).rules), 3)
        env.expect('TS.DELETERULE', key_name, '{}_agg_max_10'.format(key_name), conn=r).noError()
        env.assertEqual(len(_get_ts_info(r, key_name).rules), 2)
