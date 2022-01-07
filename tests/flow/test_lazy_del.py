import pytest
import redis
from RLTest import Env
from test_helper_classes import TSInfo, ALLOWED_ERROR, _insert_data, _get_ts_info, \
    _insert_agg_data
from includes import *

def test_lazy_del_src():
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 'src{test}')
        r.execute_command("ts.create", 'dst{test}')
        r.execute_command("ts.createrule", 'src{test}', 'dst{test}', 'AGGREGATION', 'avg', 60000)
        
        assert _get_ts_info(r, 'dst{test}').sourceKey == 'src{test}'
        assert len(_get_ts_info(r, 'src{test}').rules) == 1
        assert _get_ts_info(r, 'src{test}').rules[0][0] == 'dst{test}'
        r.execute_command('DEL', 'src{test}')

        assert _get_ts_info(r, 'dst{test}').sourceKey == None
        assert len(_get_ts_info(r, 'dst{test}').rules) == 0

def test_lazy_del_dst():
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 'src{test}')
        r.execute_command("ts.create", 'dst{test}')
        r.execute_command('TS.CREATERULE', 'src{test}', 'dst{test}', 'AGGREGATION', 'avg', 60000)
        
        assert _get_ts_info(r, 'dst{test}').sourceKey == 'src{test}'
        assert len(_get_ts_info(r, 'src{test}').rules) == 1
        assert _get_ts_info(r, 'src{test}').rules[0][0] == 'dst{test}'
        r.execute_command('DEL', 'dst{test}')

        assert _get_ts_info(r, 'src{test}').sourceKey == None
        assert len(_get_ts_info(r, 'src{test}').rules) == 0

#def test_533_dump_rules() implementing test_dump_restore_src_rule
def test_dump_restore_dst_rule():
    with Env().getClusterConnectionIfNeeded() as r:
        key1 = 'ts1{a}'
        key2 = 'ts2{a}'
        r.execute_command('TS.CREATE', key1)
        r.execute_command('TS.CREATE', key2)
        r.execute_command('TS.CREATERULE', key1, key2, 'AGGREGATION', 'avg', 60000)

        assert _get_ts_info(r, key2).sourceKey == key1
        assert len(_get_ts_info(r, key1).rules) == 1

        data = r.execute_command('DUMP', key2)
        r.execute_command('DEL', key2)
        r.execute_command('restore', key2, 0, data)

        assert _get_ts_info(r, key1).sourceKey == None
        assert len(_get_ts_info(r, key1).rules) == 0
        assert _get_ts_info(r, key2).sourceKey == None
        assert len(_get_ts_info(r, key2).rules) == 0
