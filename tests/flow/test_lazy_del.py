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
        
        assert _get_ts_info(r, 'dst{test}').sourceKey.decode() == 'src{test}'
        assert len(_get_ts_info(r, 'src{test}').rules) == 1
        assert _get_ts_info(r, 'src{test}').rules[0][0].decode() == 'dst{test}'
        r.execute_command('DEL', 'src{test}')

        assert _get_ts_info(r, 'dst{test}').sourceKey == None
        assert len(_get_ts_info(r, 'dst{test}').rules) == 0

def test_lazy_del_dst():
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 'src{test}')
        r.execute_command("ts.create", 'dst{test}')
        r.execute_command('TS.CREATERULE', 'src{test}', 'dst{test}', 'AGGREGATION', 'avg', 60000)
        
        assert _get_ts_info(r, 'dst{test}').sourceKey.decode() == 'src{test}'
        assert len(_get_ts_info(r, 'src{test}').rules) == 1
        assert _get_ts_info(r, 'src{test}').rules[0][0].decode() == 'dst{test}'
        r.execute_command('DEL', 'dst{test}')

        assert _get_ts_info(r, 'src{test}').sourceKey == None
        assert len(_get_ts_info(r, 'src{test}').rules) == 0

#def test_533_dump_rules() implementing test_dump_restore_src_rule
def test_dump_restore_dst_rule():
    with Env().getClusterConnectionIfNeeded() as r:
        key1 = 'ts1{a}'
        key2 = 'ts2{a}'
        key3 = 'ts3{a}'
        key4 = 'ts4{a}'
        key5 = 'ts5{a}'
        key6 = 'ts6{a}'
        n_dst_keys = 5
        r.execute_command('TS.CREATE', key1)
        r.execute_command('TS.CREATE', key2)
        r.execute_command('TS.CREATE', key3)
        r.execute_command('TS.CREATE', key4)
        r.execute_command('TS.CREATE', key5)
        r.execute_command('TS.CREATE', key6)
        r.execute_command('TS.CREATERULE', key1, key2, 'AGGREGATION', 'avg', 60000)
        r.execute_command('TS.CREATERULE', key1, key3, 'AGGREGATION', 'twa', 10)
        r.execute_command('TS.CREATERULE', key1, key4, 'AGGREGATION', 'last', 10)
        r.execute_command('TS.CREATERULE', key1, key5, 'AGGREGATION', 'count', 10)
        r.execute_command('TS.CREATERULE', key1, key6, 'AGGREGATION', 'first', 10)

        assert _get_ts_info(r, key2).sourceKey.decode() == key1
        assert len(_get_ts_info(r, key1).rules) == n_dst_keys

        data = r.execute_command('DUMP', key2)
        r.execute_command('DEL', key2)
        r.execute_command('restore', key2, 0, data)

        assert _get_ts_info(r, key1).sourceKey == None
        assert len(_get_ts_info(r, key1).rules) == n_dst_keys - 1
        assert _get_ts_info(r, key2).sourceKey == None
        assert len(_get_ts_info(r, key2).rules) == 0
    
        data = r.execute_command('DUMP', key3)
        r.execute_command('DEL', key3)
        r.execute_command('restore', key3, 0, data)
        assert _get_ts_info(r, key1).sourceKey == None
        assert len(_get_ts_info(r, key1).rules) == n_dst_keys - 2
        assert _get_ts_info(r, key3).sourceKey == None
        assert len(_get_ts_info(r, key3).rules) == 0

        data = r.execute_command('DUMP', key4)
        r.execute_command('DEL', key4)
        r.execute_command('restore', key4, 0, data)
        assert _get_ts_info(r, key1).sourceKey == None
        assert len(_get_ts_info(r, key1).rules) ==  n_dst_keys - 3
        assert _get_ts_info(r, key4).sourceKey == None
        assert len(_get_ts_info(r, key4).rules) == 0

        data = r.execute_command('DUMP', key5)
        r.execute_command('DEL', key5)
        r.execute_command('restore', key5, 0, data)
        assert _get_ts_info(r, key1).sourceKey == None
        assert len(_get_ts_info(r, key1).rules) ==  n_dst_keys - 4
        assert _get_ts_info(r, key5).sourceKey == None
        assert len(_get_ts_info(r, key5).rules) == 0

        data = r.execute_command('DUMP', key6)
        r.execute_command('DEL', key6)
        r.execute_command('restore', key6, 0, data)
        assert _get_ts_info(r, key1).sourceKey == None
        assert len(_get_ts_info(r, key1).rules) ==  n_dst_keys - 5
        assert _get_ts_info(r, key6).sourceKey == None
        assert len(_get_ts_info(r, key6).rules) == 0

        r.execute_command('TS.CREATERULE', key1, key2, 'AGGREGATION', 'avg', 60000)
        assert _get_ts_info(r, key2).sourceKey.decode() == key1
        assert len(_get_ts_info(r, key1).rules) == 1

        data = r.execute_command('DUMP', key1)
        r.execute_command('DEL', key1)
        r.execute_command('restore', key1, 0, data)

        assert _get_ts_info(r, key1).sourceKey == None
        assert len(_get_ts_info(r, key1).rules) == 0
        assert _get_ts_info(r, key2).sourceKey == None
        assert len(_get_ts_info(r, key2).rules) == 0

# test for version problems in dump restore
def test_dump_restore_dst_rule_force_save_refs():
    Env().skipOnCluster()
    skip_on_rlec()
    with Env(moduleArgs='DEUBG_FORCE_RULE_DUMP enable').getClusterConnectionIfNeeded() as r:
        key1 = 'ts1{a}'
        key2 = 'ts2{a}'
        key3 = 'ts3{a}'
        r.execute_command('TS.CREATE', key1)
        r.execute_command('TS.CREATE', key2)
        r.execute_command('TS.CREATE', key3)
        r.execute_command('TS.CREATERULE', key1, key2, 'AGGREGATION', 'avg', 60000)
        r.execute_command('TS.CREATERULE', key1, key3, 'AGGREGATION', 'twa', 10)

        assert _get_ts_info(r, key2).sourceKey.decode() == key1
        assert len(_get_ts_info(r, key1).rules) == 2

        data = r.execute_command('DUMP', key2)
        r.execute_command('DEL', key2)
        r.execute_command('restore', key2, 0, data)

        assert _get_ts_info(r, key1).sourceKey == None
        assert len(_get_ts_info(r, key1).rules) != 0
        assert _get_ts_info(r, key2).sourceKey != None
        assert len(_get_ts_info(r, key2).rules) == 0

def test_dump_restore_dst_rule_old_version():
    with Env().getClusterConnectionIfNeeded() as r:
        key1 = 'ts1{a}'
        key2 = 'ts2{a}'
        r.execute_command('TS.CREATE', key1)

        # data was taken from test_dump_restore_dst_rule on version TS_OVERFLOW_RDB_VER
        data = b'\a\x81M \xc1\xf96\x0f\x10\x04\x05\x06ts2{a}\x02\x00\x02P\x00\x02\x02\x02\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x02\x00\x02\x01\x05\x06ts1{a}\x02\x00\x02\x00\x02\x01\x02P\x00\x02\x00\x02\x00\x02\x00\x02\x00\x02\x00\x02\x00\x02\x00\x02 \x02 \x05\xc36P\x00\x01\x00\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0{\x00\x01\x00\x00\x00\t\x00\xb7J\aG\x17\xe4\xec\xcc'
        r.execute_command('restore', key2, 0, data)

        assert _get_ts_info(r, key1).sourceKey == None
        assert len(_get_ts_info(r, key1).rules) == 0
        assert _get_ts_info(r, key2).sourceKey == None
        assert len(_get_ts_info(r, key2).rules) == 0

        r.execute_command('DEL', key1)

        # data was taken from test_dump_restore_dst_rule on version TS_OVERFLOW_RDB_VER
        data = b'\a\x81M \xc1\xf96\x0f\x10\x04\x05\x06ts1{a}\x02\x00\x02P\x00\x02\x02\x02\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x02\x00\x02\x00\x02\x00\x02\x01\x05\x06ts2{a}\x02\x80\x00\x00\xea`\x02\x04\x02\x81\xff\xff\xff\xff\xff\xff\xff\xff\x04\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x02\x01\x02P\x00\x02\x00\x02\x00\x02\x00\x02\x00\x02\x00\x02\x00\x02\x00\x02 \x02 \x05\xc36P\x00\x01\x00\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0{\x00\x01\x00\x00\x00\t\x00j\x85\xe4Z\xb6\xb9\xc38'
        r.execute_command('restore', key1, 0, data)

        assert _get_ts_info(r, key1).sourceKey == None
        assert len(_get_ts_info(r, key1).rules) == 0
        assert _get_ts_info(r, key2).sourceKey == None
        assert len(_get_ts_info(r, key2).rules) == 0