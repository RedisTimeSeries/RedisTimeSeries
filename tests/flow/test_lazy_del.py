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
        r.execute_command('TS.CREATE', key1)
        r.execute_command('TS.CREATE', key2)
        r.execute_command('TS.CREATERULE', key1, key2, 'AGGREGATION', 'avg', 60000)

        assert _get_ts_info(r, key2).sourceKey.decode() == key1
        assert len(_get_ts_info(r, key1).rules) == 1

        data = r.execute_command('DUMP', key2)
        r.execute_command('DEL', key2)
        r.execute_command('restore', key2, 0, data)

        assert _get_ts_info(r, key1).sourceKey == None
        assert len(_get_ts_info(r, key1).rules) == 0
        assert _get_ts_info(r, key2).sourceKey == None
        assert len(_get_ts_info(r, key2).rules) == 0

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

def test_dump_restore_dst_rule_old_version():
    with Env().getClusterConnectionIfNeeded() as r:
        key1 = 'ts1{a}'
        key2 = 'ts2{a}'
        r.execute_command('TS.CREATE', key1)

        # data was taken from test_dump_restore_dst_rule on version TS_ALIGNMENT_TS_VER
        data = b'\x07\x81M \xc1\xf96\x0f\x10\x05\x05\x06ts2{a}\x02\x00\x02P\x00\x02\x02\x02\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x02\x00\x02\x01\x05\x06ts1{a}\x02\x00\x02\x00\x02\x01\x02P\x00\x02\x00\x02\x00\x02\x00\x02\x00\x02\x00\x02\x00\x02\x00\x02 \x02 \x05\xc36P\x00\x01\x00\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0{\x00\x01\x00\x00\x00\t\x00\x1e\x80\x04\xe3\xf0.\xfc$'
        r.execute_command('restore', key2, 0, data)

        assert _get_ts_info(r, key1).sourceKey == None
        assert len(_get_ts_info(r, key1).rules) == 0
        assert _get_ts_info(r, key2).sourceKey == None
        assert len(_get_ts_info(r, key2).rules) == 0

        r.execute_command('DEL', key1)

        # data was taken from test_dump_restore_dst_rule on version TS_ALIGNMENT_TS_VER
        data = b'\x07\x81M \xc1\xf96\x0f\x10\x05\x05\x06ts1{a}\x02\x00\x02P\x00\x02\x02\x02\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x02\x00\x02\x00\x02\x00\x02\x01\x05\x06ts2{a}\x02\x80\x00\x00\xea`\x02\x00\x02\x04\x02\x81\xff\xff\xff\xff\xff\xff\xff\xff\x04\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x02\x01\x02P\x00\x02\x00\x02\x00\x02\x00\x02\x00\x02\x00\x02\x00\x02\x00\x02 \x02 \x05\xc36P\x00\x01\x00\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0{\x00\x01\x00\x00\x00\t\x00\xf9\x7f\x9d\xeeN\xe5\xb3/'
        r.execute_command('restore', key1, 0, data)

        assert _get_ts_info(r, key1).sourceKey == None
        assert len(_get_ts_info(r, key1).rules) == 0
        assert _get_ts_info(r, key2).sourceKey == None
        assert len(_get_ts_info(r, key2).rules) == 0