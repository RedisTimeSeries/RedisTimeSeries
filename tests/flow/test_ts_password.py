# import redis
# from utils import Env
from includes import *
from test_helper_classes import _get_series_value, calc_rule, ALLOWED_ERROR, \
    _insert_data, _get_ts_info, _insert_agg_data
# import pytest

def test_password():
    env = Env(moduleArgs='ENCODING UNCOMPRESSED; OSS_GLOBAL_PASSWORD password', freshEnv=True)
    if not env.is_cluster():
        env.skip()
    skip_on_rlec()
    if env.envRunner.password is None:
        env.skip()
    start_ts = 1
    samples_count = 10
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        assert r.execute_command('TS.CREATE', 'tester1{1}', 'LABELS', 'name', 'bob')
        _insert_data(r, 'tester1{1}', start_ts, samples_count, 1)
        res = r1.execute_command('TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'name=bob')
        assert res != []

def test_invalid_password():
    env = Env(moduleArgs='ENCODING UNCOMPRESSED; OSS_GLOBAL_PASSWORD wrong_password', freshEnv=True)
    if not env.is_cluster():
        env.skip()
    skip_on_rlec()
    if env.envRunner.password is None:
        env.skip()
    start_ts = 1
    samples_count = 10
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        assert r.execute_command('TS.CREATE', 'tester1{1}', 'LABELS', 'name', 'bob')
        _insert_data(r, 'tester1{1}', start_ts, samples_count, 1)
        with pytest.raises(redis.ResponseError) as excinfo:
            r1.execute_command('TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'name=bob')

def test_no_password():
    env = Env(moduleArgs='ENCODING UNCOMPRESSED', freshEnv=True)
    if not env.is_cluster():
        env.skip()
    skip_on_rlec()
    if env.envRunner.password is None:
        env.skip()
    start_ts = 1
    samples_count = 10
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        assert r.execute_command('TS.CREATE', 'tester1{1}', 'LABELS', 'name', 'bob')
        _insert_data(r, 'tester1{1}', start_ts, samples_count, 1)
        with pytest.raises(redis.ResponseError) as excinfo:
            r1.execute_command('TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'name=bob')
