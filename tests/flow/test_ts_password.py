from RLTest import Env
from includes import *
from test_helper_classes import _get_series_value, calc_rule, ALLOWED_ERROR, _insert_data, \
    _get_ts_info, _insert_agg_data

def test_password():
    env = Env(moduleArgs='ENCODING UNCOMPRESSED; OSS_GLOBAL_PASSWORD password')
    if not env.isCluster:
        env.skip()
    start_ts = 1
    samples_count = 10
    with env.getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 'tester1{1}', 'LABELS', 'name', 'bob')
        _insert_data(r, 'tester1{1}', start_ts, samples_count, 1)
        res = env.getConnection(1).execute_command('TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'name=bob')
        print(res)
