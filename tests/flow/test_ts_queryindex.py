import pytest
import redis
from utils import Env
from includes import *


def test_label_index(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'tester1', 'LABELS', 'name', 'bob', 'class', 'middle', 'generation', 'x', conn=r).noError()
        env.expect('TS.CREATE', 'tester2', 'LABELS', 'name', 'rudy', 'class', 'junior', 'generation', 'x', conn=r).noError()
        assert r.execute_command('TS.CREATE', 'tester3', 'LABELS', 'name', 'fabi', 'class', 'top', 'generation', 'x',
                                 'x', '2')
        assert r.execute_command('TS.CREATE', 'tester4', 'LABELS', 'name', 'anybody', 'class', 'top', 'type', 'noone',
                                 'x', '2', 'z', '3')

        def assert_data(query, expected_data):
            assert sorted(expected_data) == sorted(r.execute_command(*query))
        assert_data(['TS.QUERYINDEX', 'generation=x'], ['tester1', 'tester2', 'tester3'])
        assert_data(['TS.QUERYINDEX', 'generation=x', 'x='], ['tester1', 'tester2'])
        assert_data(['TS.QUERYINDEX', 'generation=x', 'x=2'], ['tester3'])
        assert_data(['TS.QUERYINDEX', 'x=2'], ['tester3', 'tester4'])
        assert_data(['TS.QUERYINDEX', 'generation=x', 'class!=middle', 'x='], ['tester2'])
        assert_data(['TS.QUERYINDEX', 'generation=x', 'class=top', 'x='], [])
        assert_data(['TS.QUERYINDEX', 'generation=x', 'class=top', 'z='], ['tester3'])
        assert_data(['TS.QUERYINDEX',  'z=', 'x=2'], ['tester3'])

        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.QUERYINDEX', 'z=', 'x!=2')

        # Test filter list
        assert_data(['TS.QUERYINDEX', 'generation=x', 'class=(middle,junior)'], ['tester1', 'tester2'])
        assert_data(['TS.QUERYINDEX', 'generation=x', 'class=(a,b,c)'], [])
        assert sorted(r.execute_command('TS.QUERYINDEX', 'generation=x')) == sorted(r.execute_command('TS.QUERYINDEX',
                                                                                       'generation=(x)'))
        assert_data(['TS.QUERYINDEX', 'generation=x', 'class=()'], [])
        assert_data(['TS.QUERYINDEX', 'class=(middle,junior,top)', 'name!=(bob,rudy,fabi)'], ['tester4'])
        env.expect('TS.QUERYINDEX', 'generation=x', 'class=(', conn=r).error()
        env.expect('TS.QUERYINDEX', 'generation=x', 'class=(ab', conn=r).error()
        env.expect('TS.QUERYINDEX', 'generation!=(x,y)', conn=r).error()

def test_large_key_value_pairs(env):
     with env.getClusterConnectionIfNeeded() as r:
        number_series = 100
        for i in range(0,number_series):
            env.expect('TS.CREATE', 'ts-{}'.format(i), 'LABELS', 'baseAsset', '17049', 'counterAsset', '840', 'source', '1000', 'dataType', 'PRICE_TICK', conn=r).noError()

        kv_label1 = 'baseAsset=(13830,10249,16019,10135,17049,10777,10138,11036,11292,15778,11043,10025,11436,12207,13359,10807,12216,11833,10170,10811,12864,12738,10053,11334,12487,12619,12364,13266,11219,15827,12374,11223,10071,12249,11097,14430,13282,16226,13667,11365,12261,12646,12650,12397,12785,13941,10231,16254,12159,15103)'
        kv_label2 = 'counterAsset=(840)'
        kv_label3 = 'source=(1000)'
        kv_label4 = 'dataType=(PRICE_TICK)'
        kv_labels = [kv_label1, kv_label2, kv_label3, kv_label4]
        for kv_label in kv_labels:
            res = r.execute_command('TS.QUERYINDEX', kv_label1)
            assert len(res) == number_series
