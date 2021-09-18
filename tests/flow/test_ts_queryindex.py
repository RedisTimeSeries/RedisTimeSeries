# import pytest
import redis
from utils import Env
from includes import *


def test_label_index(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'tester1', 'LABELS', 'name', 'bob', 'class', 'middle', 'generation', 'x', conn=r).noError()
        env.expect('TS.CREATE', 'tester2', 'LABELS', 'name', 'rudy', 'class', 'junior', 'generation', 'x', conn=r).noError()
        env.expect('TS.CREATE', 'tester3', 'LABELS', 'name', 'fabi', 'class', 'top', 'generation', 'x', 'x', '2', conn=r).noError()
        env.expect('TS.CREATE', 'tester4', 'LABELS', 'name', 'anybody', 'class', 'top', 'type', 'noone', 'x', '2', 'z', '3', conn=r).noError()

        env.expect('TS.QUERYINDEX', 'generation=x', conn=r).apply(sorted).equal(['tester1', 'tester2', 'tester3'])
        env.expect('TS.QUERYINDEX', 'generation=x', 'x=', conn=r).apply(sorted).equal(['tester1', 'tester2'])
        env.expect('TS.QUERYINDEX', 'generation=x', 'x=2', conn=r).apply(sorted).equal(['tester3'])
        env.expect('TS.QUERYINDEX', 'x=2', conn=r).apply(sorted).equal(['tester3', 'tester4'])
        env.expect('TS.QUERYINDEX', 'generation=x', 'class!=middle', 'x=', conn=r).apply(sorted).equal(['tester2'])
        env.expect('TS.QUERYINDEX', 'generation=x', 'class=top', 'x=', conn=r).apply(sorted).equal([])
        env.expect('TS.QUERYINDEX', 'generation=x', 'class=top', 'z=', conn=r).apply(sorted).equal(['tester3'])
        env.expect('TS.QUERYINDEX',  'z=', 'x=2', conn=r).apply(sorted).equal(['tester3'])

        env.expect('TS.QUERYINDEX', 'z=', 'x!=2', conn=r).error()

        # Test filter list
        env.expect('TS.QUERYINDEX', 'generation=x', 'class=(middle,junior)', conn=r).apply(sorted).equal(['tester1', 'tester2'])
        env.expect('TS.QUERYINDEX', 'generation=x', 'class=(a,b,c)', conn=r).apply(sorted).equal([])
        env.assertEqual(sorted(r.execute_command('TS.QUERYINDEX', 'generation=x')),
                        sorted(r.execute_command('TS.QUERYINDEX', 'generation=(x)')))
        env.expect('TS.QUERYINDEX', 'generation=x', 'class=()', conn=r).apply(sorted).equal([])
        env.expect('TS.QUERYINDEX', 'class=(middle,junior,top)', 'name!=(bob,rudy,fabi)', conn=r).apply(sorted).equal(['tester4'])
        env.expect('TS.QUERYINDEX', 'generation=x', 'class=(', conn=r).error()
        env.expect('TS.QUERYINDEX', 'generation=x', 'class=(ab', conn=r).error()
        env.expect('TS.QUERYINDEX', 'generation!=(x,y)', conn=r).error()

def test_large_key_value_pairs(env):
     with env.getClusterConnectionIfNeeded() as r:
        number_series = 100
        for i in range(0, number_series):
            env.expect('TS.CREATE', f'ts-{i}', 'LABELS', 'baseAsset', '17049', 'counterAsset', '840',
                       'source', '1000', 'dataType', 'PRICE_TICK', conn=r).noError()

        kv_label1 = 'baseAsset=(13830,10249,16019,10135,17049,10777,10138,11036,11292,15778,11043,10025,11436,12207,13359,10807,12216,11833,10170,10811,12864,12738,10053,11334,12487,12619,12364,13266,11219,15827,12374,11223,10071,12249,11097,14430,13282,16226,13667,11365,12261,12646,12650,12397,12785,13941,10231,16254,12159,15103)'
        kv_label2 = 'counterAsset=(840)'
        kv_label3 = 'source=(1000)'
        kv_label4 = 'dataType=(PRICE_TICK)'
        kv_labels = [kv_label1, kv_label2, kv_label3, kv_label4]
        for kv_label in kv_labels:
            res = r.execute_command('TS.QUERYINDEX', kv_label1)
            env.assertEqual(len(res), number_series)
