import pytest
import redis
from itertools import chain
# from utils import Env
from includes import *


def test_filter_resolution_after_label_changes():
    env = Env()
    labels = {}
    with env.getClusterConnectionIfNeeded() as client, env.getConnection(1) as coordinator:
        for i in range(96):
            key = f'resolved:{i}'
            labels[key] = {'family': 'resolved', 'selection': 'rare' if i < 16 else 'other',
                           'colour': 'a' if i < 8 else 'b'}
            client.execute_command('TS.CREATE', key, 'LABELS', *chain.from_iterable(labels[key].items()))
            client.execute_command('TS.ADD', key, 0, i)

        def check():
            cases = [
                (['colour=(a,b)', 'optional='], lambda item: 'optional' not in item),
                (['colour!=(a,missing)'], lambda item: item.get('colour') not in ('a', 'missing')),
                (['colour=(a,a,b)'], lambda item: item.get('colour') in ('a', 'b')),
                (['optional=(yes)'], lambda item: item.get('optional') == 'yes'),
            ]
            for filters, matches in cases:
                expected = {key.encode() for key, item in labels.items()
                            if item['selection'] == 'rare' and matches(item)}
                filters = ['family=resolved', 'selection=rare', *filters]
                env.assertEqual(set(coordinator.execute_command('TS.QUERYINDEX', *filters)), expected)
                reply = coordinator.execute_command('TS.MRANGE', 0, 0, 'FILTER', *filters)
                actual = set(reply) if isinstance(reply, dict) else {row[0] for row in reply}
                env.assertEqual(actual, expected)

        check()
        # Remove an entire posting list, then recreate it in a later command.
        for i in range(8):
            key = f'resolved:{i}'
            labels[key]['colour'] = 'b'
            client.execute_command('TS.ALTER', key, 'LABELS', *chain.from_iterable(labels[key].items()))
        check()
        for i in range(4):
            key = f'resolved:{i}'
            labels[key].update(colour='a', optional='yes')
            client.execute_command('TS.ALTER', key, 'LABELS', *chain.from_iterable(labels[key].items()))
        check()
        for i in range(16):
            key = f'resolved:{i}'
            client.delete(key)
            del labels[key]
        check()


def test_label_index():
    env = Env()
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        assert r.execute_command('TS.CREATE', 'tester1', 'LABELS', 'name', 'bob', 'class', 'middle', 'generation', 'x')
        assert r.execute_command('TS.CREATE', 'tester2', 'LABELS', 'name', 'rudy', 'class', 'junior', 'generation', 'x')
        assert r.execute_command('TS.CREATE', 'tester3', 'LABELS', 'name', 'fabi', 'class', 'top', 'generation', 'x',
                                 'x', '2')
        assert r.execute_command('TS.CREATE', 'tester4', 'LABELS', 'name', 'anybody', 'class', 'top', 'type', 'noone',
                                 'x', '2', 'z', '3')

        def assert_data(query, expected_data):
            res = r1.execute_command(*query)
            assert sorted(expected_data) == sorted(res)

        assert_data(['TS.QUERYINDEX', 'generation=x'], [b'tester1', b'tester2', b'tester3'])
        assert_data(['TS.QUERYINDEX', 'generation=x', 'x='], [b'tester1', b'tester2'])
        assert_data(['TS.QUERYINDEX', 'generation=x', 'x=2'], [b'tester3'])
        assert_data(['TS.QUERYINDEX', 'x=2'], [b'tester3', b'tester4'])
        assert_data(['TS.QUERYINDEX', 'generation=x', 'class!=middle', 'x='], [b'tester2'])
        assert_data(['TS.QUERYINDEX', 'generation=x', 'class=top', 'x='], [])
        assert_data(['TS.QUERYINDEX', 'generation=x', 'class=top', 'z='], [b'tester3'])
        assert_data(['TS.QUERYINDEX',  'z=', 'x=2'], [b'tester3'])

        with pytest.raises(redis.ResponseError):
            r1.execute_command('TS.QUERYINDEX', 'z=', 'x!=2')

        # Test filter list
        assert_data(['TS.QUERYINDEX', 'generation=x', 'class=(middle,junior)'], [b'tester1', b'tester2'])
        assert_data(['TS.QUERYINDEX', 'generation=x', 'class=(a,b,c)'], [])
        assert sorted(r1.execute_command('TS.QUERYINDEX', 'generation=x')) == \
               sorted(r1.execute_command('TS.QUERYINDEX', 'generation=(x)'))
        assert_data(['TS.QUERYINDEX', 'generation=x', 'class=()'], [])
        assert_data(['TS.QUERYINDEX', 'class=(middle,junior,top)', 'name!=(bob,rudy,fabi)'], [b'tester4'])
        with pytest.raises(redis.ResponseError):
            assert r1.execute_command('TS.QUERYINDEX', 'generation=x', 'class=(')
        with pytest.raises(redis.ResponseError):
            assert r1.execute_command('TS.QUERYINDEX', 'generation=x', 'class=(ab')
        with pytest.raises(redis.ResponseError):
            assert r1.execute_command('TS.QUERYINDEX', 'generation!=(x,y)')

def test_large_key_value_pairs():
    env = Env()
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        number_series = 100
        for i in range(0,number_series):
            assert r.execute_command('TS.CREATE', 'ts-{}'.format(i), 'LABELS', 'baseAsset', '17049', 'counterAsset', '840', 'source', '1000', 'dataType', 'PRICE_TICK')

        kv_label1 = 'baseAsset=(13830,10249,16019,10135,17049,10777,10138,11036,11292,15778,11043,10025,11436,12207,13359,10807,12216,11833,10170,10811,12864,12738,10053,11334,12487,12619,12364,13266,11219,15827,12374,11223,10071,12249,11097,14430,13282,16226,13667,11365,12261,12646,12650,12397,12785,13941,10231,16254,12159,15103)'
        kv_label2 = 'counterAsset=(840)'
        kv_label3 = 'source=(1000)'
        kv_label4 = 'dataType=(PRICE_TICK)'
        kv_labels = [kv_label1, kv_label2, kv_label3, kv_label4]
        for kv_label in kv_labels:
            res = r1.execute_command('TS.QUERYINDEX', kv_label1)
            assert len(res) == number_series
