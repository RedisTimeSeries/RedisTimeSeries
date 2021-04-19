import pytest
import redis
from utils import Env


def test_label_index():
    with Env().getClusterConnectionIfNeeded() as r:
        assert r.execute_command('TS.CREATE', 'tester1', 'LABELS', 'name', 'bob', 'class', 'middle', 'generation', 'x')
        assert r.execute_command('TS.CREATE', 'tester2', 'LABELS', 'name', 'rudy', 'class', 'junior', 'generation', 'x')
        assert r.execute_command('TS.CREATE', 'tester3', 'LABELS', 'name', 'fabi', 'class', 'top', 'generation', 'x',
                                 'x', '2')
        assert r.execute_command('TS.CREATE', 'tester4', 'LABELS', 'name', 'anybody', 'class', 'top', 'type', 'noone',
                                 'x', '2', 'z', '3')

        def assert_data(query, expected_data):
            assert sorted(expected_data) == sorted(r.execute_command(*query))
        assert_data(['TS.QUERYINDEX', 'generation=x'], [b'tester1', b'tester2', b'tester3'])
        assert_data(['TS.QUERYINDEX', 'generation=x', 'x='], [b'tester1', b'tester2'])
        assert_data(['TS.QUERYINDEX', 'generation=x', 'x=2'], [b'tester3'])
        assert_data(['TS.QUERYINDEX', 'x=2'], [b'tester3', b'tester4'])
        assert_data(['TS.QUERYINDEX', 'generation=x', 'class!=middle', 'x='], [b'tester2'])
        assert_data(['TS.QUERYINDEX', 'generation=x', 'class=top', 'x='], [])
        assert_data(['TS.QUERYINDEX', 'generation=x', 'class=top', 'z='], [b'tester3'])
        assert_data(['TS.QUERYINDEX',  'z=', 'x=2'], [b'tester3'])

        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.QUERYINDEX', 'z=', 'x!=2')

        # Test filter list
        assert_data(['TS.QUERYINDEX', 'generation=x', 'class=(middle,junior)'], [b'tester1', b'tester2'])
        assert_data(['TS.QUERYINDEX', 'generation=x', 'class=(a,b,c)'], [])
        assert sorted(r.execute_command('TS.QUERYINDEX', 'generation=x')) == sorted(r.execute_command('TS.QUERYINDEX',
                                                                                       'generation=(x)'))
        assert_data(['TS.QUERYINDEX', 'generation=x', 'class=()'], [])
        assert_data(['TS.QUERYINDEX', 'class=(middle,junior,top)', 'name!=(bob,rudy,fabi)'], [b'tester4'])
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.QUERYINDEX', 'generation=x', 'class=(')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.QUERYINDEX', 'generation=x', 'class=(ab')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.QUERYINDEX', 'generation!=(x,y)')
