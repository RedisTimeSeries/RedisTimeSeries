import pytest
import redis
from RLTest import Env


def test_label_index():
    with Env().getConnection() as r:
        assert r.execute_command('TS.CREATE', 'tester1', 'LABELS', 'name', 'bob', 'class', 'middle', 'generation', 'x')
        assert r.execute_command('TS.CREATE', 'tester2', 'LABELS', 'name', 'rudy', 'class', 'junior', 'generation', 'x')
        assert r.execute_command('TS.CREATE', 'tester3', 'LABELS', 'name', 'fabi', 'class', 'top', 'generation', 'x',
                                 'x', '2')
        assert r.execute_command('TS.CREATE', 'tester4', 'LABELS', 'name', 'anybody', 'class', 'top', 'type', 'noone',
                                 'x', '2', 'z', '3')

        assert [b'tester1', b'tester2', b'tester3'] == r.execute_command('TS.QUERYINDEX', 'generation=x')
        assert [b'tester1', b'tester2'] == r.execute_command('TS.QUERYINDEX', 'generation=x', 'x=')
        assert [b'tester3'] == r.execute_command('TS.QUERYINDEX', 'generation=x', 'x=2')
        assert [b'tester3', b'tester4'] == r.execute_command('TS.QUERYINDEX', 'x=2')
        assert [b'tester1', b'tester2'] == r.execute_command('TS.QUERYINDEX', 'generation=x', 'class!=top')
        assert [b'tester2'] == r.execute_command('TS.QUERYINDEX', 'generation=x', 'class!=middle', 'x=')
        assert [] == r.execute_command('TS.QUERYINDEX', 'generation=x', 'class=top', 'x=')
        assert [b'tester3'] == r.execute_command('TS.QUERYINDEX', 'generation=x', 'class=top', 'z=')
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.QUERYINDEX', 'z=', 'x!=2')
        assert [b'tester3'] == r.execute_command('TS.QUERYINDEX', 'z=', 'x=2')

        # Test filter list
        assert [b'tester1', b'tester2'] == r.execute_command('TS.QUERYINDEX', 'generation=x', 'class=(middle,junior)')
        assert [] == r.execute_command('TS.QUERYINDEX', 'generation=x', 'class=(a,b,c)')
        assert r.execute_command('TS.QUERYINDEX', 'generation=x') == r.execute_command('TS.QUERYINDEX',
                                                                                       'generation=(x)')
        assert r.execute_command('TS.QUERYINDEX', 'generation=x', 'class=()') == []
        assert r.execute_command('TS.QUERYINDEX', 'class=(middle,junior,top)', 'name!=(bob,rudy,fabi)') == [b'tester4']
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.QUERYINDEX', 'generation=x', 'class=(')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.QUERYINDEX', 'generation=x', 'class=(ab')
        with pytest.raises(redis.ResponseError):
            assert r.execute_command('TS.QUERYINDEX', 'generation!=(x,y)')
