import pytest
import redis
from includes import *
from utils import is_resp3_possible


def _make_fixture(r):
    assert r.execute_command('TS.CREATE', 'tester1', 'LABELS', 'name', 'bob', 'class', 'middle', 'generation', 'x')
    assert r.execute_command('TS.CREATE', 'tester2', 'LABELS', 'name', 'rudy', 'class', 'junior', 'generation', 'x')
    assert r.execute_command('TS.CREATE', 'tester3', 'LABELS', 'name', 'fabi', 'class', 'top', 'generation', 'x',
                             'x', '2')
    assert r.execute_command('TS.CREATE', 'tester4', 'LABELS', 'name', 'anybody', 'class', 'top', 'type', 'noone',
                             'x', '2', 'z', '3')


def test_querylabels_labels():
    env = Env()
    env.flush()
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        _make_fixture(r)

        res = r1.execute_command('TS.QUERYLABELS', 'LABELS', 'FILTER', 'generation=x')
        assert sorted(res) == sorted([b'name', b'class', b'generation', b'x'])


def test_querylabels_values():
    env = Env()
    env.flush()
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        _make_fixture(r)

        res = r1.execute_command('TS.QUERYLABELS', 'VALUES', 'name', 'FILTER', 'generation=x')
        assert sorted(res) == sorted([b'bob', b'rudy', b'fabi'])

        res = r1.execute_command('TS.QUERYLABELS', 'VALUES', 'class', 'FILTER', 'generation=x')
        assert sorted(res) == sorted([b'middle', b'junior', b'top'])

        res = r1.execute_command('TS.QUERYLABELS', 'VALUES', 'x', 'FILTER', 'generation=x')
        assert sorted(res) == sorted([b'2'])

        # A label that exists but has no matching series under this filter -> empty result.
        res = r1.execute_command('TS.QUERYLABELS', 'VALUES', 'type', 'FILTER', 'generation=x')
        assert res == []

        # A label that doesn't exist at all -> empty result, not an error.
        res = r1.execute_command('TS.QUERYLABELS', 'VALUES', 'nosuchlabel', 'FILTER', 'generation=x')
        assert res == []


def test_querylabels_no_filter():
    env = Env()
    env.flush()
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        _make_fixture(r)

        res = r1.execute_command('TS.QUERYLABELS', 'LABELS')
        assert sorted(res) == sorted([b'name', b'class', b'generation', b'x', b'type', b'z'])

        res = r1.execute_command('TS.QUERYLABELS', 'VALUES', 'class')
        assert sorted(res) == sorted([b'middle', b'junior', b'top'])


def test_querylabels_errors():
    env = Env()
    env.flush()
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        _make_fixture(r)

        with pytest.raises(redis.ResponseError):
            r1.execute_command('TS.QUERYLABELS')

        with pytest.raises(redis.ResponseError):
            r1.execute_command('TS.QUERYLABELS', 'NOT_A_SUBTYPE')

        with pytest.raises(redis.ResponseError):
            r1.execute_command('TS.QUERYLABELS', 'VALUES')

        with pytest.raises(redis.ResponseError):
            r1.execute_command('TS.QUERYLABELS', 'LABELS', 'FILTER')

        with pytest.raises(redis.ResponseError):
            r1.execute_command('TS.QUERYLABELS', 'LABELS', 'garbage', 'FILTER', 'generation=x')

        with pytest.raises(redis.ResponseError):
            r1.execute_command('TS.QUERYLABELS', 'VALUES', 'name', 'garbage', 'FILTER', 'generation=x')


def test_querylabels_resp3():
    env = Env(protocol=3)
    if not is_resp3_possible(env):
        env.skip()
    env.flush()
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        _make_fixture(r)
        res = r1.execute_command('TS.QUERYLABELS', 'VALUES', 'name', 'FILTER', 'generation=x')
        assert set(res) == {b'bob', b'rudy', b'fabi'}


@skip(onVersionLowerThan='7.4.0')
def test_querylabels_acl_filters_unreadable_keys(env):
    env.flush()
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        r.execute_command('TS.CREATE', 'allowed_key', 'LABELS', 'group', 'acltest', 'onlyallowed', 'x')
        r.execute_command('TS.CREATE', 'blocked_key', 'LABELS', 'group', 'acltest', 'onlyblocked', 'y')

        for i in range(env.shardsCount):
            env.getConnection(i).execute_command(
                'ACL', 'SETUSER', 'ql_acl_user', 'on', '>pass', '+@all', '~allowed_key*')
        r1.execute_command('AUTH', 'ql_acl_user', 'pass')
        try:
            res = r1.execute_command('TS.QUERYLABELS', 'LABELS', 'FILTER', 'group=acltest')
            assert sorted(res) == sorted([b'group', b'onlyallowed'])

            res = r1.execute_command('TS.QUERYLABELS', 'VALUES', 'group', 'FILTER', 'group=acltest')
            assert res == [b'acltest']

            res = r1.execute_command('TS.QUERYLABELS', 'VALUES', 'onlyblocked', 'FILTER', 'group=acltest')
            assert res == []

            # No FILTER at all must still work (never error), silently omitting blocked_key.
            res = r1.execute_command('TS.QUERYLABELS', 'LABELS')
            assert sorted(res) == sorted([b'group', b'onlyallowed'])
        finally:
            r1.execute_command('AUTH', 'default', '')
            for i in range(env.shardsCount):
                env.getConnection(i).execute_command('ACL', 'DELUSER', 'ql_acl_user')
