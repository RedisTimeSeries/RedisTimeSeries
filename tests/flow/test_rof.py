import pytest
import redis
import time

from packaging import version
from includes import *
from utils import Env

def init(env, r, compression="COMPRESSED"):
    r.execute_command("config set bigredis-max-ram-keys 10")
    assert r.execute_command('TS.CREATE', 't{1}', 'ENCODING', compression, 'LABELS', 'name', 'mush', 'fname', 'ox')
    assert r.execute_command('TS.CREATE', 't{2}', 'ENCODING', compression, 'LABELS', 'name', 'zavi', 'fname', 'zav')
    assert r.execute_command('TS.CREATE', 't{1}_agg', 'ENCODING', compression, 'LABELS', 'name', 'rex', 'fname', 'dog')
    assert r.execute_command('TS.CREATERULE', 't{1}', 't{1}_agg', 'AGGREGATION', 'avg', '10')
    
    assert r.execute_command('TS.add', 't{1}', '10', '19')

    res = r.execute_command('TS.QUERYINDEX', 'name=mush')
    env.assertEqual(res[0], b't{1}')

    res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
    env.assertEqual(sorted(res), sorted([b't{1}', b't{2}', b't{1}_agg']))

def test_move_to_out_of_disk():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)
        r.execute_command("DEBUG BIGREDIS-SWAPIN")
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{1}', b't{2}', b't{1}_agg']))
        r.execute_command("DEBUG BIGREDIS-SWAPOUT")
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{1}', b't{2}', b't{1}_agg']))
        r.execute_command("DEBUG BIGREDIS-SWAPIN")
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{1}', b't{2}', b't{1}_agg']))

def test_del_from_disk():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)

        r.execute_command("DEBUG BIGREDIS-SWAPOUT")
        assert r.execute_command('del', 't{1}')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{2}', b't{1}_agg']))
        r.execute_command("DEBUG BIGREDIS-SWAPIN")
        res = r.execute_command('TS.MGET', 'filter',  'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([[b't{2}', [], []], [b't{1}_agg', [], []]]))

def test_del_from_ram():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)

        r.execute_command("DEBUG BIGREDIS-SWAPIN")
        assert r.execute_command('del', 't{1}')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{2}', b't{1}_agg']))
        res = r.execute_command('TS.MGET', 'filter',  'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([[b't{2}', [], []], [b't{1}_agg', [], []]]))

def test_flush_from_ram():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)

        assert r.execute_command('FLUSHALL')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(res, [])

        init(env, r)
        r.execute_command("DEBUG BIGREDIS-SWAPIN")
        assert r.execute_command('FLUSHDB')
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(res, [])

def test_flush_from_disk():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)

        assert r.execute_command('FLUSHALL')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(res, [])

        init(env, r)
        r.execute_command("DEBUG BIGREDIS-SWAPOUT")
        assert r.execute_command('FLUSHDB')
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(res, [])

def test_set_from_ram():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)

        r.execute_command("DEBUG BIGREDIS-SWAPIN")
        assert r.execute_command('SET', 't{1}', 'awesome')
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{2}', b't{1}_agg']))
        env.assertEqual(r.execute_command('GET', 't{1}'), b'awesome')
        res = r.execute_command('TS.MGET', 'filter', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([[b't{2}', [], []], [b't{1}_agg', [], []]]))

def test_set_from_disk():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)

        r.execute_command("DEBUG BIGREDIS-SWAPOUT")
        assert r.execute_command('SET', 't{1}', 'awesome')
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{2}', b't{1}_agg']))
        env.assertEqual(r.execute_command('GET', 't{1}'), b'awesome')
        res = r.execute_command('TS.MGET', 'filter', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([[b't{2}', [], []], [b't{1}_agg', [], []]]))

def test_expire_from_ram():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)
        r.execute_command("DEBUG BIGREDIS-SWAPIN")

        res = r.execute_command('EXPIRE', 't{1}', 1)
        time.sleep(2)

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')

        env.assertEqual(sorted(res), sorted([b't{2}', b't{1}_agg']))
        res = r.execute_command('TS.MGET', 'filter', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([[b't{2}', [], []], [b't{1}_agg', [], []]]))

def test_expire_from_disk():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)
        r.execute_command("DEBUG BIGREDIS-SWAPOUT")

        res = r.execute_command('EXPIRE', 't{1}', 1)
        time.sleep(2)

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')

        env.assertEqual(sorted(res), sorted([b't{2}', b't{1}_agg']))
        res = r.execute_command('TS.MGET', 'filter', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([[b't{2}', [], []], [b't{1}_agg', [], []]]))

def test_unlink_from_ram():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)
        r.execute_command("DEBUG BIGREDIS-SWAPIN")

        res = r.execute_command('UNLINK', 't{1}')
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{2}', b't{1}_agg']))
        res = r.execute_command('TS.MGET', 'filter', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([[b't{2}', [], []], [b't{1}_agg', [], []]]))

def test_unlink_from_disk():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)
        r.execute_command("DEBUG BIGREDIS-SWAPOUT")

        res = r.execute_command('UNLINK', 't{1}')
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{2}', b't{1}_agg']))
        res = r.execute_command('TS.MGET', 'filter', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([[b't{2}', [], []], [b't{1}_agg', [], []]]))
