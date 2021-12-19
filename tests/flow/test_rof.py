import pytest
import redis
import time

from packaging import version
from includes import *
from utils import Env

def init(env, r, compression="COMPRESSED"):
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
        r.execute_command("config set bigredis-max-ram-keys 0")
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{1}', b't{2}', b't{1}_agg']))
        r.execute_command("config set bigredis-max-ram-keys 3")
        r.execute_command("config set bigredis-max-disk-value 0")
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{1}', b't{2}', b't{1}_agg']))
        r.execute_command("config set bigredis-max-disk-value 1000")
        r.execute_command("config set bigredis-max-ram-keys 0")
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{1}', b't{2}', b't{1}_agg']))
        r.execute_command("config set bigredis-max-ram-keys 3")

def test_del_from_disk():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)

        r.execute_command("config set bigredis-max-ram-keys 0")
        assert r.execute_command('del', 't{1}')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{2}', b't{1}_agg']))
        r.execute_command("config set bigredis-max-ram-keys 3")
        res = r.execute_command('TS.MGET', 'filter',  'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([[b't{2}', [], []], [b't{1}_agg', [], []]]))

def test_del_from_ram():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)

        r.execute_command("config set bigredis-max-disk-value 0")
        assert r.execute_command('del', 't{1}')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{2}', b't{1}_agg']))
        res = r.execute_command('TS.MGET', 'filter',  'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([[b't{2}', [], []], [b't{1}_agg', [], []]]))

        r.execute_command("config set bigredis-max-disk-value 1000")
