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

def test_del():
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)
        
        assert r.execute_command('del', 't{1}')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{2}', b't{1}_agg']))
        res = r.execute_command('TS.MGET', 'filter',  'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([[b't{2}', [], []], [b't{1}_agg', [], []]]))

def test_flush():
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)

        assert r.execute_command('FLUSHALL')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(res, [])

        init(env, r)
        assert r.execute_command('FLUSHDB')
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(res, [])

def test_set():
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)

        assert r.execute_command('SET', 't{1}', 'awesome')
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{2}', b't{1}_agg']))
        env.assertEqual(r.execute_command('GET', 't{1}'), b'awesome')
        res = r.execute_command('TS.MGET', 'filter', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([[b't{2}', [], []], [b't{1}_agg', [], []]]))

def test_evict():
    env = Env()
    env.skipOnCluster()
    skip_on_rlec()
    with env.getClusterConnectionIfNeeded() as r:
        info = r.execute_command('INFO')
        max_mem = info['used_memory'] + 1024*1024
        assert r.execute_command('CONFIG', 'SET', 'maxmemory', str(max_mem) + 'b')
        assert r.execute_command('CONFIG', 'SET', 'maxmemory-policy', 'allkeys-lru')
        init(env, r)

        # make sure t{1} deleted
        res = r.execute_command('keys *')
        i = 4
        while b't{1}' in res:
            assert r.execute_command('TS.CREATE', 't{%s}' % (i,))
            i += 1
            res = r.execute_command('keys *')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush)')
        env.assertEqual(res, [])

        # restore maxmemory
        assert r.execute_command('CONFIG', 'SET', 'maxmemory', '0')


def test_expire():
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)
        
        res = r.execute_command('EXPIRE', 't{1}', 1)
        time.sleep(2)

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')

        env.assertEqual(sorted(res), sorted([b't{2}', b't{1}_agg']))
        res = r.execute_command('TS.MGET', 'filter', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([[b't{2}', [], []], [b't{1}_agg', [], []]]))


def test_unlink():
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)
        
        res = r.execute_command('UNLINK', 't{1}')
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{2}', b't{1}_agg']))
        res = r.execute_command('TS.MGET', 'filter', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([[b't{2}', [], []], [b't{1}_agg', [], []]]))

def test_restore():
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)
        
        serialized_val = r.execute_command('DUMP', 't{1}')
        assert r.execute_command('del', 't{1}')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted({b't{2}', b't{1}_agg'}))
        res = r.execute_command('TS.MGET', 'filter', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([[b't{1}_agg', [], []],
                                              [b't{2}', [], []]
                                              ]))

        assert r.execute_command('RESTORE', 't{1}', '0', serialized_val)
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{1}', b't{2}', b't{1}_agg']))
        res = r.execute_command('TS.MGET', 'filter', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([[b't{1}', [], [10, b'19']],
                                              [b't{2}', [], []],
                                              [b't{1}_agg', [], []]
                                              ]))

def test_rename():
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)
        
        assert r.execute_command('RENAME', 't{1}', 't{1}_renamed')
        res = r.execute_command('ts.info', 't{1}_agg')
        index = res.index(b'sourceKey')
        env.assertEqual(res[index+1], b't{1}_renamed')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([b't{2}', b't{1}_agg', b't{1}_renamed']))
        res = r.execute_command('TS.MGET', 'filter',  'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), sorted([[b't{2}', [], []],
                                             [b't{1}_agg', [], []],
                                             [b't{1}_renamed', [],  [10, b'19']]]))

        assert r.execute_command('RENAME', 't{1}_agg', 't{1}')
        res = r.execute_command('ts.info', 't{1}_renamed')
        index = res.index(b'rules')
        env.assertEqual(res[index+1], [[b't{1}', 10, b'AVG']])


def test_renamenx():
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)
        
        assert r.execute_command('RENAMENX', 't{1}', 't{1}_renamed')
        res = r.execute_command('ts.info', 't{1}_agg')
        index = res.index(b'sourceKey')
        env.assertEqual(res[index+1], b't{1}_renamed')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), [b't{1}_agg', b't{1}_renamed', b't{2}'])
        res = r.execute_command('TS.MGET', 'filter', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), [[b't{1}_agg', [], []], [b't{1}_renamed', [], [10, b'19']], [b't{2}', [], []]])

        assert r.execute_command('RENAMENX', 't{1}_agg', 't{1}_agg_renamed')
        res = r.execute_command('ts.info', 't{1}_renamed')
        index = res.index(b'rules')
        env.assertEqual(res[index+1], [[b't{1}_agg_renamed', 10, b'AVG']])


def test_copy_compressed_uncompressed():
    env = Env()
    env.skipOnVersionSmaller("6.2.0")
    for compresssion in ["UNCOMPRESSED", 'COMPRESSED']:
        with env.getClusterConnectionIfNeeded() as r:
            r.execute_command('FLUSHALL')
            init(env, r, compression=compresssion)
            for i in range(1000):
                assert r.execute_command('TS.ADD', 't{1}', 1638304650 + i, i)

            assert r.execute_command('COPY', 't{1}', 't{1}_copied')
            t1_info = r.execute_command('ts.info', 't{1}')
            res = r.execute_command('ts.info', 't{1}_agg')
            index = res.index(b'sourceKey')
            env.assertEqual(res[index+1], b't{1}')

            res = r.execute_command('ts.info', 't{1}_copied')
            index = res.index(b'sourceKey')
            env.assertEqual(res[index+1], None)
            index = res.index(b'rules')
            env.assertEqual(res[index+1], [])
            index = res.index(b'labels')
            env.assertEqual(res[index+1], t1_info[index+1])

            data = r.execute_command('TS.range', 't{1}', '-', '+')
            copied_data = r.execute_command('TS.range', 't{1}_copied', '-', '+',)
            assert data == copied_data

            res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
            env.assertEqual(sorted(res), sorted([b't{1}', b't{2}', b't{1}_agg', b't{1}_copied']))
            res = r.execute_command('TS.MGET', 'filter', 'name=(mush,zavi,rex)')
            env.assertEqual(sorted(res), sorted([[b't{1}', [], [1638305649, b'999']],
                                                 [b't{2}', [], []],
                                                 [b't{1}_agg', [],  [1638305630, b'984.5']],
                                                 [b't{1}_copied', [], [1638305649, b'999']]]))

            assert r.execute_command('COPY', 't{1}_agg', 't{1}_agg_copied')
            res = r.execute_command('ts.info', 't{1}_agg_copied')
            index = res.index(b'sourceKey')
            env.assertEqual(res[index+1], None)
            index = res.index(b'rules')
            env.assertEqual(res[index+1], [])

            res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
            env.assertEqual(sorted(res), sorted([b't{1}', b't{2}', b't{1}_agg', b't{1}_copied', b't{1}_agg_copied']))
            res = r.execute_command('TS.MGET', 'filter', 'name=(mush,zavi,rex)')
            env.assertEqual(sorted(res), sorted([[b't{1}', [], [1638305649, b'999']],
                                                 [b't{2}', [], []],
                                                 [b't{1}_agg', [],  [1638305630, b'984.5']],
                                                 [b't{1}_copied', [], [1638305649, b'999']],
                                                 [b't{1}_agg_copied', [], [1638305630, b'984.5']]]
                                                ))
