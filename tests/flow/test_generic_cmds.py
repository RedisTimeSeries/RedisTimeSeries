import pytest
import redis
import time

from RLTest import Env
from packaging import version

def init(env, r, compression="COMPRESSED"):
    assert r.execute_command('TS.CREATE', 't1', 'ENCODING', compression, 'LABELS', 'name', 'mush', 'fname', 'ox')
    assert r.execute_command('TS.CREATE', 't2', 'ENCODING', compression, 'LABELS', 'name', 'zavi', 'fname', 'zav')
    assert r.execute_command('TS.CREATE', 't3', 'ENCODING', compression, 'LABELS', 'name', 'rex', 'fname', 'dog')
    assert r.execute_command('TS.CREATERULE', 't1', 't3', 'AGGREGATION', 'avg', '10')
    
    assert r.execute_command('TS.add', 't1', '10', '19')

    res = r.execute_command('TS.QUERYINDEX', 'name=mush')
    env.assertEqual(res[0], b't1')

    res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
    env.assertEqual(sorted(res), [b't1', b't2', b't3'])

def test_del():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)
        
        assert r.execute_command('del t1')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), [b't2', b't3'])
        res = r.execute_command('keys *')
        env.assertEqual(sorted(res), [b't2', b't3'])

def test_flush():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)

        assert r.execute_command('FLUSHALL')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(res, [])
        res = r.execute_command('keys *')
        env.assertEqual(res, [])

        init(env, r)
        assert r.execute_command('FLUSHDB')
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(res, [])
        res = r.execute_command('keys *')
        env.assertEqual(res, [])

def test_set():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)

        assert r.execute_command('SET', 't1', 'awesome')
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), [b't2', b't3'])
        res = r.execute_command('keys *')
        env.assertEqual(sorted(res), [b't1', b't2', b't3'])

def test_evict():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        info = r.execute_command('INFO')
        max_mem = info['used_memory'] + 1024*1024
        assert r.execute_command('CONFIG', 'SET', 'maxmemory', str(max_mem) + 'b')
        assert r.execute_command('CONFIG', 'SET', 'maxmemory-policy', 'allkeys-lru')
        init(env, r)

        # make sure t1 deleted
        res = r.execute_command('keys *')
        i = 4
        while(sorted(res)[0] == b't1'):
            assert r.execute_command('TS.CREATE', 't%s' % (i))
            i += 1
            res = r.execute_command('keys *')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush)')
        env.assertEqual(res, [])

        # restore maxmemory
        assert r.execute_command('CONFIG', 'SET', 'maxmemory', '0')


def test_expire():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)
        
        res = r.execute_command('EXPIRE', 't1', 1)
        time.sleep(2)

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')

        env.assertEqual(sorted(res), [b't2', b't3'])
        res = r.execute_command('keys *')
        env.assertEqual(sorted(res), [b't2', b't3'])


def test_unlink():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)
        
        res = r.execute_command('UNLINK', 't1')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), [b't2', b't3'])
        res = r.execute_command('keys *')
        env.assertEqual(sorted(res), [b't2', b't3'])

def test_restore():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)
        
        serialized_val = r.execute_command('DUMP', 't1')
        assert r.execute_command('del t1')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), [b't2', b't3'])
        res = r.execute_command('keys *')
        env.assertEqual(sorted(res), [b't2', b't3'])

        assert r.execute_command('RESTORE', 't1', '0', serialized_val)
        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), [b't1',b't2', b't3'])
        res = r.execute_command('keys *')
        env.assertEqual(sorted(res), [b't1', b't2', b't3'])

def test_rename():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)
        
        assert r.execute_command('RENAME', 't1', 't4')
        res = r.execute_command('ts.info', 't3')
        index = res.index(b'sourceKey')
        env.assertEqual(res[index+1], b't4')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), [b't2', b't3', b't4'])
        res = r.execute_command('keys *')
        env.assertEqual(sorted(res), [b't2', b't3', b't4'])

        assert r.execute_command('RENAME', 't3', 't1')
        res = r.execute_command('ts.info', 't4')
        index = res.index(b'rules')
        env.assertEqual(res[index+1], [[b't1', 10, b'AVG']])


def test_renamenx():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        init(env, r)
        
        assert r.execute_command('RENAMENX', 't1', 't4')
        res = r.execute_command('ts.info', 't3')
        index = res.index(b'sourceKey')
        env.assertEqual(res[index+1], b't4')

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), [b't2', b't3', b't4'])
        res = r.execute_command('keys *')
        env.assertEqual(sorted(res), [b't2', b't3', b't4'])

        assert r.execute_command('RENAMENX', 't3', 't1')
        res = r.execute_command('ts.info', 't4')
        index = res.index(b'rules')
        env.assertEqual(res[index+1], [[b't1', 10, b'AVG']])


def test_copy_compressed():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        res = r.execute_command('INFO')
        if(version.parse(res['redis_version']) < version.parse("6.0.0")):
            self.skip() # copy exists only from version 6

        init(env, r)
        
        assert r.execute_command('COPY', 't1', 't4')
        res = r.execute_command('ts.info', 't3')
        index = res.index(b'sourceKey')
        env.assertEqual(res[index+1], b't1')

        res = r.execute_command('ts.info', 't4')
        index = res.index(b'sourceKey')
        env.assertEqual(res[index+1], None)
        index = res.index(b'rules')
        env.assertEqual(res[index+1], [])

        res = r.execute_command('TS.get', 't4')
        assert res == [10, b'19']

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), [b't1', b't2', b't3', b't4'])
        res = r.execute_command('keys *')
        env.assertEqual(sorted(res), [b't1', b't2', b't3', b't4'])

        assert r.execute_command('COPY', 't3', 't5')
        res = r.execute_command('ts.info', 't5')
        index = res.index(b'sourceKey')
        env.assertEqual(res[index+1], None)
        index = res.index(b'rules')
        env.assertEqual(res[index+1], [])

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), [b't1', b't2', b't3', b't4', b't5'])
        res = r.execute_command('keys *')
        env.assertEqual(sorted(res), [b't1', b't2', b't3', b't4', b't5'])

def test_copy_uncompressed():
    env = Env()
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        res = r.execute_command('INFO')
        if(version.parse(res['redis_version']) < version.parse("6.0.0")):
            self.skip() # copy exists only from version 6

        init(env, r, "UNCOMPRESSED")
        
        assert r.execute_command('COPY', 't1', 't4')
        res = r.execute_command('ts.info', 't3')
        index = res.index(b'sourceKey')
        env.assertEqual(res[index+1], b't1')

        res = r.execute_command('ts.info', 't4')
        index = res.index(b'sourceKey')
        env.assertEqual(res[index+1], None)
        index = res.index(b'rules')
        env.assertEqual(res[index+1], [])

        res = r.execute_command('TS.get', 't4')
        assert res == [10, b'19']

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), [b't1', b't2', b't3', b't4'])
        res = r.execute_command('keys *')
        env.assertEqual(sorted(res), [b't1', b't2', b't3', b't4'])

        assert r.execute_command('COPY', 't3', 't5')
        res = r.execute_command('ts.info', 't5')
        index = res.index(b'sourceKey')
        env.assertEqual(res[index+1], None)
        index = res.index(b'rules')
        env.assertEqual(res[index+1], [])

        res = r.execute_command('TS.QUERYINDEX', 'name=(mush,zavi,rex)')
        env.assertEqual(sorted(res), [b't1', b't2', b't3', b't4', b't5'])
        res = r.execute_command('keys *')
        env.assertEqual(sorted(res), [b't1', b't2', b't3', b't4', b't5'])
