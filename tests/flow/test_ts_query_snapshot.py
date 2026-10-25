import time
from includes import *


def test_queryindex_union_snapshot():
    """Overlapping leaves and repeated matchers emit each key once, even on expiry."""
    skip_on_rlec()
    env = Env()
    shards = [env.getConnection(i + 1) for i in range(env.shardsCount)]
    try:
        for shard in shards:
            shard.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', 0)
        env.flush()
        with env.getClusterConnectionIfNeeded() as r:
            for i in range(64):
                key = f'key:{i}'
                r.execute_command('TS.CREATE', key, 'LABELS', 'group', 'all',
                                  'a', 'x', 'a', 'y')
                if i % 2:
                    r.pexpire(key, 500)
            time.sleep(.6)
            actual = shards[0].execute_command('TS.QUERYINDEX', 'a=(x,y,x,y)', 'group=all')
            env.assertEqual(sorted(actual), sorted(f'key:{i}'.encode() for i in range(0, 64, 2)))
            env.assertEqual(len(actual), 32)
    finally:
        for shard in shards:
            shard.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', 1)


def test_queryindex_binary_names():
    env = Env()
    env.flush()
    with env.getClusterConnectionIfNeeded() as r:
        keys = [b'key\0one', b'key\0two', b'plain']
        for key in keys:
            r.execute_command('TS.CREATE', key, 'LABELS', 'group', 'all')
        env.assertEqual(sorted(env.getConnection(1).execute_command('TS.QUERYINDEX', 'group=all')),
                        sorted(keys))


def test_querylabels_index_compatibility():
    """Preserve how the existing index represents duplicate and ambiguous labels."""
    env = Env()
    fixtures = [
        ([b'a', b'z', b'a', b'b'], [b'a', b'group'], [b'b']),
        ([b'a=b', b'c', b'a', b'b=c', b'a', b'z'], [b'a', b'a=b', b'group'], [b'z']),
        ([b'a\0x', b'v\0y', b'a', b'v'], [b'a', b'group'], [b'v']),
    ]
    for labels, names, values in fixtures:
        env.flush()
        with env.getClusterConnectionIfNeeded() as r:
            r.execute_command('TS.CREATE', 'key', 'LABELS', 'group', 'all', *labels)
            for suffix in ([], ['FILTER', 'group=all']):
                shard = env.getConnection(1)
                env.assertEqual(sorted(shard.execute_command('TS.QUERYLABELS', 'LABELS', *suffix)),
                                names)
                env.assertEqual(sorted(shard.execute_command('TS.QUERYLABELS', 'VALUES', 'a', *suffix)),
                                values)
