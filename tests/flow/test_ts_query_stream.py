import time
from includes import *


def test_query_expiry_rebuilds_cursors():
    """Expiry may remove several predicate leaves or the whole seed dictionary."""
    skip_on_rlec()
    env = Env()
    shards = [env.getConnection(i + 1) for i in range(env.shardsCount)]
    commands = [
        ('TS.QUERYINDEX', 'group=all'),
        ('TS.QUERYINDEX', 'tag=(a,b,a,b)', 'group=all'),
        ('TS.QUERYINDEX', 'group=all', 'tag!=(missing)'),
        ('TS.QUERYLABELS', 'VALUES', 'tag', 'FILTER', 'group=all'),
        ('TS.QUERYLABELS', 'VALUES', 'tag'),
    ]
    try:
        for shard in shards:
            shard.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', 0)
        for command in commands:
            for count, live_indices in [(4, []), (4, [1, 3]), (300, []),
                                        (300, [1, 257, 299]), (300, list(range(256, 300)))]:
                env.flush()
                with env.getClusterConnectionIfNeeded() as r:
                    # Same slot ensures one traversal experiences every deletion.
                    keys = [(f'{{stream}}:{i:04d}' + 'x' * 300 + '\0tail').encode()
                            for i in range(count)]
                    for i, key in enumerate(keys):
                        r.execute_command('TS.CREATE', key, 'LABELS', 'group', 'all',
                                          'tag', 'b' if i in live_indices else 'a')
                    for i, key in enumerate(keys):
                        if i not in live_indices:
                            r.pexpire(key, 100)
                    time.sleep(.15)
                    result = shards[0].execute_command(*command)
                    expected = ([keys[i] for i in live_indices]
                                if command[0] == 'TS.QUERYINDEX'
                                else ([b'b'] if live_indices else []))
                    env.assertEqual(sorted(result), sorted(expected))
                    env.assertEqual(len(result), len(expected))
                    env.assertEqual(sorted(shards[0].execute_command(*command)), sorted(expected))
    finally:
        for shard in shards:
            shard.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', 1)


def test_query_union_deduplicates_across_batches():
    """One series can appear in distinct union leaves on a batch boundary."""
    skip_on_rlec()
    env = Env()
    shards = [env.getConnection(i + 1) for i in range(env.shardsCount)]
    keys = [f'{{stream-union}}:{i:04d}'.encode() for i in range(300)]
    live_indices = [255, 256, 257, 299]
    try:
        for shard in shards:
            shard.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', 0)
        with env.getClusterConnectionIfNeeded() as r:
            for key in keys:
                r.execute_command('TS.CREATE', key, 'LABELS', 'group', 'all',
                                  'tag', 'a', 'tag', 'b')
            command = ('TS.QUERYINDEX', 'tag=(a,b,a,b)', 'group=all')
            env.assertEqual(sorted(shards[0].execute_command(*command)), sorted(keys))
            for i, key in enumerate(keys):
                if i not in live_indices:
                    r.pexpire(key, 100)
            time.sleep(.15)
            expected = sorted(keys[i] for i in live_indices)
            for _ in range(2):
                env.assertEqual(sorted(shards[0].execute_command(*command)), expected)
    finally:
        for shard in shards:
            shard.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', 1)
