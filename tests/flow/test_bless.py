# RED-213286: BLESS (Redis 8.12) marks a key NO-EVICT so maxmemory eviction never
# picks it. The flag lives outside the module's value, so every module path that
# writes, copies, saves or reloads a series has to keep it. RED-220725 was such a
# path in RedisBloom: replacing a value through RM_ModuleTypeSetValue dropped the
# flag. RedisTimeSeries writes in place; this guards that every write keeps it.

from includes import *
from test_replication import ROWS


def _bless_env():
    env = Env()
    env.skipOnCluster()  # BLESS state and INFO blessed_keys are per shard
    try:
        env.cmd('BLESS', 'GET', 'missing')
    except redis.ResponseError as e:
        if 'unknown command' in str(e):
            env.skip()
    return env


def _assert_blessed(env, keys, when):
    for key in keys:
        env.assertEqual(env.cmd('BLESS', 'GET', key), [b'NO-EVICT'], message=f'{when}: {key}')
    env.assertEqual(env.cmd('BLESS', 'GET', 'plain'), [], message=f'{when}: plain')
    env.assertEqual(env.cmd('INFO', 'stats')['blessed_keys'], len(keys), message=when)


def _wait_aof_rewrite(env, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        info = env.cmd('INFO', 'persistence')
        if not info['aof_rewrite_in_progress'] and not info['aof_rewrite_scheduled']:
            env.assertEqual(info['aof_last_bgrewrite_status'], 'ok')
            return
        time.sleep(0.1)
    env.assertTrue(False, message='AOF rewrite did not finish')


def _create_series(env):
    for key in ('series', 'plain'):
        env.cmd('TS.CREATE', key, 'LABELS', 'name', key)
        env.cmd('TS.ADD', key, 1, 1)
    env.cmd('BLESS', 'SET', 'series', 'NO-EVICT')


def test_write_commands_keep_no_evict():
    """Bless every key the setup creates, then run the write command on them."""
    env = _bless_env()
    con = env.getConnection()
    for cmd, setup, write, _ in ROWS:
        con.execute_command('FLUSHALL')
        for spec in (setup or []):
            con.execute_command(*spec)
        keys = con.execute_command('KEYS', '*')
        if not keys:
            continue  # the write creates the key (ts.create)
        for key in keys:
            con.execute_command('BLESS', 'SET', key, 'NO-EVICT')
        con.execute_command(*write)
        for key in keys:
            env.assertEqual(con.execute_command('BLESS', 'GET', key), [b'NO-EVICT'], message=f'{cmd}: {key}')
        env.assertEqual(con.execute_command('INFO', 'stats')['blessed_keys'], len(keys), message=cmd)


def test_no_evict_survives_copy_rdb_and_aof_rewrite():
    env = _bless_env()
    _create_series(env)

    # COPY gives the destination the source's flag, the same way it gives it the TTL.
    env.cmd('COPY', 'series', 'copy')
    _assert_blessed(env, ['series', 'copy'], 'COPY')

    env.cmd('DEBUG', 'RELOAD')
    _assert_blessed(env, ['series', 'copy'], 'DEBUG RELOAD')

    # Without the RDB preamble the rewrite emits the type's aof_rewrite callback
    # output (RESTORE) followed by BLESS SET.
    env.cmd('CONFIG', 'SET', 'aof-use-rdb-preamble', 'no')
    env.cmd('CONFIG', 'SET', 'appendonly', 'yes')
    _wait_aof_rewrite(env)
    env.cmd('BGREWRITEAOF')
    _wait_aof_rewrite(env)
    env.cmd('DEBUG', 'LOADAOF')
    _assert_blessed(env, ['series', 'copy'], 'AOF rewrite')


def test_blessed_series_survives_eviction():
    env = _bless_env()
    env.cmd('CONFIG', 'SET', 'maxmemory-policy', 'allkeys-lru')
    _create_series(env)

    # Under --use-slaves the 1MB replication backlog counts toward maxmemory, so the
    # headroom has to exceed it or eviction runs out of unblessed keys and fails with OOM.
    env.cmd('CONFIG', 'SET', 'maxmemory', env.cmd('INFO', 'memory')['used_memory'] + 4000000)
    with env.getConnection().pipeline(transaction=False) as pipe:
        for i in range(4000):
            pipe.set(f'flood:{i}', 'x' * 2000)
        pipe.execute()
    env.cmd('CONFIG', 'SET', 'maxmemory', 0)

    env.assertGreater(env.cmd('INFO', 'stats')['evicted_keys'], 0)
    env.assertEqual(env.cmd('EXISTS', 'series'), 1)
