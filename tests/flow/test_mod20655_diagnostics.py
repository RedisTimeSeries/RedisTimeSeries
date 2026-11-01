"""Investigation-only wrapper; retains original test behavior and shard evidence."""
import json
import time
import test_ts_mrange as original
from includes import Env


def test_expiry_diagnostics():
    env = Env(shardsCount=3, noLog=False)
    assert env.shardsCount == 3
    def snapshot(stage):
        for shard in range(1, 4):
            conn = env.getConnection(shard)
            conn.connection_pool.connection_kwargs['socket_timeout'] = 3
            try:
                logfile = conn.execute_command('CONFIG', 'GET', 'logfile')[1]
                assert logfile not in (b'/dev/null', b'', '/dev/null', '')
                data = {'stage': stage, 'shard': shard, 'utc_epoch': time.time(),
                        'logfile': logfile, 'cluster': conn.execute_command('CLUSTER', 'INFO'),
                        'server': conn.info('server'), 'stats': conn.info('stats')}
                print('SHARD_DIAGNOSTICS', json.dumps(data, default=str), flush=True)
            except Exception as exc:
                print('SNAPSHOT_ERROR', stage, shard, repr(exc), flush=True)
                if stage == 'before':
                    raise
    snapshot('before')
    try:
        original.test_mrange_with_expire_cmd(env)
    finally:
        snapshot('after')
