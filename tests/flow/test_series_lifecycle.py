import os

import redis
from includes import *
from test_helper_classes import _get_ts_info


class testSeriesLifecycle:
    """Series, rule and label-index lifecycle edge cases in src/tsdb.c.

    All methods share one env. Every method writes, so setUp flushes. Keys in a
    method share a hash tag, so the same code runs on the cluster lane.
    """

    def __init__(self):
        skip_on_rlec()
        self.env = Env(enableDebugCommand=True)

    def setUp(self):
        self.env.flush()

    def _shards(self):
        return [self.env.getConnection(i) for i in range(1, self.env.shardsCount + 1)]

    def _query_index(self, *filters):
        # Keyless: any shard answers for the whole cluster.
        return sorted(self.env.getConnection(1).execute_command('TS.QUERYINDEX', *filters))

    def test_createrule_with_partial_key_permissions(self):
        # TS.CREATERULE's key specs have no ACCESS/UPDATE flags, so Redis admits a user with
        # any permission on both keys. The module then checks read and write itself.
        env = self.env
        r = env.getClusterConnectionIfNeeded()
        src, dst = '{acl}src', '{acl}dst'
        r.execute_command('TS.CREATE', src)
        r.execute_command('TS.CREATE', dst)
        users = {
            'src_write_only': (['%W~{acl}src', '~{acl}dst'], 'read permission'),
            'src_read_only': (['%R~{acl}src', '~{acl}dst'], 'write permission'),
            'dst_read_only': (['~{acl}src', '%R~{acl}dst'], 'write permission'),
        }
        # The shard that owns the keys (RLTest's getConnectionByKey breaks on newer redis-py).
        owner = r.get_redis_connection(r.get_node_from_key(src)) if env.isCluster() else r
        pool = owner.connection_pool
        try:
            for name, (patterns, msg) in users.items():
                # ACL users are per node.
                for conn in self._shards():
                    conn.execute_command('ACL', 'SETUSER', name, 'on', '>pw', 'resetkeys', *patterns, '+@all')
                options = dict(pool.connection_kwargs, username=name, password='pw')
                user_pool = redis.ConnectionPool(connection_class=pool.connection_class, **options)
                try:
                    with env.assertResponseError(contained=msg):
                        redis.Redis(connection_pool=user_pool).execute_command(
                            'TS.CREATERULE', src, dst, 'AGGREGATION', 'sum', 10)
                finally:
                    user_pool.disconnect()
                # The denied request changed neither series.
                env.assertEqual(_get_ts_info(r, src).rules, [])
                env.assertEqual(_get_ts_info(r, dst).sourceKey, None)
        finally:
            for conn in self._shards():
                conn.execute_command('ACL', 'DELUSER', *users)

        # With full access the same rule is created and compacts.
        env.assertEqual(r.execute_command('TS.CREATERULE', src, dst, 'AGGREGATION', 'sum', 10), b'OK')
        r.execute_command('TS.MADD', src, 1, 1, src, 2, 2, src, 11, 5)
        env.assertEqual(r.execute_command('TS.RANGE', dst, '-', '+'), [[0, b'3']])

    def test_restore_over_indexed_series(self):
        env = self.env
        r = env.getClusterConnectionIfNeeded()
        k, donor, keep = b'{rs}k', b'{rs}donor', b'{rs}keep'
        r.execute_command('TS.CREATE', k, 'LABELS', 'gen', 'old', 'grp', 'rs')
        r.execute_command('TS.ADD', k, 1, 1)
        r.execute_command('TS.CREATE', donor, 'LABELS', 'gen', 'new', 'grp', 'rs')
        r.execute_command('TS.ADD', donor, 5, 50)
        r.execute_command('TS.CREATE', keep, 'LABELS', 'grp', 'rs')
        r.execute_command('TS.ADD', keep, 7, 70)

        # A series over an indexed series: Redis sends only "restore", so the module drops
        # the old key's labels before it indexes the restored ones.
        r.restore(k, 0, r.dump(donor), replace=True)
        env.assertEqual(self._query_index('gen=old'), [])
        env.assertEqual(self._query_index('gen=new'), [donor, k])
        env.assertEqual(self._query_index('grp=rs'), [donor, k, keep])
        env.assertEqual(r.execute_command('TS.RANGE', k, '-', '+'), [[5, b'50']])

        # A string over an indexed series: the series leaves the index, and the module's
        # "restore" handler leaves the string alone.
        r.set('{rs}str', 'v')
        r.restore(donor, 0, r.dump('{rs}str'), replace=True)
        env.assertEqual(r.get(donor), b'v')
        env.assertEqual(self._query_index('gen=new'), [k])
        env.assertEqual(self._query_index('grp=rs'), [k, keep])

        # A plain series over a compaction destination: the dump has no source, so the
        # source's rule is stale and is dropped instead of writing into the new key.
        src, dst = '{rs}src', '{rs}dst'
        r.execute_command('TS.CREATE', src)
        r.execute_command('TS.CREATE', dst)
        r.execute_command('TS.CREATERULE', src, dst, 'AGGREGATION', 'sum', 10)
        r.execute_command('TS.CREATE', '{rs}plain')
        r.execute_command('TS.ADD', '{rs}plain', 3, 30)
        r.restore(dst, 0, r.dump('{rs}plain'), replace=True)
        r.execute_command('TS.MADD', src, 1, 1, src, 11, 1)
        env.assertEqual(r.execute_command('TS.RANGE', dst, '-', '+'), [[3, b'30']])
        env.assertEqual(_get_ts_info(r, src).rules, [])
        env.assertEqual(_get_ts_info(r, dst).sourceKey, None)

        # The series nobody touched still takes writes.
        env.assertEqual(r.execute_command('TS.ADD', keep, 8, 80), 8)
        env.assertEqual(r.execute_command('TS.RANGE', keep, '-', '+'), [[7, b'70'], [8, b'80']])

    def test_reload_over_indexed_series(self):
        env = self.env
        r = env.getClusterConnectionIfNeeded()
        n, src, dst, s = b'{ld}n', b'{ld}src', b'{ld}dst', b'{ld}str'
        r.execute_command('TS.CREATE', n, 'LABELS', 'v', 'saved')
        r.execute_command('TS.ADD', n, 1, 1)
        r.execute_command('TS.CREATE', src)
        r.execute_command('TS.CREATE', dst)
        r.execute_command('TS.CREATERULE', src, dst, 'AGGREGATION', 'sum', 10)
        r.set(s, 'x')

        # A plain reload also loads the string; the module's "loaded" handler skips it.
        for conn in self._shards():
            conn.execute_command('DEBUG', 'RELOAD')
        env.assertEqual(r.get(s), b'x')
        env.assertEqual(self._query_index('v=saved'), [n])

        for conn in self._shards():
            conn.execute_command('SAVE')
        r.execute_command('TS.ALTER', n, 'LABELS', 'v', 'live')
        r.execute_command('TS.ADD', n, 2, 2)

        # MERGE NOFLUSH loads the saved keys over the live ones without a flush, so each
        # loaded series is already in the index. Only the saved labels may remain.
        for conn in self._shards():
            conn.execute_command('DEBUG', 'RELOAD', 'MERGE', 'NOFLUSH', 'NOSAVE')
        env.assertEqual(self._query_index('v=live'), [])
        env.assertEqual(self._query_index('v=(saved,live)'), [n])
        env.assertEqual(r.execute_command('TS.RANGE', n, '-', '+'), [[1, b'1']])
        env.assertEqual(r.get(s), b'x')

        # The reloaded rule still compacts.
        r.execute_command('TS.MADD', src, 1, 1, src, 2, 2, src, 11, 1)
        env.assertEqual(r.execute_command('TS.RANGE', dst, '-', '+'), [[0, b'3']])

    def test_destination_replaced_by_non_series_key(self):
        env = self.env
        r = env.getClusterConnectionIfNeeded()
        src, gone, kept = b'{nd}src', b'{nd}gone', b'{nd}kept'
        for key in (src, gone, kept):
            r.execute_command('TS.CREATE', key)
        r.execute_command('TS.CREATERULE', src, gone, 'AGGREGATION', 'sum', 10)
        r.execute_command('TS.CREATERULE', src, kept, 'AGGREGATION', 'sum', 10)
        r.execute_command('TS.MADD', src, 1, 1, src, 11, 1, src, 21, 1)
        r.set(gone, 'not a series')

        # Append, upsert into a closed bucket, and range delete each skip the stale rule
        # and keep updating the other destination.
        r.execute_command('TS.ADD', src, 31, 1)
        r.execute_command('TS.ADD', src, 2, 5)
        r.execute_command('TS.DEL', src, 11, 11)
        env.assertEqual(r.get(gone), b'not a series')
        env.assertEqual(_get_ts_info(r, src).rules, [[kept, 10, b'SUM', 0]])
        env.assertEqual(r.execute_command('TS.RANGE', kept, '-', '+'), [[0, b'6'], [20, b'1']])


def test_policy_skips_destination_in_other_slot():
    # Needs cluster mode in every lane; a 1-shard cluster is enough for the slot check.
    # Non-cluster lanes don't pass tests.sh's cluster-bus flag, so set it here.
    skip_on_rlec()
    env = Env(env='oss-cluster', shardsCount=1, useSlaves=False, useAof=False,
              clusterBusPortProtectedMode=os.getenv('CLUSTER_BUS_PROTECTED_MODE', 'no') or None)
    conn = env.getConnection()  # the only shard owns every slot
    plain, tagged = 'lifecycle', '{lifecycle}tagged'
    env.assertNotEqual(conn.execute_command('CLUSTER', 'KEYSLOT', plain),
                       conn.execute_command('CLUSTER', 'KEYSLOT', plain + '_SUM_1000'))
    old_policy = conn.execute_command('CONFIG', 'GET', 'ts-compaction-policy')[1]
    try:
        conn.execute_command('CONFIG', 'SET', 'ts-compaction-policy', 'sum:1s:1h')
        # TS.ADD creates both series with the global policy. The generated destination of
        # the untagged key hashes to another slot, so it gets no rule and no key.
        conn.execute_command('TS.ADD', plain, 1000, 1)
        conn.execute_command('TS.ADD', tagged, 1000, 1)
        env.assertEqual(_get_ts_info(conn, plain).rules, [])
        env.assertEqual(conn.exists(plain + '_SUM_1000'), 0)
        env.assertEqual(_get_ts_info(conn, tagged).rules, [[(tagged + '_SUM_1000').encode(), 1000, b'SUM', 0]])

        # Both series keep working, and the tagged one compacts.
        conn.execute_command('TS.ADD', plain, 2000, 2)
        conn.execute_command('TS.ADD', tagged, 2000, 2)
        env.assertEqual(conn.execute_command('TS.RANGE', plain, '-', '+'), [[1000, b'1'], [2000, b'2']])
        env.assertEqual(conn.execute_command('TS.RANGE', tagged + '_SUM_1000', '-', '+'), [[1000, b'1']])
    finally:
        conn.execute_command('CONFIG', 'SET', 'ts-compaction-policy', old_policy)
