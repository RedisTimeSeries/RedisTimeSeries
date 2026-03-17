import math
import time

import pytest
import redis
from RLTest import Env
import redis.exceptions
from includes import *


def _shard_for_slot(env, slot):
    """Return the env.getConnection() index that owns *slot*.

    CLUSTER SLOTS entries are matched to env connections by port so that
    the mapping is correct regardless of slot-range ordering.
    """
    shard0 = env.getConnection(0)
    slots_raw = shard0.execute_command('CLUSTER', 'SLOTS')

    # Build port → shard-index map
    port_to_idx = {}
    for i in range(env.shardsCount):
        c = env.getConnection(i)
        port_to_idx[c.connection_pool.connection_kwargs['port']] = i

    for s in slots_raw:
        if int(s[0]) <= slot <= int(s[1]):
            owner_port = int(s[2][1])
            return port_to_idx.get(owner_port, 0)
    return 0

@skip(onVersionLowerThan='7.4.0')
def test_acl_ignore_on_metadata(env):
    with env.getClusterConnectionIfNeeded() as conn, env.getConnection(1) as conn1:
        conn.execute_command('TS.CREATE', 'a{x}', 'LABELS', 'x', 'y')
        conn.execute_command('TS.CREATE', 'b{x}', 'LABELS', 'x', 'y')
        conn.execute_command('TS.CREATERULE', 'a{x}', 'b{x}', 'AGGREGATION', 'SUM', 100)

        # Create a user with permissions only for 'a{x}'
        conn.execute_command(
            'ACL', 'SETUSER', 'usr1',
            'on', '>pw1', 'resetkeys', '~a*',
            '+@timeseries', '+@keyspace'
        )
        conn.execute_command('AUTH', 'usr1', 'pw1')

        # The user should be able to access the metadata of 'b{x}'
        result = conn1.execute_command('TS.QUERYINDEX', 'x=y')
        env.assertEqual(len(result), 2)

@skip(onVersionLowerThan='7.4.0')
def test_acl_with_ts_mget_mrange(env):
    username = 'testuser3'

    with env.getClusterConnectionIfNeeded() as conn, env.getConnection(1) as conn1:
        # Create two time series with labels
        conn.execute_command('TS.CREATE', 'series1', 'LABELS', 'group', 'test', 'name', 'series1')
        conn.execute_command('TS.CREATE', 'series2', 'LABELS', 'group', 'test', 'name', 'series2')

        # Add data to both series
        conn.execute_command('TS.ADD', 'series1', '*', 1)
        conn.execute_command('TS.ADD', 'series2', '*', 2)

        # Create a user with read permissions only for 'series1'
        conn.execute_command(
            'ACL', 'SETUSER', username,
            'on', '>password',
            '+ACL', '+TS.MGET', '+TS.MRANGE', '+TS.MREVRANGE', '+TS.INFO',
            '+TS.CREATERULE', '+TS.DELETERULE', '+TS.QUERYINDEX',
            '+@read',
            '~series1'
        )

        # Authenticate as the restricted user.
        conn.execute_command('AUTH', username, 'password')
        conn1.execute_command('AUTH', username, 'password')

        result = conn.execute_command('TS.INFO', 'series1')
        env.assertGreaterEqual(len(result), 1)

        with pytest.raises(redis.exceptions.NoPermissionError):
            result = conn1.execute_command('TS.INFO', 'series2')

        with pytest.raises(redis.exceptions.NoPermissionError):
            result = conn1.execute_command('TS.MGET', 'FILTER', 'group=test')

        with pytest.raises(redis.exceptions.NoPermissionError):
            result = conn1.execute_command('TS.MRANGE', '-', '+', 'FILTER', 'group=test')

        with pytest.raises(redis.exceptions.NoPermissionError):
            result = conn1.execute_command('TS.MRANGE', '-', '+', 'FILTER', 'group=test', 'GROUPBY', 'name', 'REDUCE', 'MAX')

        with pytest.raises(redis.exceptions.NoPermissionError):
            result = conn1.execute_command('TS.MREVRANGE', '-', '+', 'FILTER', 'group=test')

        with pytest.raises(redis.exceptions.NoPermissionError):
            result = conn1.execute_command('TS.MREVRANGE', '-', '+', 'FILTER', 'group=test', 'GROUPBY', 'name', 'REDUCE', 'MAX')

        if env.shardsCount == 1:
            with pytest.raises(redis.exceptions.NoPermissionError):
                result = conn.execute_command('TS.CREATERULE', 'series1', 'series2', 'AGGREGATION', 'AVG', '1')

            with pytest.raises(redis.exceptions.NoPermissionError):
                result = conn.execute_command('TS.CREATERULE', 'series2', 'series1', 'AGGREGATION', 'AVG', '1')

        # Now change the ACL to allow access to both series
        conn.execute_command('ACL', 'SETUSER', username, '+@read', '~*')

        result = conn.execute_command('TS.INFO', 'series1')
        env.assertGreaterEqual(len(result), 1)
        result = conn.execute_command('TS.INFO', 'series2')
        env.assertGreaterEqual(len(result), 1)

        result = conn1.execute_command('TS.MGET', 'FILTER', 'group=test')
        env.assertEqual(len(result), 2, message="TS.MGET should have returned two series")

        result = conn1.execute_command('TS.MRANGE', '-', '+', 'FILTER', 'group=test')
        env.assertEqual(len(result), 2, message="TS.MRANGE should have returned two series")
        result = conn1.execute_command('TS.MRANGE', '-', '+', 'FILTER', 'group=test', 'GROUPBY', 'name', 'REDUCE', 'MAX')
        env.assertEqual(len(result), 2, message="TS.MRANGE should have returned two series")

        result = conn1.execute_command('TS.MREVRANGE', '-', '+', 'FILTER', 'group=test')
        env.assertEqual(len(result), 2, message="TS.MREVRANGE should have returned two series")
        result = conn1.execute_command('TS.MREVRANGE', '-', '+', 'FILTER', 'group=test', 'GROUPBY', 'name', 'REDUCE', 'MAX')
        env.assertEqual(len(result), 2, message="TS.MREVRANGE should have returned two series")

        result = conn1.execute_command('TS.QUERYINDEX', 'group=test')
        env.assertEqual(len(result), 2, message="TS.QUERYINDEX should have returned two series")

        if env.shardsCount == 1:
            result = conn.execute_command('TS.CREATERULE', 'series1', 'series2', 'AGGREGATION', 'AVG', '1')
            env.assertEqual(result, b'OK')
            result = conn.execute_command('TS.DELETERULE', 'series1', 'series2')
            env.assertEqual(result, b'OK')

            result = conn.execute_command('TS.CREATERULE', 'series2', 'series1', 'AGGREGATION', 'AVG', '1')
            env.assertEqual(result, b'OK')
            result = conn.execute_command('TS.DELETERULE', 'series2', 'series1')
            env.assertEqual(result, b'OK')

        # Clean up
        conn.execute_command('AUTH', 'default', '')
        conn.execute_command('ACL', 'DELUSER', username)

@skip(onVersionLowerThan='7.4.0')
def test_non_local_data_when_the_user_has_no_access(env):
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.ADD', '{host1}_metric_1', 1, 100, 'LABELS', 'metric', 'cpu')
        r.execute_command('TS.ADD', '{host2}_metric_2', 2, 40, 'LABELS', 'metric', 'cpu')
        r.execute_command('TS.ADD', '{host1}_metric_1', 2, 95)
        r.execute_command('TS.ADD', '{host1}_metric_1', 10, 99)

    # Create a user with read permissions only for 'series1'
    env.execute_command(
        'ACL', 'SETUSER', 'testuser',
        'on', '>password',
        '~{host1}_metric_1',
        '+@all',
    )

    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        r1.execute_command('AUTH', 'testuser', 'password')

        with pytest.raises(redis.exceptions.NoPermissionError):
            r1.execute_command('TS.MRANGE - + FILTER metric=cpu')

@skip(onVersionLowerThan='7.4.0')
def test_non_local_data_when_the_user_has_access(env):
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        r.execute_command('TS.ADD', '{host1}_metric_1', 1, 100, 'LABELS', 'metric', 'cpu')
        r.execute_command('TS.ADD', '{host2}_metric_2', 2, 40, 'LABELS', 'metric', 'cpu')
        r.execute_command('TS.ADD', '{host1}_metric_1', 2, 95)
        r.execute_command('TS.ADD', '{host1}_metric_1', 10, 99)

        # Create a user with read permissions only for 'series1'
        r.execute_command(
            'ACL', 'SETUSER', 'testuser2',
            'on', '>password',
            '+@all', '~*',
        )

        r1.execute_command('AUTH', 'testuser2', 'password')

        env.assertEqual(len(r1.execute_command('TS.MRANGE - + FILTER metric=cpu')), 2)

def do_test_libmr(env):
    if not env.isCluster() or SANITIZER == 'address':
        env.skip()

    for shard in range(0, env.shardsCount):
        conn = env.getConnection(shard)
        conn.execute_command('debug', 'MARK-INTERNAL-CLIENT')
        conn.execute_command('timeseries.REFRESHCLUSTER')
        # make sure cluster will not turn to failed state and we will not be
        # able to execute commands on shards, on slow envs, run with valgrind,
        # or mac, it is needed.
        conn.execute_command('CONFIG', 'set', 'cluster-node-timeout', '120000')
        conn.execute_command('timeseries.FORCESHARDSCONNECTION')
    with TimeLimit(2):
        verifyClusterInitialized(env)

    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        r.execute_command('TS.ADD', '{host1}_metric_1', 1, 100, 'LABELS', 'metric', 'cpu')
        r.execute_command('TS.ADD', '{host2}_metric_2', 2, 40, 'LABELS', 'metric', 'cpu')
        r.execute_command('TS.ADD', '{host1}_metric_1', 2, 95)
        r.execute_command('TS.ADD', '{host1}_metric_1', 10, 99)

        # Create a user that cannot directly invoke the internal
        # libmr commands.
        r.execute_command(
            'ACL', 'SETUSER', 'testuser4',
            'on', '>password',
            '+ts.mrange', '~*',
        )

        r1.execute_command('AUTH', 'testuser4', 'password')

        env.assertEqual(len(r1.execute_command('TS.MRANGE - + FILTER metric=cpu')), 2)

@skip(onVersionLowerThan='8.0.0')
def test_libmr_with_internal_secret_support(env):
    do_test_libmr(env)

# Test constants: 3-shard MRANGE ACL test
NUM_SHARDS_REQUIRED = 3
POINTS_PER_SERIES = 2
# Sample data (timestamp, value) added to each series
SAMPLE_DATA = [(1, 10), (2, 20)]
MAX_KEYS_TO_TRY = 30  # try up to this many hash tags to get one key per shard
SHARD_0, SHARD_1, SHARD_2 = 0, 1, 2
COORD_SHARD_USER1 = SHARD_1
COORD_SHARD_USER2 = SHARD_1
COORD_SHARD_USER3 = SHARD_2
USER1_EXPECTED_SERIES_COUNT = 3
KEY_PATTERN_ALL = '*'
# Pattern that matches no key in this test (participant still replies with empty array)
NO_MATCH_KEY_PATTERN = 'no_acl_key_*'


@skip(onVersionLowerThan='7.4.0')
def test_mrange_acl_partial_then_full_3shards(env):
    """MRANGE with 3 shards: one timeseries per shard (same filter), 2 values each.
    user1 = full permission on all shards → MRANGE returns 3 series.
    user2 = read permission on 2 shards only → MRANGE returns NOPERM error.
    user3 = read permission on 1 shard only → MRANGE returns NOPERM error."""
    if not env.isCluster() or env.shardsCount < NUM_SHARDS_REQUIRED:
        env.skip()

    filter_label = 'group=acl_mrange_3s'
    conns = [env.getConnection(SHARD_0), env.getConnection(SHARD_1), env.getConnection(SHARD_2)]

    def slot_to_shard(slot):
        return _shard_for_slot(env, slot)

    # Create keys with different hash tags until we have exactly one key per shard.
    keys_by_shard = [[] for _ in range(env.shardsCount)]
    with env.getClusterConnectionIfNeeded() as r:
        for i in range(MAX_KEYS_TO_TRY):
            key = 'ts_acl_{}'.format(i) + '{' + str(i) + '}'
            r.execute_command('TS.CREATE', key, 'LABELS', 'group', 'acl_mrange_3s')
            slot = conns[SHARD_0].execute_command('CLUSTER', 'KEYSLOT', key)
            shard = slot_to_shard(slot)
            keys_by_shard[shard].append(key)
            if all(keys_by_shard[s] for s in range(NUM_SHARDS_REQUIRED)):
                break

    if not all(keys_by_shard[s] for s in range(NUM_SHARDS_REQUIRED)):
        env.skip()  # could not get one key per shard

    # Keep exactly one key per shard; delete the rest.
    key_per_shard = [keys_by_shard[s][0] for s in range(NUM_SHARDS_REQUIRED)]
    # QUERYINDEX has no key; use a shard connection so redis-py can route (command runs as MR on server).
    raw = conns[SHARD_0].execute_command('TS.QUERYINDEX', filter_label)
    all_created = [k.decode() if isinstance(k, bytes) else k for k in raw]
    with env.getClusterConnectionIfNeeded() as r:
        for k in all_created:
            if k not in key_per_shard:
                r.delete(k)

    # Add the same sample data to each timeseries.
    with env.getClusterConnectionIfNeeded() as r:
        for key in key_per_shard:
            for ts, val in SAMPLE_DATA:
                r.execute_command('TS.ADD', key, ts, val)

    # ACL helper: set key patterns for a user on a given connection (shard).
    def set_user_on_shard(conn, username, password, key_patterns):
        cmd = ['ACL', 'SETUSER', username, 'on', '>' + password,
               '+@read', '+TS.MRANGE', '+@timeseries']
        for p in key_patterns:
            cmd.append('~' + p)
        conn.execute_command(*cmd)

    user1, pw1 = 'acl_mrange_user1', 'pw1'
    user2, pw2 = 'acl_mrange_user2', 'pw2'
    user3, pw3 = 'acl_mrange_user3', 'pw3'
    user4, pw4 = 'acl_mrange_user4', 'pw4'

    # user1: full permission on all shards
    for conn in conns:
        set_user_on_shard(conn, user1, pw1, [KEY_PATTERN_ALL])

    # user2: permission only on shards 0 and 1. On shard 2 use a pattern that matches no key.
    set_user_on_shard(conns[SHARD_0], user2, pw2, [KEY_PATTERN_ALL])
    set_user_on_shard(conns[SHARD_1], user2, pw2, [KEY_PATTERN_ALL])
    set_user_on_shard(conns[SHARD_2], user2, pw2, [NO_MATCH_KEY_PATTERN])

    # user3: permission only on shard 2; on shards 0 and 1 use a pattern that matches no key.
    set_user_on_shard(conns[SHARD_0], user3, pw3, [NO_MATCH_KEY_PATTERN])
    set_user_on_shard(conns[SHARD_1], user3, pw3, [NO_MATCH_KEY_PATTERN])
    set_user_on_shard(conns[SHARD_2], user3, pw3, [KEY_PATTERN_ALL])

    # user4: on the coordinator shard (SHARD_1) use a restricted pattern (no access to its key),
    # full permission on the other shards. The coordinator itself should detect the NOPERM.
    set_user_on_shard(conns[SHARD_0], user4, pw4, [KEY_PATTERN_ALL])
    set_user_on_shard(conns[SHARD_1], user4, pw4, [NO_MATCH_KEY_PATTERN])
    set_user_on_shard(conns[SHARD_2], user4, pw4, [KEY_PATTERN_ALL])

    # Run MRANGE from a shard where the user has full key access (~*), so the coordinator
    # passes IsCurrentUserAllowedToReadAllTheKeys and the permission restriction only applies
    # on participant shards (which then return empty).
    def run_mrange(coord_conn, username, password):
        coord_conn.execute_command('AUTH', username, password)
        try:
            return coord_conn.execute_command('TS.MRANGE', '-', '+', 'FILTER', filter_label)
        finally:
            coord_conn.execute_command('AUTH', 'default', '')

    def series_names(result):
        return [r[0] if isinstance(r[0], str) else r[0].decode() for r in result]

    # Format: list of [name, labels, samples] - same as MRANGE reply. Prints in Redis-style.
    def print_mrange_format(label, data, kind='result'):
        print('MRANGE {} {}:'.format(label, kind))
        for i, series in enumerate(data):
            outer_n = i + 1
            name = series[0]
            name_str = name.decode() if isinstance(name, bytes) else name
            labels = series[1] if len(series) > 1 else []
            samples = series[2] if len(series) > 2 else []
            print('{}) 1) "{}"'.format(outer_n, name_str))
            print('   2) (empty array)' if not labels else '   2) {}'.format(labels))
            if samples:
                print('   3) 1) 1) (integer) {}'.format(samples[0][0]))
                print('         2) {}'.format(samples[0][1]))
                for j in range(1, len(samples)):
                    print('      2) 1) (integer) {}'.format(samples[j][0]))
                    print('         2) {}'.format(samples[j][1]))
            else:
                print('   3) (empty array)')
        print('')

    # Build expected reply shape: [name, [], [samples]] per series, using SAMPLE_DATA.
    def expected_reply(keys):
        samples = [[ts, val] for ts, val in SAMPLE_DATA]
        return [[k, [], samples] for k in keys]

    # ---------- Scenario 1: user1 (full permission on all shards) ----------
    print('')
    print('=== Scenario 1: user1 ===')
    print('Permissions: user1 has ~* (read all keys) on shard {}, shard {}, and shard {}.'.format(SHARD_0, SHARD_1, SHARD_2))
    print('MRANGE is sent from coordinator shard {}. Expected: {} series (one per shard), each with {} points.'.format(
        COORD_SHARD_USER1, USER1_EXPECTED_SERIES_COUNT, POINTS_PER_SERIES))
    print('')
    expected_series_1 = sorted(key_per_shard)
    expected_data_1 = expected_reply(expected_series_1)
    print_mrange_format('user1', expected_data_1, 'expected')
    res1 = run_mrange(conns[COORD_SHARD_USER1], user1, pw1)
    print_mrange_format('user1', res1, 'result received')
    got_count_1, got_series_1 = len(res1), sorted(series_names(res1))
    env.assertEqual(got_count_1, USER1_EXPECTED_SERIES_COUNT,
                    message='user1: expected {} series {}, got {} {}'.format(
                        USER1_EXPECTED_SERIES_COUNT, expected_series_1, got_count_1, got_series_1))
    for series in res1:
        name = series[0] if isinstance(series[0], str) else series[0].decode()
        points = series[2]
        env.assertEqual(len(points), POINTS_PER_SERIES,
                        message='user1: each series should have {} points, {} has {}'.format(POINTS_PER_SERIES, name, len(points)))

    # ---------- Scenario 2: user2 (no permission on shard 2) ----------
    print('=== Scenario 2: user2 ===')
    print('Permissions: user2 has ~* on shard {} and shard {}; on shard {} has ~{} (no access to key).'.format(
        SHARD_0, SHARD_1, SHARD_2, NO_MATCH_KEY_PATTERN))
    print('MRANGE is sent from coordinator shard {}. Expected: NOPERM error (shard {} rejects the key).'.format(
        COORD_SHARD_USER2, SHARD_2))
    print('')
    with pytest.raises(redis.exceptions.NoPermissionError):
        run_mrange(conns[COORD_SHARD_USER2], user2, pw2)

    # ---------- Scenario 3: user3 (no permission on shards 0 and 1) ----------
    print('=== Scenario 3: user3 ===')
    print('Permissions: user3 has ~{} on shard {} and shard {} (no access to keys); ~* on shard {}.'.format(
        NO_MATCH_KEY_PATTERN, SHARD_0, SHARD_1, SHARD_2))
    print('MRANGE is sent from coordinator shard {}. Expected: NOPERM error (shards {} and {} reject the keys).'.format(
        COORD_SHARD_USER3, SHARD_0, SHARD_1))
    print('')
    with pytest.raises(redis.exceptions.NoPermissionError):
        run_mrange(conns[COORD_SHARD_USER3], user3, pw3)

    # ---------- Scenario 4: user4 (coordinator shard has no permission) ----------
    print('=== Scenario 4: user4 ===')
    print('Permissions: user4 has ~* on shard {} and shard {}; on coordinator shard {} has ~{} (no access to its key).'.format(
        SHARD_0, SHARD_2, SHARD_1, NO_MATCH_KEY_PATTERN))
    print('MRANGE is sent from coordinator shard {}. Expected: NOPERM error (coordinator rejects its own key).'.format(
        SHARD_1))
    print('')
    with pytest.raises(redis.exceptions.NoPermissionError):
        run_mrange(conns[SHARD_1], user4, pw4)

    # Cleanup
    for conn in conns:
        for u, p in [(user1, pw1), (user2, pw2), (user3, pw3), (user4, pw4)]:
            try:
                conn.execute_command('ACL', 'DELUSER', u)
            except Exception:
                pass
    with env.getClusterConnectionIfNeeded() as r:
        try:
            r.execute_command('AUTH', 'default', '')
        except Exception:
            pass
        for key in key_per_shard:
            try:
                r.delete(key)
            except Exception:
                pass


@skip(onVersionLowerThan='7.4.0')
def test_mrange_acl_coordinator_key_restriction(env):
    """The user sends the command directly to the COORDINATOR shard, and that
    user has a key restriction (can only access ~allowed_key*).  Both keys
    live on the coordinator's own shard.  The coordinator should return
    NOPERM for TS.MRANGE / TS.MREVRANGE / TS.MGET."""
    if not env.isCluster() or env.shardsCount < 2:
        env.skip()

    label = 'env=coord_acl_test'
    # Use hash tag {1} so both keys land on the same shard.
    allowed_key = 'allowed_key{1}'
    blocked_key = 'blocked_key{1}'

    # Find which shard owns the {1} slot — that shard will be our coordinator.
    shard0 = env.getConnection(0)
    slot = shard0.execute_command('CLUSTER', 'KEYSLOT', allowed_key)
    coord_shard = _shard_for_slot(env, slot)
    coord = env.getConnection(coord_shard)

    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', allowed_key, 'LABELS', 'env', 'coord_acl_test')
        r.execute_command('TS.CREATE', blocked_key, 'LABELS', 'env', 'coord_acl_test')
        r.execute_command('TS.ADD', allowed_key, 1, 100)
        r.execute_command('TS.ADD', blocked_key, 1, 200)

    # Create restricted user on the coordinator shard.
    coord.execute_command(
        'ACL', 'SETUSER', 'restricted_coord', 'on', '>pass',
        '+@all', '~allowed_key*'
    )

    # Send as restricted user from the coordinator shard.
    coord.execute_command('AUTH', 'restricted_coord', 'pass')
    try:
        with pytest.raises(redis.exceptions.NoPermissionError):
            coord.execute_command('TS.MRANGE', '-', '+', 'FILTER', label)

        with pytest.raises(redis.exceptions.NoPermissionError):
            coord.execute_command('TS.MREVRANGE', '-', '+', 'FILTER', label)

        with pytest.raises(redis.exceptions.NoPermissionError):
            coord.execute_command('TS.MGET', 'FILTER', label)
    finally:
        coord.execute_command('AUTH', 'default', '')
        coord.execute_command('ACL', 'DELUSER', 'restricted_coord')
        with env.getClusterConnectionIfNeeded() as r:
            r.delete(allowed_key)
            r.delete(blocked_key)


@skip(onVersionLowerThan='7.4.0')
def test_mrange_acl_participant_key_restriction(env):
    """Restricted key is on a PARTICIPANT shard (not the coordinator).
    The participant detects the NOPERM and replies with an error, which
    LibMR propagates back to the client. Expected: NOPERM error."""
    if not env.isCluster() or env.shardsCount < 2:
        env.skip()

    label = 'env=part_acl_test'
    allowed_key = 'allowed_key{1}'
    blocked_key = 'blocked_key{1}'

    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', allowed_key, 'LABELS', 'env', 'part_acl_test')
        r.execute_command('TS.CREATE', blocked_key, 'LABELS', 'env', 'part_acl_test')
        r.execute_command('TS.ADD', allowed_key, 1, 100)
        r.execute_command('TS.ADD', blocked_key, 1, 200)

    # Find which shard owns the {1} slot, then pick a DIFFERENT shard as coordinator.
    shard0 = env.getConnection(0)
    slot = shard0.execute_command('CLUSTER', 'KEYSLOT', allowed_key)
    key_shard = _shard_for_slot(env, slot)
    coord_shard = (key_shard + 1) % env.shardsCount
    coord = env.getConnection(coord_shard)

    # The user must have full permission on the coordinator shard (so
    # IsCurrentUserAllowedToReadAllTheKeys passes), but restricted on
    # the participant shard where the keys actually live.
    for i in range(env.shardsCount):
        conn = env.getConnection(i)
        conn.execute_command(
            'ACL', 'SETUSER', 'restricted_part', 'on', '>pass',
            '+@all', '~allowed_key*'
        )

    coord.execute_command('AUTH', 'restricted_part', 'pass')
    try:
        with pytest.raises(redis.exceptions.NoPermissionError):
            coord.execute_command('TS.MRANGE', '-', '+', 'FILTER', label)

        with pytest.raises(redis.exceptions.NoPermissionError):
            coord.execute_command('TS.MREVRANGE', '-', '+', 'FILTER', label)

        with pytest.raises(redis.exceptions.NoPermissionError):
            coord.execute_command('TS.MGET', 'FILTER', label)
    finally:
        coord.execute_command('AUTH', 'default', '')
        for i in range(env.shardsCount):
            try:
                env.getConnection(i).execute_command('ACL', 'DELUSER', 'restricted_part')
            except Exception:
                pass
        with env.getClusterConnectionIfNeeded() as r:
            r.delete(allowed_key)
            r.delete(blocked_key)


@skip(onVersionLowerThan='7.4.0')
def test_mget_acl_key_restriction(env):
    """TS.MGET with ACL key restriction should return NOPERM.
    Tests both coordinator and participant error paths."""
    if not env.isCluster() or env.shardsCount < 2:
        env.skip()

    label = 'env=mget_acl_test'
    allowed_key = 'mget_allowed{1}'
    blocked_key = 'mget_blocked{1}'

    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', allowed_key, 'LABELS', 'env', 'mget_acl_test')
        r.execute_command('TS.CREATE', blocked_key, 'LABELS', 'env', 'mget_acl_test')
        r.execute_command('TS.ADD', allowed_key, 1, 100)
        r.execute_command('TS.ADD', blocked_key, 1, 200)

    # Find which shard owns the {1} slot.
    shard0 = env.getConnection(0)
    slot = shard0.execute_command('CLUSTER', 'KEYSLOT', allowed_key)
    key_shard = _shard_for_slot(env, slot)

    # Create users on all shards.
    for i in range(env.shardsCount):
        env.getConnection(i).execute_command(
            'ACL', 'SETUSER', 'mget_restricted', 'on', '>pass',
            '+@all', '~mget_allowed*'
        )
        env.getConnection(i).execute_command(
            'ACL', 'SETUSER', 'mget_full', 'on', '>pass',
            '+@all', '~*'
        )

    # Scenario 0: user with full permission gets both keys.
    print('')
    print('=== Scenario 0: full permission user (mget_full) ===')
    print('Permissions: mget_full has ~* on all shards.')
    print('TS.MGET sent from shard {}. Expected: 2 series returned.'.format(key_shard))
    print('')
    coord_full = env.getConnection(key_shard)
    coord_full.execute_command('AUTH', 'mget_full', 'pass')
    try:
        res = coord_full.execute_command('TS.MGET', 'FILTER', label)
        print('Result: {} series returned'.format(len(res)))
        for s in res:
            name = s[0].decode() if isinstance(s[0], bytes) else s[0]
            print('  - {}: {}'.format(name, s))
        env.assertEqual(len(res), 2, message='mget_full: expected 2 series, got {}'.format(len(res)))
    finally:
        coord_full.execute_command('AUTH', 'default', '')

    # Scenario A: coordinator is the shard that owns the keys.
    print('')
    print('=== Scenario A: coordinator owns the keys (shard {}) ==='.format(key_shard))
    print('Permissions: mget_restricted has ~mget_allowed* (no access to {}).'.format(blocked_key))
    print('TS.MGET sent from coordinator shard {}. Expected: NOPERM error.'.format(key_shard))
    print('')
    coord_same = env.getConnection(key_shard)
    coord_same.execute_command('AUTH', 'mget_restricted', 'pass')
    try:
        with pytest.raises(redis.exceptions.NoPermissionError):
            coord_same.execute_command('TS.MGET', 'FILTER', label)
    finally:
        coord_same.execute_command('AUTH', 'default', '')

    # Scenario B: coordinator is a different shard (participant has the keys).
    other_shard = (key_shard + 1) % env.shardsCount
    print('=== Scenario B: participant owns the keys (shard {}), coordinator is shard {} ==='.format(key_shard, other_shard))
    print('Permissions: mget_restricted has ~mget_allowed* (no access to {}).'.format(blocked_key))
    print('TS.MGET sent from coordinator shard {}. Expected: NOPERM error via LibMR.'.format(other_shard))
    print('')
    coord_other = env.getConnection(other_shard)
    coord_other.execute_command('AUTH', 'mget_restricted', 'pass')
    try:
        with pytest.raises(redis.exceptions.NoPermissionError):
            coord_other.execute_command('TS.MGET', 'FILTER', label)
    finally:
        coord_other.execute_command('AUTH', 'default', '')

    # Cleanup
    for i in range(env.shardsCount):
        for u in ['mget_restricted', 'mget_full']:
            try:
                env.getConnection(i).execute_command('ACL', 'DELUSER', u)
            except Exception:
                pass
    with env.getClusterConnectionIfNeeded() as r:
        r.delete(allowed_key)
        r.delete(blocked_key)


@skip(onVersionLowerThan='8.0.0')
def test_libmr_internal_commands_is_not_allowed(env):
    env.expect('timeseries.INNERCOMMUNICATION').error().contains('unknown command')
    env.expect('timeseries.HELLO').error().contains('unknown command')
    env.expect('timeseries.CLUSTERSETFROMSHARD').error().contains('unknown command')
    env.expect('timeseries.INFOCLUSTER').error().contains('unknown command')
    env.expect('timeseries.NETWORKTEST').error().contains('unknown command')
    env.expect('timeseries.FORCESHARDSCONNECTION').error().contains('unknown command')

