import math
import time

import pytest
import redis
from RLTest import Env
import redis.exceptions
import redis.exceptions
import redis.exceptions
from includes import *


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
            result = conn1.execute_command('TS.QUERYINDEX', 'group=test')

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

def test_libmr_adds_acl_category(env):
    if not env.isCluster() or SANITIZER == 'address' or is_redis_version_lower_than(env, '7.4.0', env.isCluster()):
        env.skip()

    acl_category = '_timeseries_libmr_internal'
    env.expect('acl', 'cat').contains(acl_category.encode())

def do_test_libmr(env):
    if not env.isCluster() or SANITIZER == 'address' or is_redis_version_lower_than(env, '7.4.0', env.isCluster()):
        env.skip()

    for shard in range(0, env.shardsCount):
        conn = env.getConnection(shard)
        conn.execute_command('timeseries.REFRESHCLUSTER')
        # make sure cluster will not turn to failed state and we will not be
        # able to execute commands on shards, on slow envs, run with valgrind,
        # or mac, it is needed.
        env.broadcast('CONFIG', 'set', 'cluster-node-timeout', '120000')
        env.broadcast('timeseries.FORCESHARDSCONNECTION')
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
            '+ts.mrange', '-@_timeseries_libmr_internal', '~*',
        )

        r1.execute_command('AUTH', 'testuser4', 'password')

        env.assertEqual(len(r1.execute_command('TS.MRANGE - + FILTER metric=cpu')), 2)

def test_acl_libmr_username_with_password_in_config():
    redisConfigFile = '/tmp/redis.conf'
    if os.path.isfile(redisConfigFile):
        os.unlink(redisConfigFile)
    with open(redisConfigFile, 'w') as f:
        f.write('user tslibmr on >tslibmrpassword -@all +@_timeseries_libmr_internal\n')

    env = Env(redisConfigFile=redisConfigFile, moduleArgs='global-user tslibmr; global-password tslibmrpassword')

    do_test_libmr(env)

def test_acl_libmr_username_with_password_in_config_set_incorrectly(env):
    if not env.isCluster() or SANITIZER == 'address' or is_redis_version_lower_than(env, '7.4.0', env.isCluster()):
        env.skip()

    redisConfigFile = '/tmp/redis2.conf'
    if os.path.isfile(redisConfigFile):
        os.unlink(redisConfigFile)
    with open(redisConfigFile, 'w') as f:
        f.write('user tslibmr2 on >tslibmrpassword +@all -@_timeseries_libmr_internal\nuser default on >password +@all -@_timeseries_libmr_internal +timeseries.FORCESHARDSCONNECTION +timeseries.INFOCLUSTER\n')

    env = Env(password='password', redisConfigFile=redisConfigFile, moduleArgs='global-user nonexistinguseeeer; global-password nonexistinguserpasswd')

    # Should raise a timeout error, as we can't connect to the nodes
    # due to the user having no permissions to invoke the corresponding
    # commands.
    with pytest.raises(ShardConnectionTimeoutException):
        do_test_libmr(env)

def test_acl_libmr_username_with_password_in_config_set_to_disallow():
    redisConfigFile = '/tmp/redis3.conf'
    if os.path.isfile(redisConfigFile):
        os.unlink(redisConfigFile)
    with open(redisConfigFile, 'w') as f:
        f.write('user tslibmr3 on >tslibmrpassword +@all -@_timeseries_libmr_internal\n')

    env = Env(redisConfigFile=redisConfigFile, moduleArgs='global-user tslibmr3; global-password tslibmrpassword')

    # Should raise a timeout error, as we can't connect to the nodes
    # due to the user having no permissions to invoke the corresponding
    # commands.
    with pytest.raises(ShardConnectionTimeoutException):
        do_test_libmr(env)

