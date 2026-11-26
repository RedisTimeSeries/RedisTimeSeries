"""MOD-21473: align denial semantics for users present on every shard.

The same test user and key permissions are configured on every shard.
"""
import pytest
from redis.exceptions import NoPermissionError

import os
from contextlib import contextmanager

import redis
from includes import Env, RLEC_CLUSTER
from utils import slot_table


def _cluster(protocol, transport):
    # These focused cases create disposable OSS shards, not an Enterprise endpoint.
    if RLEC_CLUSTER:
        Env().skip()
    env = Env(env='oss-cluster', shardsCount=3, protocol=protocol,
              useSlaves=False, useAof=False,
              redisConfigFileContent=f'ts-libmr-protocol {transport}\n',
              clusterBusPortProtectedMode=os.getenv('CLUSTER_BUS_PROTECTED_MODE', 'no') or None)
    assert env.isCluster() and env.shardsCount == 3
    for shard in range(1, 4):
        with _connection(env, shard, protocol) as connection:
            config = connection.execute_command('CONFIG', 'GET', 'ts-libmr-protocol')
            actual = config[1] if protocol == 2 else config[b'ts-libmr-protocol']
            assert actual.lower() == transport.encode()
    return env


@contextmanager
def _connection(env, shard, protocol):
    original = env.getConnection(shard)
    options = dict(original.connection_pool.connection_kwargs, protocol=protocol,
                   decode_responses=False)
    pool = redis.ConnectionPool(connection_class=original.connection_pool.connection_class,
                                **options)
    original.close()
    try:
        with redis.Redis(connection_pool=pool) as connection:
            yield connection
    finally:
        pool.disconnect()


def _check_permissions(protocol, transport):
    env = _cluster(protocol, transport)
    keys = [f'acl:{{{slot_table[slot]}}}'.encode() for slot in (0, 8192, 16383)]
    with env.getClusterConnectionIfNeeded() as client:
        for i, key in enumerate(keys):
            client.execute_command('TS.CREATE', key, 'LABELS', 'metric', 'acl', 'sensor', i)
            client.execute_command('TS.ADD', key, 1, i + 1)
    commands = [('TS.MGET', 'FILTER', 'metric=acl')]
    for command in ('TS.MRANGE', 'TS.MREVRANGE'):
        commands.append((command, '-', '+', 'FILTER', 'metric=acl'))
        commands.append((command, '-', '+', 'FILTER', 'metric=acl',
                         'GROUPBY', 'metric', 'REDUCE', 'sum'))
    try:
        for permissions, visible in ((b'~' + keys[0], {b'0'}), (b'~no-match', set())):
            for shard in range(1, 4):
                with _connection(env, shard, protocol) as admin:
                    admin.execute_command('ACL', 'SETUSER', 'query_reader', 'on',
                                          '>test-pass', '+@all', 'resetkeys', permissions)
            for shard in range(1, 4):
                with _connection(env, shard, protocol) as restricted:
                    restricted.execute_command('AUTH', 'query_reader', 'test-pass')
                    for command in commands:
                        with pytest.raises(NoPermissionError):
                            restricted.execute_command(*command)
                    assert set(restricted.execute_command('TS.QUERYINDEX', 'metric=acl')) == set(keys)
                    assert set(restricted.execute_command(
                        'TS.QUERYLABELS', 'VALUES', 'sensor', 'FILTER', 'metric=acl')) == visible
            with _connection(env, 1, protocol) as admin:
                assert len(admin.execute_command('TS.MRANGE', '-', '+', 'FILTER', 'metric=acl')) == 3
        for shard in range(1, 4):
            with _connection(env, shard, protocol) as admin:
                admin.execute_command('ACL', 'SETUSER', 'query_reader', 'resetkeys', '~*')
        with _connection(env, 1, protocol) as allowed:
            allowed.execute_command('AUTH', 'query_reader', 'test-pass')
            for command in commands:
                # GEARS/RESP3 MGET success is independently fixed and covered by MOD-21471.
                # Denial above is checked for this combination as well.
                if command[0] == 'TS.MGET' and protocol == 3 and transport == 'gears':
                    continue
                assert len(allowed.execute_command(*command)) == (1 if 'GROUPBY' in command else 3)
    finally:
        for shard in range(1, 4):
            with _connection(env, shard, protocol) as admin:
                admin.execute_command('ACL', 'DELUSER', 'query_reader')


def test_permissions_gears_resp2():
    _check_permissions(2, 'gears')


def test_permissions_gears_resp3():
    _check_permissions(3, 'gears')


def test_permissions_internal_resp2():
    _check_permissions(2, 'internal')


def test_permissions_internal_resp3():
    _check_permissions(3, 'internal')
