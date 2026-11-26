"""MOD-21471: merge distributed MGET replies in the client's RESP version."""
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


def _rows(reply, protocol):
    if protocol == 2:
        assert isinstance(reply, list)
        return {key: (dict(labels), sample) for key, labels, sample in reply}
    assert isinstance(reply, dict)
    assert all(len(fields) == 2 for fields in reply.values())
    return {key: (fields[0], fields[1]) for key, fields in reply.items()}


def _check_mget(protocol, transport):
    env = _cluster(protocol, transport)
    keys = [f'sensor:{{{slot_table[slot]}}}'.encode() for slot in (0, 8192, 16383)]
    empty = f'empty:{{{slot_table[16383]}}}'.encode()
    source = f'source:{{{slot_table[16383]}}}'.encode()
    dest = f'compact:{{{slot_table[16383]}}}'.encode()
    with env.getClusterConnectionIfNeeded() as client:
        for i, key in enumerate(keys):
            client.execute_command('TS.CREATE', key, 'LABELS', 'metric', 'temp', 'sensor', i)
            client.execute_command('TS.ADD', key, 12, i + 1)
        client.execute_command('TS.CREATE', empty, 'LABELS', 'metric', 'empty')
        client.execute_command('TS.CREATE', source)
        client.execute_command('TS.CREATE', dest, 'LABELS', 'metric', 'compact')
        client.execute_command('TS.CREATERULE', source, dest, 'AGGREGATION', 'avg', 10)
        for timestamp, value in ((1, 2), (2, 4), (11, 8), (12, 12)):
            client.execute_command('TS.ADD', source, timestamp, value)
    for shard in range(1, 4):
        with _connection(env, shard, protocol) as coordinator:
            for label_args in ([], ['WITHLABELS'], ['SELECTED_LABELS', 'sensor', 'missing']):
                result = _rows(coordinator.execute_command(
                    'TS.MGET', *label_args, 'FILTER', 'metric=temp'), protocol)
                assert set(result) == set(keys)
                for i, key in enumerate(keys):
                    expected = ({b'metric': b'temp', b'sensor': str(i).encode()}
                                if label_args == ['WITHLABELS'] else
                                {b'sensor': str(i).encode(), b'missing': None} if label_args else {})
                    assert result[key][0] == expected
                    assert result[key][1][0] == 12 and float(result[key][1][1]) == i + 1
            assert _rows(coordinator.execute_command(
                'TS.MGET', 'FILTER', 'metric=empty'), protocol) == {empty: ({}, [])}
            assert _rows(coordinator.execute_command(
                'TS.MGET', 'FILTER', 'metric=absent'), protocol) == {}
            latest = _rows(coordinator.execute_command(
                'TS.MGET', 'LATEST', 'FILTER', 'metric=compact'), protocol)
            assert set(latest) == {dest}
            assert latest[dest][1][0] == 10 and float(latest[dest][1][1]) == 10.0
    with env.getClusterConnectionIfNeeded() as client:
        rows = client.execute_command('TS.RANGE', dest, '-', '+')
        assert [[t, float(v)] for t, v in rows] == [[0, 3.0]]


def test_mget_gears_resp2():
    _check_mget(2, 'gears')


def test_mget_gears_resp3():
    _check_mget(3, 'gears')


def test_mget_internal_resp2():
    _check_mget(2, 'internal')


def test_mget_internal_resp3():
    _check_mget(3, 'internal')
