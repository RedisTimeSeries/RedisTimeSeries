"""Exercise both distributed query protocols against real, separate shards.

These tests explicitly create a cluster even in the general test pass: selecting
three shards without env='oss-cluster' still creates standalone Redis in QUICK=1.
Small fixtures keep this contract test practical in both PR and nightly coverage.
Enterprise runs use the configured proxy and its real topology; deploy each
immutable LibMR protocol separately to cover both transports.
"""
import os
from contextlib import contextmanager

import redis

import pytest
from redis.exceptions import NoPermissionError

from includes import Env, RLEC_CLUSTER
from utils import slot_table


def _rows(reply, protocol, command):
    """Preserve labels and samples while normalizing the RESP2/RESP3 envelopes."""
    if protocol == 2:
        return {key: (dict(labels), samples) for key, labels, samples in reply}
    # MRANGE has an extra aggregators field in RESP3; MGET does not.
    result = {}
    for key, fields in reply.items():
        assert len(fields) == (3 if command == 'range' else 2)
        result[key] = (fields[0], fields[-1])
    return result


def _samples(samples):
    return [[row[0], *[float(v) for v in row[1:]]] for row in samples]


@contextmanager
def _connection(env, shard, protocol):
    # Existing Enterprise environments may ignore RLTest's protocol argument.
    # Use a fresh pool so RESP3 is really negotiated and AUTH cannot leak to the
    # administrator connection used by another check.
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


@contextmanager
def _data_connection(env, protocol):
    if RLEC_CLUSTER:
        with _connection(env, 1, protocol) as connection:
            yield connection
    else:
        with env.getClusterConnectionIfNeeded() as connection:
            yield connection


def _check_distributed_queries(protocol, transport):
    if RLEC_CLUSTER:
        # Keep the Enterprise runner's configured endpoint. An existing database
        # cannot change its immutable LibMR protocol: run its matching cases and
        # run the other two cases on a database started with the other protocol.
        env = Env(protocol=protocol)
        coordinators = (1,)  # the Enterprise proxy chooses the coordinator
        with _connection(env, 1, protocol) as admin:
            assert 'rlec_version' in admin.info('server'), 'Expected an Enterprise endpoint'
            config = admin.execute_command('CONFIG', 'GET', 'ts-libmr-protocol')
            actual = config[1] if protocol == 2 else config[b'ts-libmr-protocol']
            if actual.lower() != transport.encode():
                print(f'Enterprise database uses {actual.decode()}; {transport} requires a separate deployment')
                env.skip()
            topology = admin.execute_command('timeseries.INFOCLUSTER')
            assert isinstance(topology, list) and len(topology) == 5, topology
            nodes = [dict(zip(node[::2], node[1::2])) for node in topology[4]]
            assert len(nodes) >= 3, 'Distributed coverage requires at least three Enterprise shards'
            assert all(node[b'status'] == b'connected' for node in nodes), nodes
            slots = sorted(node[b'minHslot'] for node in nodes)[:3]
    else:
        env = Env(env='oss-cluster', shardsCount=3, protocol=protocol,
                  useSlaves=False, useAof=False,
                  redisConfigFileContent=f'ts-libmr-protocol {transport}\n',
                  clusterBusPortProtectedMode=os.getenv('CLUSTER_BUS_PROTECTED_MODE', 'no') or None)
        assert env.isCluster() and env.shardsCount == 3
        coordinators = range(1, 4)
        slots = (0, 8192, 16383)
    env.flush()
    for shard in coordinators:
        with _connection(env, shard, protocol) as admin:
            config = admin.execute_command('CONFIG', 'GET', 'ts-libmr-protocol')
            actual = config[1] if protocol == 2 else config[b'ts-libmr-protocol']
            assert actual.lower() == transport.encode(), (shard, config)
    keys = [f'sensor:{{{slot_table[slot]}}}'.encode() for slot in slots]
    empty = f'empty:{{{slot_table[slots[-1]]}}}'.encode()
    labels = [{b'metric': b'temp', b'site': b'shared', b'sensor': str(i).encode()}
              for i in range(3)]
    with _data_connection(env, protocol) as client:
        for i, key in enumerate(keys):
            encoding = 'UNCOMPRESSED' if i == 1 else 'COMPRESSED'
            client.execute_command('TS.CREATE', key, 'ENCODING', encoding,
                                   'CHUNK_SIZE', 48, 'LABELS',
                                   'metric', 'temp', 'site', 'shared', 'sensor', i)
            for timestamp in range(1, 13):
                client.execute_command('TS.ADD', key, timestamp, i * 10 + timestamp)
        client.execute_command('TS.CREATE', empty, 'LABELS', 'metric', 'empty')

    # Each OSS node coordinates queries. Enterprise exercises the proxy path.
    # In both cases the selected keys live on three distinct shards.
    for shard in coordinators:
        with _connection(env, shard, protocol) as coordinator:
            assert set(coordinator.execute_command('TS.QUERYINDEX', 'metric=temp')) == set(keys)
            assert set(coordinator.execute_command('TS.QUERYINDEX', 'metric=temp', 'sensor!=(1)')) == {keys[0], keys[2]}
            assert not coordinator.execute_command('TS.QUERYINDEX', 'metric=absent')
            assert set(coordinator.execute_command('TS.QUERYLABELS', 'LABELS', 'FILTER', 'metric=temp')) == {b'metric', b'site', b'sensor'}
            assert set(coordinator.execute_command('TS.QUERYLABELS', 'VALUES', 'sensor', 'FILTER', 'metric=temp')) == {b'0', b'1', b'2'}
            assert set(coordinator.execute_command('TS.QUERYLABELS', 'VALUES', 'site')) == {b'shared'}
            assert not coordinator.execute_command('TS.QUERYLABELS', 'VALUES', 'missing')

            for label_args in ([], ['WITHLABELS'], ['SELECTED_LABELS', 'sensor', 'missing']):
                result = _rows(coordinator.execute_command('TS.MGET', *label_args, 'FILTER', 'metric=temp'), protocol, 'get')
                assert set(result) == set(keys)
                for i, key in enumerate(keys):
                    expected_labels = labels[i] if label_args == ['WITHLABELS'] else (
                        {b'sensor': str(i).encode(), b'missing': None} if label_args else {})
                    assert result[key][0] == expected_labels
                    assert _samples([result[key][1]]) == [[12, i * 10 + 12.0]]
            result = _rows(coordinator.execute_command('TS.MGET', 'FILTER', 'metric=empty'), protocol, 'get')
            assert result == {empty: ({}, [])}
            assert not coordinator.execute_command('TS.MGET', 'FILTER', 'metric=absent')

            # Raw/reverse queries cross compressed and uncompressed chunk boundaries.
            for command in ('TS.MRANGE', 'TS.MREVRANGE'):
                result = _rows(coordinator.execute_command(command, 3, 10, 'WITHLABELS', 'FILTER', 'metric=temp'), protocol, 'range')
                assert set(result) == set(keys)
                timestamps = range(3, 11) if command == 'TS.MRANGE' else range(10, 2, -1)
                for i, key in enumerate(keys):
                    assert result[key][0] == labels[i]
                    assert _samples(result[key][1]) == [[t, i * 10 + float(t)] for t in timestamps]

            # The coordinator must not reapply FILTERBYVALUE to aggregated values:
            # each input is 2..4, but their sum (9) falls outside that interval.
            result = _rows(coordinator.execute_command(
                'TS.MRANGE', '-', '+', 'FILTER_BY_TS', 2, 3, 4,
                'FILTER_BY_VALUE', 2, 4, 'AGGREGATION', 'sum', 10,
                'SELECTED_LABELS', 'sensor', 'missing', 'FILTER', 'metric=temp'), protocol, 'range')
            assert _samples(result[keys[0]][1]) == [[0, 9.0]]
            assert result[keys[0]][0] == {b'sensor': b'0', b'missing': None}
            assert result[keys[1]][1] == result[keys[2]][1] == []

            result = _rows(coordinator.execute_command(
                'TS.MRANGE', '-', '+', 'FILTER_BY_VALUE', 100, 200,
                'EXCLUDEEMPTY', 'FILTER', 'metric=temp'), protocol, 'range')
            assert result == {}

            # Empty keys must not shift the multi-aggregation grouping of later keys.
            result = _rows(coordinator.execute_command(
                'TS.MREVRANGE', '-', '+', 'COUNT', 2,
                'AGGREGATION', 'min,max', 5, 'FILTER', 'metric=(temp,empty)'), protocol, 'range')
            assert set(result) == set(keys + [empty])
            assert result[empty][1] == []
            for i, key in enumerate(keys):
                assert _samples(result[key][1]) == [[10, i * 10 + 10.0, i * 10 + 12.0],
                                                   [5, i * 10 + 5.0, i * 10 + 9.0]]

            # One group spans every shard; reduce shard-local averages exactly once.
            grouped = coordinator.execute_command(
                'TS.MRANGE', '-', '+', 'AGGREGATION', 'avg', 5,
                'FILTER', 'metric=temp', 'GROUPBY', 'site', 'REDUCE', 'sum')
            if protocol == 2:
                assert len(grouped) == 1 and grouped[0][0] == b'site=shared'
                samples = grouped[0][-1]
            else:
                assert set(grouped) == {b'site=shared'}
                samples = grouped[b'site=shared'][-1]
            assert _samples(samples) == [[0, 37.5], [5, 51.0], [10, 63.0]]

    # LATEST must serialize an unfinished compaction bucket without mutating it.
    with _data_connection(env, protocol) as client:
        source = f'source:{{{slot_table[slots[-1]]}}}'
        dest = f'compact:{{{slot_table[slots[-1]]}}}'.encode()
        client.execute_command('TS.CREATE', source)
        client.execute_command('TS.CREATE', dest, 'LABELS', 'metric', 'compact')
        client.execute_command('TS.CREATERULE', source, dest, 'AGGREGATION', 'avg', 10)
        for t, v in ((1, 2), (2, 4), (11, 8), (12, 12)):
            client.execute_command('TS.ADD', source, t, v)
        with _connection(env, 1, protocol) as coordinator:
            latest = _rows(coordinator.execute_command('TS.MGET', 'LATEST', 'FILTER', 'metric=compact'), protocol, 'get')
            assert _samples([latest[dest][1]]) == [[10, 10.0]]
            latest = _rows(coordinator.execute_command('TS.MRANGE', '-', '+', 'LATEST', 'FILTER', 'metric=compact'), protocol, 'range')
            assert _samples(latest[dest][1]) == [[0, 3.0], [10, 10.0]]
        assert _samples(client.execute_command('TS.RANGE', dest, '-', '+')) == [[0, 3.0]]

    # Both transports reject the complete data query if any matching series is
    # unreadable. Metadata discovery retains its existing filtering semantics.
    commands = [('TS.MGET', 'FILTER', 'metric=temp')]
    for command in ('TS.MRANGE', 'TS.MREVRANGE'):
        commands.append((command, '-', '+', 'FILTER', 'metric=temp'))
        commands.append((command, '-', '+', 'FILTER', 'metric=temp',
                         'GROUPBY', 'site', 'REDUCE', 'sum'))
    try:
        for permissions, visible_sensors in ((b'~' + keys[0], {b'0'}), (b'~no-matching-key', set())):
            for shard in coordinators:
                with _connection(env, shard, protocol) as admin:
                    admin.execute_command('ACL', 'SETUSER', 'distributed_reader', 'on',
                                          '>test-pass', '+@all', 'resetkeys', permissions)
            # Exercise denial on the coordinator and on remote shards.
            for shard in coordinators:
                with _connection(env, shard, protocol) as restricted:
                    restricted.execute_command('AUTH', 'distributed_reader', 'test-pass')
                    assert set(restricted.execute_command(
                        'TS.QUERYLABELS', 'VALUES', 'sensor', 'FILTER', 'metric=temp')) == visible_sensors
                    assert set(restricted.execute_command('TS.QUERYINDEX', 'metric=temp')) == set(keys)
                    for command in commands:
                        with pytest.raises(NoPermissionError):
                            restricted.execute_command(*command)
            # An error must release worker locks and restore the context user.
            with _connection(env, 1, protocol) as admin:
                assert len(admin.execute_command('TS.MGET', 'FILTER', 'metric=temp')) == 3

        # Widen permissions and retry the same queries successfully.
        for shard in coordinators:
            with _connection(env, shard, protocol) as admin:
                admin.execute_command('ACL', 'SETUSER', 'distributed_reader', 'resetkeys', '~*')
        with _connection(env, 1, protocol) as allowed:
            allowed.execute_command('AUTH', 'distributed_reader', 'test-pass')
            for command in commands:
                result = allowed.execute_command(*command)
                assert len(result) == (1 if 'GROUPBY' in command else 3)
    finally:
        for shard in coordinators:
            with _connection(env, shard, protocol) as admin:
                admin.execute_command('ACL', 'DELUSER', 'distributed_reader')


def test_distributed_queries_internal_resp2():
    _check_distributed_queries(2, 'internal')


def test_distributed_queries_internal_resp3():
    _check_distributed_queries(3, 'internal')


def test_distributed_queries_gears_resp2():
    _check_distributed_queries(2, 'gears')


def test_distributed_queries_gears_resp3():
    _check_distributed_queries(3, 'gears')
