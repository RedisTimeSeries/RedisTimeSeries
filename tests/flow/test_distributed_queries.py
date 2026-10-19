"""Exercise both distributed query protocols against real, separate shards.

These tests explicitly create a cluster even in the general test pass: selecting
three shards without env='oss-cluster' still creates standalone Redis in QUICK=1.
Small fixtures keep this contract test practical in both PR and nightly coverage.
"""
import os

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


def _check_distributed_queries(protocol, transport):
    if RLEC_CLUSTER:
        Env().skip()
    env = Env(env='oss-cluster', shardsCount=3, protocol=protocol,
              useSlaves=False, useAof=False,
              redisConfigFileContent=f'ts-libmr-protocol {transport}\n',
              clusterBusPortProtectedMode=os.getenv('CLUSTER_BUS_PROTECTED_MODE', 'no') or None)
    assert env.isCluster() and env.shardsCount == 3
    env.flush()
    for shard in range(1, 4):
        with env.getConnection(shard) as admin:
            config = admin.execute_command('CONFIG', 'GET', 'ts-libmr-protocol')
            actual = config[1] if protocol == 2 else config[b'ts-libmr-protocol']
            assert actual.lower() == transport.encode(), (shard, config)
    keys = [f'sensor:{{{slot_table[slot]}}}'.encode() for slot in (0, 8192, 16383)]
    empty = f'empty:{{{slot_table[16383]}}}'.encode()
    labels = [{b'metric': b'temp', b'site': b'shared', b'sensor': str(i).encode()}
              for i in range(3)]
    with env.getClusterConnectionIfNeeded() as client:
        for i, key in enumerate(keys):
            encoding = 'UNCOMPRESSED' if i == 1 else 'COMPRESSED'
            client.execute_command('TS.CREATE', key, 'ENCODING', encoding,
                                   'CHUNK_SIZE', 48, 'LABELS',
                                   'metric', 'temp', 'site', 'shared', 'sensor', i)
            for timestamp in range(1, 13):
                client.execute_command('TS.ADD', key, timestamp, i * 10 + timestamp)
        client.execute_command('TS.CREATE', empty, 'LABELS', 'metric', 'empty')

    # Each node must coordinate queries, including results from the other two nodes.
    for shard in range(1, 4):
        with env.getConnection(shard) as coordinator:
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
    with env.getClusterConnectionIfNeeded() as client:
        source = f'source:{{{slot_table[16383]}}}'
        dest = f'compact:{{{slot_table[16383]}}}'.encode()
        client.execute_command('TS.CREATE', source)
        client.execute_command('TS.CREATE', dest, 'LABELS', 'metric', 'compact')
        client.execute_command('TS.CREATERULE', source, dest, 'AGGREGATION', 'avg', 10)
        for t, v in ((1, 2), (2, 4), (11, 8), (12, 12)):
            client.execute_command('TS.ADD', source, t, v)
        with env.getConnection(1) as coordinator:
            latest = _rows(coordinator.execute_command('TS.MGET', 'LATEST', 'FILTER', 'metric=compact'), protocol, 'get')
            assert _samples([latest[dest][1]]) == [[10, 10.0]]
            latest = _rows(coordinator.execute_command('TS.MRANGE', '-', '+', 'LATEST', 'FILTER', 'metric=compact'), protocol, 'range')
            assert _samples(latest[dest][1]) == [[0, 3.0], [10, 10.0]]
        assert _samples(client.execute_command('TS.RANGE', dest, '-', '+')) == [[0, 3.0]]

    # Read permissions must reach remote shards. Label discovery omits unreadable
    # keys. Internal commands reject a partially unreadable result; the legacy
    # gears protocol omits unreadable series. A later admin request
    # must still work after the distributed error path releases its resources.
    for shard in range(1, 4):
        with env.getConnection(shard) as admin:
            admin.execute_command('ACL', 'SETUSER', 'distributed_reader', 'on',
                                  '>test-pass', '+@all', b'~' + keys[0])
    try:
        with env.getConnection(1) as restricted:
            restricted.execute_command('AUTH', 'distributed_reader', 'test-pass')
            assert set(restricted.execute_command(
                'TS.QUERYLABELS', 'VALUES', 'sensor', 'FILTER', 'metric=temp')) == {b'0'}
            for command in (('TS.MGET', 'FILTER', 'metric=temp'),
                            ('TS.MRANGE', '-', '+', 'FILTER', 'metric=temp')):
                if transport == 'internal':
                    with pytest.raises(NoPermissionError):
                        restricted.execute_command(*command)
                else:
                    kind = 'get' if command[0] == 'TS.MGET' else 'range'
                    result = _rows(restricted.execute_command(*command), protocol, kind)
                    assert set(result) == {keys[0]}
        with env.getConnection(1) as admin:
            assert len(admin.execute_command('TS.MGET', 'FILTER', 'metric=temp')) == 3
    finally:
        for shard in range(1, 4):
            with env.getConnection(shard) as admin:
                admin.execute_command('ACL', 'DELUSER', 'distributed_reader')


def test_distributed_queries_internal_resp2():
    _check_distributed_queries(2, 'internal')


def test_distributed_queries_internal_resp3():
    _check_distributed_queries(3, 'internal')


def test_distributed_queries_gears_resp2():
    _check_distributed_queries(2, 'gears')


def test_distributed_queries_gears_resp3():
    _check_distributed_queries(3, 'gears')
