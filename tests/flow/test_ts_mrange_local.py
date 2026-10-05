import pytest
import redis

from includes import Env, is_rlec, skip
from utils import slot_table
from redis.crc import key_slot


def test_mrange_local_shard_isolation():
    env = Env(shardsCount=3, protocol=2)
    # This test needs connections to individual Redis processes. Enterprise proxy
    # routing must be tested separately with supported shard-targeted connections.
    if is_rlec() or not env.is_cluster():
        env.skip()

    keys = ['local-range:{%s}' % slot_table[s] for s in (0, 8192, 16383)]
    with env.getClusterConnectionIfNeeded() as writer:
        for value, key in enumerate(keys, 1):
            writer.execute_command('TS.ADD', key, 0, value,
                                   'LABELS', 'test', 'local-range', 'LOCAL', 'all')
            writer.execute_command('TS.ADD', key, 10, value * 10)

    expected_keys = {key.encode() for key in keys}
    for command in ('TS.MRANGE', 'TS.MREVRANGE'):
        seen = set()
        coverage = []
        for shard in range(1, env.shardsCount + 1):
            with env.getConnection(shard) as conn:
                global_rows = conn.execute_command(command, '-', '+',
                                                   'FILTER', 'test=local-range')
                assert {row[0] for row in global_rows} == expected_keys

                local_reply = conn.execute_command(command, '-', '+', 'lOcAl',
                                            'FILTER', 'test=local-range')
                assert local_reply[0] == 1
                ranges, rows = local_reply[1:]
                coverage.extend(ranges)
                local_keys = {row[0] for row in rows}
                for key in local_keys:
                    assert any(start <= key_slot(key) <= end for start, end in ranges)
                assert local_keys
                assert local_keys < expected_keys
                assert not seen.intersection(local_keys)
                seen.update(local_keys)
                expected_rows = [row for row in global_rows if row[0] in local_keys]
                assert sorted(rows) == sorted(expected_rows)

                grouped = conn.execute_command(
                    command, '-', '+', 'LOCAL', 'AGGREGATION', 'sum', 20,
                    'FILTER', 'test=local-range', 'GROUPBY', 'LOCAL', 'REDUCE', 'sum')
                expected_sum = sum(float(sample[1]) for row in rows for sample in row[2])
                assert grouped[0] == 1
                assert grouped[1] == ranges
                grouped = grouped[2]
                assert len(grouped) == 1
                assert grouped[0][0] == b'LOCAL=all'
                assert len(grouped[0][2]) == 1
                assert grouped[0][2][0][0] == 0
                assert float(grouped[0][2][0][1]) == expected_sum

                # A label named LOCAL must never change query routing.
                selected = conn.execute_command(
                    command, '-', '+', 'SELECTED_LABELS', 'LOCAL',
                    'FILTER', 'test=local-range')
                assert {row[0] for row in selected} == expected_keys
                grouped_all = conn.execute_command(
                    command, '-', '+', 'FILTER', 'test=local-range',
                    'GROUPBY', 'LOCAL', 'REDUCE', 'sum')
                assert sorted((ts, float(value)) for ts, value in grouped_all[0][2]) == [
                    (0, 6.0), (10, 60.0)]

                assert conn.execute_command(command, '-', '+', 'LOCAL',
                                            'FILTER', 'test=missing-local-range') == [1, ranges, []]
        assert seen == expected_keys
        next_slot = 0
        for start, end in sorted(coverage):
            assert start == next_slot
            next_slot = end + 1
        assert next_slot == 16384


@skip(on_cluster=True)
def test_mrange_local_options(env):
    with env.getConnection() as conn:
        conn.execute_command('TS.ADD', 'local-options', 0, 2,
                             'LABELS', 'test', 'local-options', 'LOCAL', 'all')
        conn.execute_command('TS.ADD', 'local-options', 10, 4)
        for command in ('TS.MRANGE', 'TS.MREVRANGE'):
            for options in (
                ['WITHLABELS'],
                ['SELECTED_LABELS', 'LOCAL'],
                ['FILTER_BY_TS', 0, 10, 'COUNT', 1],
                ['FILTER_BY_VALUE', 3, 5],
                ['AGGREGATION', 'avg', 20],
                ['EXCLUDEEMPTY'],
            ):
                suffix = options + ['FILTER', 'test=local-options']
                local_reply = conn.execute_command(command, '-', '+', 'LOCAL', *suffix)
                assert local_reply[:2] == [1, [[0, 16383]]]
                assert local_reply[2] == conn.execute_command(command, '-', '+', *suffix)

            for args in (
                ['-', '+', 'LOCAL'],
                ['-', '+', 'LOCAL', 'FILTER'],
                ['bad-timestamp', '+', 'LOCAL', 'FILTER', 'test=local-options'],
                ['-', '+', 'LOCAL', 'AGGREGATION', 'invalid', 20,
                 'FILTER', 'test=local-options'],
            ):
                with pytest.raises(redis.ResponseError):
                    conn.execute_command(command, *args)


@skip(on_cluster=True, onVersionLowerThan='7.4.0')
def test_mrange_local_acl(env):
    with env.getConnection() as conn:
        for key in ('local-allowed', 'local-denied'):
            conn.execute_command('TS.ADD', key, 0, 1, 'LABELS', 'test', 'local-acl')
        conn.execute_command('ACL', 'SETUSER', 'local-reader', 'reset', 'on',
                             '>local-password', '~local-allowed',
                             '+TS.MRANGE', '+TS.MREVRANGE')
        try:
            conn.execute_command('AUTH', 'local-reader', 'local-password')
            for command in ('TS.MRANGE', 'TS.MREVRANGE'):
                for grouping in ([], ['GROUPBY', 'test', 'REDUCE', 'sum']):
                    with pytest.raises(redis.exceptions.NoPermissionError):
                        conn.execute_command(command, '-', '+', 'LOCAL',
                                             'FILTER', 'test=local-acl', *grouping)
        finally:
            conn.execute_command('AUTH', 'default', '')
            conn.execute_command('ACL', 'DELUSER', 'local-reader')
