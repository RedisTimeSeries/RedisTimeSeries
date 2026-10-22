"""An RDB reload must preserve each kind of unfinished aggregation context."""
import math

from includes import Env


def test_compaction_contexts_survive_reload():
    env = Env()
    env.skipOnAOF()  # This test specifically checks the RDB context encoding.
    source = 'checkpoint:source{checkpoint}'
    expected = {
        'min': 2.0, 'max': 8.0, 'sum': 14.0, 'count': 3.0,
        'first': 2.0, 'last': 8.0, 'range': 6.0, 'avg': 14.0 / 3,
        'var.p': 56.0 / 9, 'var.s': 28.0 / 3,
        'std.p': math.sqrt(56.0 / 9), 'std.s': math.sqrt(28.0 / 3),
        # Linear interpolation: areas 6 (0..2), 24 (2..6), 40 (6..10).
        'twa': 7.0,
    }
    destinations = {agg: f'checkpoint:{agg}{{checkpoint}}' for agg in expected}
    with env.getClusterConnectionIfNeeded() as client:
        client.execute_command('TS.CREATE', source)
        for agg, dest in destinations.items():
            client.execute_command('TS.CREATE', dest)
            client.execute_command('TS.CREATERULE', source, dest, 'AGGREGATION', agg, 10)
        for timestamp, value in ((0, 2), (2, 4)):
            client.execute_command('TS.ADD', source, timestamp, value)

        # LATEST finalizes a clone. Repeated reads must leave the live context intact.
        before = {agg: client.execute_command('TS.GET', dest, 'LATEST')
                  for agg, dest in destinations.items()}
        for agg, dest in destinations.items():
            assert client.execute_command('TS.GET', dest, 'LATEST') == before[agg]
            assert client.execute_command('TS.RANGE', dest, '-', '+') == []

        env.dumpAndReload()
        for agg, dest in destinations.items():
            assert client.execute_command('TS.GET', dest, 'LATEST') == before[agg], agg
            assert client.execute_command('TS.RANGE', dest, '-', '+') == []

        # Continue the same bucket after reload, then close it with a future sample.
        client.execute_command('TS.ADD', source, 6, 8)
        client.execute_command('TS.ADD', source, 10, 12)
        for agg, dest in destinations.items():
            samples = client.execute_command('TS.RANGE', dest, '-', '+')
            assert len(samples) == 1 and samples[0][0] == 0, (agg, samples)
            assert math.isclose(float(samples[0][1]), expected[agg], rel_tol=1e-9), (agg, samples)
