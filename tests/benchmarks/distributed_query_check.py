"""Check old/new coordinators and shards against a standalone baseline.

Requires redis-py. Supply --baseline, --candidate, --redis-server and a fresh --root.
Every cluster endpoint receives concurrent requests in both RESP2 and RESP3.
"""

import argparse
import concurrent.futures
import math
import pathlib

import redis

from coordinator_reply_bench import Cluster


def normalize(reply):
    rows = reply.items() if isinstance(reply, dict) else ((r[0], r[1:]) for r in reply)
    result = {}
    for key, row in rows:
        labels = dict(row[0])
        if b'__source__' in labels:
            labels[b'__source__'] = sorted(labels[b'__source__'].split(b','))
        samples = [[sample[0], [float(v) for v in sample[1:]]]
                   for sample in row[-1]]
        sources = sorted(row[2][b'sources']) if len(row) == 4 else None
        result[key] = (labels, sources, samples)
    return result


def equal(actual, expected):
    if isinstance(expected, float):
        return ((math.isnan(actual) and math.isnan(expected)) or
                math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12))
    if isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(equal(actual[k], v) for k, v in expected.items())
    if isinstance(expected, (list, tuple)):
        return len(actual) == len(expected) and all(equal(a, b) for a, b in zip(actual, expected))
    return actual == expected


def seed(cluster):
    cluster.seed(series=27, samples=12)
    # Unequal populations, empty series, missing group labels, NaNs, and overflow-safe AVG.
    for i in range(20):
        key = f'edge:{i}'
        conn = cluster.owner(key)
        labels = ['measurement', 'edge']
        if i != 19:
            labels += ['group', 'empty' if i == 18 else ('odd' if i % 2 else 'even')]
        conn.execute_command('TS.CREATE', key, 'LABELS', *labels)
        if i == 18:
            continue
        for ts, value in [(0, i), (5, 'nan'), (10, 1e308), (15, i / 7), (20, -1e308)]:
            if not (ts == 15 and i % 3 == 0):
                conn.execute_command('TS.ADD', key, ts, value)


def commands():
    for measurement, start, end in [('cpu', '-', '+'), ('edge', 0, 25)]:
        for command in ['TS.MRANGE', 'TS.MREVRANGE']:
            for options in [[], ['COUNT', 2], ['AGGREGATION', 'avg', 5, 'EMPTY'],
                            ['AGGREGATION', 'sum', 10], ['FILTER_BY_VALUE', -1, 10]]:
                for reducer in ['sum', 'min', 'max', 'avg', 'count', 'countnan', 'countall', 'range', 'std.p']:
                    yield [command, start, end, *options, 'WITHLABELS', 'FILTER',
                           f'measurement={measurement}', 'GROUPBY', 'group', 'REDUCE', reducer]
            for labels in [[], ['WITHLABELS'], ['SELECTED_LABELS', 'group', 'missing']]:
                for options in [[], ['COUNT', 2], ['AGGREGATION', 'min,max', 10]]:
                    yield [command, start, end, *labels, *options, 'FILTER', f'measurement={measurement}']
        yield ['TS.MRANGE', start, end, 'EXCLUDE_EMPTY', 'SELECTED_LABELS', 'group', '__source__',
               'FILTER', f'measurement={measurement}', 'GROUPBY', 'group', 'REDUCE', 'avg']


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--baseline', required=True)
    parser.add_argument('--candidate', required=True)
    parser.add_argument('--redis-server', required=True)
    parser.add_argument('--root', required=True)
    parser.add_argument('--base-port', type=int, default=24100)
    args = parser.parse_args()
    root = pathlib.Path(args.root)
    root.mkdir(parents=True, exist_ok=False)
    oracle = Cluster(args.baseline, 1, root / 'oracle', args.redis_server, args.base_port)
    try:
        seed(oracle)
        queries = list(commands())
        expected = {}
        for protocol in (2, 3):
            with redis.Redis(port=args.base_port, protocol=protocol) as conn:
                expected[protocol] = [normalize(conn.execute_command(*cmd)) for cmd in queries]
        for upgraded in (0, 1, 2, 3):
            modules = [args.candidate] * upgraded + [args.baseline] * (3 - upgraded)
            cluster = Cluster(modules, 3, root / f'upgraded{upgraded}', args.redis_server,
                              args.base_port + 10)
            try:
                seed(cluster)

                def check(port, protocol):
                    with redis.Redis(port=port, protocol=protocol, socket_timeout=30) as conn:
                        for cmd, want in zip(queries, expected[protocol]):
                            got = normalize(conn.execute_command(*cmd))
                            assert equal(got, want), (upgraded, port, protocol, cmd, got, want)

                with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
                    futures = [pool.submit(check, port, protocol)
                               for port in cluster.ports for protocol in (2, 3)]
                    for future in futures:
                        future.result()
                print(f'{upgraded}/3 upgraded: {len(queries) * 6} checks passed', flush=True)
            finally:
                cluster.close()
    finally:
        oracle.close()


if __name__ == '__main__':
    main()
