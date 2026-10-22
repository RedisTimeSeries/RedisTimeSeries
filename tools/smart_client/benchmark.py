"""Read-only benchmark against an existing, quiescent OSS TimeSeries dataset."""

import argparse
import asyncio
import itertools
import json
import math
import os
import time

from client import LocalMRClient, merge_series


async def measure(call, queries, concurrency):
    counter = itertools.count()
    latencies = []

    async def worker():
        while next(counter) < queries:
            start = time.perf_counter()
            await call()
            latencies.append(time.perf_counter() - start)

    start = time.perf_counter()
    await LocalMRClient._all([worker() for _ in range(concurrency)])
    elapsed = time.perf_counter() - start
    latencies.sort()
    return {
        'completed_logical_queries': len(latencies),
        'seconds': elapsed,
        'logical_queries_per_second': len(latencies) / elapsed,
        'p50_ms': latencies[math.ceil(len(latencies) * .50) - 1] * 1000,
        'p99_ms': latencies[math.ceil(len(latencies) * .99) - 1] * 1000,
    }


async def run(args):
    options = {}
    if os.environ.get('REDIS_USERNAME'):
        options['username'] = os.environ['REDIS_USERNAME']
    if os.environ.get('REDIS_PASSWORD'):
        options['password'] = os.environ['REDIS_PASSWORD']
    if args.tls:
        options['ssl'] = True

    async with await LocalMRClient.connect(
            args.host, args.port, timeout=args.timeout, max_inflight=args.concurrency,
            check_topology=not args.static_topology, **options) as client:
        kwargs = dict(filters=args.filters, count=args.count,
                      aggregation=args.aggregation, bucket=args.bucket)
        command = ['TS.MRANGE', args.start, args.end]
        if args.count is not None:
            command += ['COUNT', args.count]
        if args.aggregation is not None:
            command += ['AGGREGATION', args.aggregation, args.bucket]
        command += ['FILTER', *args.filters]

        async def server():
            return merge_series([await client.seed.execute_command(*command)])

        async def smart():
            return await client.mrange(args.start, args.end, **kwargs)

        expected = await server()
        actual = await smart()
        if not expected:
            raise RuntimeError('No matching series; load representative data before benchmarking')
        if actual != expected:
            raise RuntimeError('Parity check failed; use a quiescent dataset with finite values')
        for _ in range(args.warmup):
            await server()
            await smart()

        print(json.dumps({'parity': 'passed', 'primaries': len(client._nodes),
                          'series_per_query': len(expected),
                          'samples_per_query': sum(len(row.samples) for row in expected),
                          'concurrency': args.concurrency,
                          'topology_checks_per_smart_query': 0 if args.static_topology else 2}))
        for name, call in [('server_coordinator', server), ('client_coordinator', smart)]:
            stats = await measure(call, args.queries, args.concurrency)
            print(json.dumps(dict(mode=name, **stats)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=6379)
    parser.add_argument('--tls', action='store_true')
    parser.add_argument('--start', default='-')
    parser.add_argument('--end', default='+')
    parser.add_argument('--filter', dest='filters', action='append', required=True)
    parser.add_argument('--count', type=int)
    parser.add_argument('--aggregation')
    parser.add_argument('--bucket', type=int)
    parser.add_argument('--queries', type=int, default=1000)
    parser.add_argument('--concurrency', type=int, default=32)
    parser.add_argument('--timeout', type=float, default=5.0)
    parser.add_argument('--warmup', type=int, default=20)
    parser.add_argument('--static-topology', action='store_true',
                        help='Disable per-query slot checks; isolated stable-topology benchmark only')
    args = parser.parse_args()
    if args.queries < 1 or args.concurrency < 1 or args.timeout <= 0 or args.warmup < 0:
        parser.error('queries/concurrency/timeout must be positive; warmup must be nonnegative')
    if (args.aggregation is None) != (args.bucket is None):
        parser.error('--aggregation and --bucket must be provided together')
    asyncio.run(run(args))


if __name__ == '__main__':
    main()
