"""Filter a few series from a large label index, then aggregate with/without GROUPBY.

Requires Redis, memtier, redis-py and psutil. Every coordinator receives the same
fixed pool of queries concurrently; matches and samples/query do not grow with
shards. Background series contain one sample in small chunks to isolate index
cardinality within a local machine's memory budget. Selected series have full
history in normal chunks. This is not a production dataset/memory-capacity test.

Example:
  python selective_cardinality_bench.py --module /path/to/redistimeseries.so \
    --root /tmp/ts-selective --series 100000 1000000 --shards 2 4 8 --workers 1 4
"""

import argparse
import hashlib
import json
import math
import pathlib
import platform
import random
import shutil
import statistics
import subprocess
import time

import psutil
from redis.crc import key_slot

from coordinator_reply_bench import Cluster
from scaling_bottleneck_bench import run_load


def seed(cluster, args, total):
    # Stable eight-way placement keeps the same query pool and key names when
    # changing 2/4/8 shards. Every series has the same 16 label names.
    partitions = args.partitions
    tags = [next(str(i) for i in range(100000)
                 if p * 16384 // partitions <= key_slot(str(i).encode()) < (p + 1) * 16384 // partitions)
            for p in range(partitions)]
    def partition(index):
        # Rotate each cohort so single-match queries use every shard too.
        return (index + 5 * (index // args.cohort_size)) % partitions

    hot = args.query_pool * args.cohort_size
    keys = [f'selective:{{{tags[partition(i)]}}}:{i:09}' for i in range(hot)]
    before = psutil.swap_memory()._asdict()
    start = time.monotonic()
    for base in range(0, total, 2048):
        if psutil.virtual_memory().available < args.min_available_mb * 1024**2:
            raise MemoryError('Insufficient available memory while seeding; reduce --series')
        pipes = [c.pipeline(transaction=False) for c in cluster.connections]
        try:
            for i in range(base, min(base + 2048, total)):
                key = f'selective:{{{tags[partition(i)]}}}:{i:09}'
                shard = partition(i) * len(pipes) // partitions
                labels = ['measurement', 'cpu', 'fieldname', 'usage_nice',
                          'hostname', f'h{i // args.cohort_size}',
                          'ordinal', f'o{i % args.cohort_size}', 'group', f'g{i % 4}']
                for j in range(11):
                    labels += [f'tag{j}', f'value-{j}-abcdefghijklmnopqrstuv']
                pipes[shard].execute_command('TS.CREATE', key, 'CHUNK_SIZE',
                                             4096 if i < hot else 128, 'LABELS', *labels)
                pipes[shard].execute_command('TS.ADD', key, 0, i / 3.0)
            for pipe in pipes:
                pipe.execute()
        finally:
            for pipe in pipes:
                pipe.close()
        if base % (2048 * 50) == 0:
            print(json.dumps({'seeding_series': min(base + 2048, total), 'total': total}), flush=True)
    for i, key in enumerate(keys):
        conn = cluster.connections[partition(i) * len(cluster.connections) // partitions]
        for start_sample in range(1, args.samples, 1024):
            with conn.pipeline(transaction=False) as pipe:
                for sample in range(start_sample, min(start_sample + 1024, args.samples)):
                    pipe.execute_command('TS.ADD', key, sample * 1000, (i + sample) / 3.0)
                pipe.execute()
    counts = [c.dbsize() for c in cluster.connections]
    assert counts == [total // len(counts)] * len(counts), counts
    selected_per_shard = {}
    for matches in args.matches:
        owners = [0] * len(counts)
        for cohort in range(args.query_pool):
            for key in keys[cohort * args.cohort_size:cohort * args.cohort_size + matches]:
                owners[key_slot(key.encode()) * len(counts) // 16384] += 1
        assert owners == [args.query_pool * matches // len(counts)] * len(counts), owners
        selected_per_shard[matches] = owners
    return keys, {'seconds': time.monotonic() - start, 'series_per_shard': counts,
                  'query_pool_matches_per_shard': selected_per_shard,
                  'hot_series': hot, 'background_samples_per_series': 1,
                  'selected_samples_per_series': args.samples,
                  'used_memory_bytes': sum(c.info('memory')['used_memory'] for c in cluster.connections),
                  'host_available_bytes': psutil.virtual_memory().available,
                  'host_swap_before': before, 'host_swap_after': psutil.swap_memory()._asdict()}


def make_cases(args, shards):
    cases = {}
    for matches in args.matches:
        for count in args.ranges:
            for grouped in (False, True):
                name = f'm{matches}_t{count}_' + ('group' if grouped else 'plain')
                pool = []
                for cohort in range(args.query_pool):
                    command = (f'TS.MRANGE {(args.samples-count)*1000} {args.samples*1000-1}'
                               f' AGGREGATION avg 60000 FILTER measurement=cpu fieldname=usage_nice hostname=h{cohort}')
                    if matches < args.cohort_size:
                        command += ' ordinal=(' + ','.join(f'o{i}' for i in range(matches)) + ')'
                    if grouped:
                        command += ' GROUPBY group REDUCE avg'
                    pool.append(command)
                # Offset the order without changing the queries or their weights.
                commands = [pool[i * len(pool) // shards:] + pool[:i * len(pool) // shards]
                            for i in range(shards)]
                cases[name] = {'matches': matches, 'range_samples': count, 'grouped': grouped,
                               'pool': pool, 'commands': commands}
    return cases


def verify(cluster, args, keys, cases):
    checks = 0
    for name, case in cases.items():
        for conn in cluster.connections:
            for cohort, query in enumerate(case['pool']):
                reply = conn.execute_command(*query.split())
                actual = {row[0].decode(): row[2] for row in reply}
                ids = list(range(cohort * args.cohort_size,
                                 cohort * args.cohort_size + case['matches']))
                expected = ({f'group=g{g}': [i for i in ids if i % 4 == g]
                             for g in range(min(4, case['matches']))} if case['grouped'] else
                            {keys[i]: [i] for i in ids})
                assert len(reply) == len(expected), (name, cohort, len(reply), len(expected))
                assert set(actual) == set(expected), (name, cohort, set(actual), set(expected))
                for key, members in expected.items():
                    samples = actual[key]
                    timestamps = list(range((args.samples-case['range_samples'])*1000,
                                            args.samples*1000, 60000))
                    assert [t for t, _ in samples] == timestamps, (name, key)
                    for timestamp, value in samples:
                        want = (statistics.mean(members) + timestamp / 1000 + 29.5) / 3
                        assert math.isclose(float(value), want, rel_tol=1e-11), (name, key, timestamp, value, want)
                checks += 1
    return checks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--module', required=True)
    parser.add_argument('--root', required=True)
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--redis-server', default=shutil.which('redis-server'))
    parser.add_argument('--memtier', default=shutil.which('memtier_benchmark'))
    parser.add_argument('--series', nargs='+', type=int, default=[100000, 1000000])
    parser.add_argument('--shards', nargs='+', type=int, default=[2, 4, 8])
    parser.add_argument('--workers', nargs='+', type=int, default=[1, 4])
    parser.add_argument('--matches', nargs='+', type=int, default=[1, 8, 32])
    parser.add_argument('--ranges', nargs='+', type=int, default=[60, 6000])
    parser.add_argument('--samples', type=int, default=6000)
    parser.add_argument('--query-pool', type=int, default=16)
    parser.add_argument('--cohort-size', type=int, default=32)
    parser.add_argument('--partitions', type=int, default=8)
    parser.add_argument('--clients', type=int, default=48)
    parser.add_argument('--pipeline', type=int, default=2)
    parser.add_argument('--seconds', type=int, default=5)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--base-port', type=int, default=25200)
    parser.add_argument('--min-available-mb', type=int, default=1024)
    parser.add_argument('--profile-case', help='Capture separate macOS stacks for this case after timings')
    args = parser.parse_args()
    if not args.redis_server or not args.memtier:
        parser.error('Redis and memtier are required')
    numbers = args.series + args.shards + args.workers + args.matches + args.ranges + [
        args.samples, args.query_pool, args.cohort_size, args.partitions, args.clients,
        args.pipeline, args.seconds, args.repeats, args.min_available_mb]
    if min(numbers) < 1 or min(args.shards) < 2:
        parser.error('parameters must be positive; use at least two shards')
    if args.samples % 60 or any(r % 60 or r > args.samples for r in args.ranges):
        parser.error('samples/ranges must be multiples of 60, ranges <= samples')
    if args.cohort_size % 4 or max(args.matches) > args.cohort_size:
        parser.error('cohort size must be divisible by four and cover all match counts')
    if any(args.partitions % s or args.clients % s or args.query_pool % s for s in args.shards):
        parser.error('partitions, clients and query pool must be divisible by shard counts')
    if args.cohort_size % args.partitions or args.query_pool % args.partitions or math.gcd(5, args.partitions) != 1:
        parser.error('balanced placement requires cohort size and query pool divisible by partitions, and partitions coprime to five')
    if any(n % args.partitions or n < args.query_pool * args.cohort_size for n in args.series):
        parser.error('series must cover the hot query pool and be divisible by partitions')
    if not 1024 <= args.base_port <= 65535 - 10000 - max(args.shards):
        parser.error('invalid port range')
    if args.profile_case and (platform.system() != 'Darwin' or args.profile_case not in make_cases(args, args.shards[0])):
        parser.error('profile case must name a selected case on macOS')
    root = pathlib.Path(args.root).resolve()
    root.mkdir(parents=True, exist_ok=args.resume)
    module = pathlib.Path(args.module).resolve()
    fingerprint = hashlib.sha256(module.read_bytes()).hexdigest()
    evidence = {'method': vars(args), 'module_sha256': fingerprint,
                'platform': platform.platform(), 'logical_cpus': psutil.cpu_count(),
                'redis_version': subprocess.check_output([args.redis_server, '--version'], text=True).strip(),
                'limitations': ['Shared local host; short runs are diagnostic, not production capacity.',
                                'All keys are populated; background series have one sample in 128-byte chunks.',
                                'Selected series have full history in 4096-byte chunks; the hot query pool is fixed and cache-friendly.',
                                'CPU is CPU-seconds/elapsed, stack shares are not CPU utilization.',
                                'Client CPU is sampled; P99 is the maximum endpoint P99, not a global percentile.'],
                'configurations': []}
    if args.resume:
        evidence = json.loads((root / 'evidence.json').read_text())
        if evidence['module_sha256'] != fingerprint:
            parser.error('resume module changed')
        if any(k != 'resume' and evidence['method'].get(k) != v for k, v in vars(args).items()):
            parser.error('resume parameters changed')
        (root / f'evidence-before-resume-{time.time_ns()}.json').write_text(json.dumps(evidence, indent=2) + '\n')
        evidence['configurations'] = [c for c in evidence['configurations'] if c.get('complete')]
    completed = {(c['series'], c['shards'], c['workers']) for c in evidence['configurations']}
    configurations = [(total, shards, workers) for total in args.series
                      for shards in args.shards for workers in args.workers]
    random.Random(184104).shuffle(configurations)
    for total, shards, workers in configurations:
        if (total, shards, workers) in completed:
            continue
        cfgroot = root / f'n{total}-s{shards}-w{workers}'
        if cfgroot.exists():
            cfgroot = root / f'n{total}-s{shards}-w{workers}-retry-{time.time_ns()}'
        cluster = Cluster(str(module), shards, cfgroot, args.redis_server, args.base_port, workers)
        try:
            keys, seed_info = seed(cluster, args, total)
            cases = make_cases(args, shards)
            cfg = {'series': total, 'shards': shards, 'workers': workers,
                   'root': str(cfgroot), 'seed': seed_info,
                   'analytic_reply_checks': verify(cluster, args, keys, cases),
                   'cases': cases, 'runs': [], 'summary': {}, 'complete': False}
            evidence['configurations'].append(cfg)
            names = list(cases)
            random.Random(shards * 100 + workers).shuffle(names)
            for name in names:
                run_load(cluster, cases[name]['commands'], args, name + '-warmup', 2)
                for repeat in range(args.repeats):
                    row = run_load(cluster, cases[name]['commands'], args, f'{name}-{repeat}', args.seconds)
                    cfg['runs'].append(row)
                    print(json.dumps({'series': total, 'shards': shards, 'workers': workers,
                                      'case': name, 'repeat': repeat, 'qps': round(row['ops_per_second']),
                                      'main_max': round(row['max_main_cores'], 2),
                                      'worker_cores': round(row['worker_cores'], 2),
                                      'host_cores': round(row['host_busy_cores'], 2)}), flush=True)
                rows = [r for r in cfg['runs'] if r['case'] == name]
                cfg['summary'][name] = {k: statistics.median(r[k] for r in rows) for k in [
                    'ops_per_second', 'max_main_cores', 'max_event_loop_cores', 'worker_cores',
                    'host_busy_cores', 'redis_cores', 'client_cores_sampled', 'cpu_us_per_request',
                    'max_endpoint_p99_ms']}
                cfg['summary'][name].update(qps_min=min(r['ops_per_second'] for r in rows),
                                             qps_max=max(r['ops_per_second'] for r in rows))
                (root / 'evidence.json').write_text(json.dumps(evidence, indent=2) + '\n')
            if args.profile_case:
                cfg['profile_run'] = run_load(cluster, cases[args.profile_case]['commands'], args,
                                              args.profile_case + '-profile', 6, profile=True)
            cfg['complete'] = True
            cfg['host_swap_after_tests'] = psutil.swap_memory()._asdict()
            (root / 'evidence.json').write_text(json.dumps(evidence, indent=2) + '\n')
        finally:
            cluster.close()
    print(root / 'evidence.json', flush=True)


if __name__ == '__main__':
    main()
