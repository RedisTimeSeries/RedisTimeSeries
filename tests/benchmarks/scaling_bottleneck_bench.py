"""Measure shard/worker scaling on one machine, including competing CPU demand.

Requires redis-py, psutil, Redis and memtier_benchmark. All coordinators receive
traffic concurrently. The total dataset and number of clients stay fixed.
Use a fresh --root. JSON preserves every repetition and per-thread CPU measurement.
CPU occupancy is measured CPU seconds / elapsed seconds, not a stack sample ratio.
On macOS, --profile-case also saves separate diagnostic stacks after timed runs.

Example (use longer repetitions on a dedicated host):
  python scaling_bottleneck_bench.py --module /path/to/redistimeseries.so \
    --root /tmp/ts-scaling --shards 2 4 8 --workers 1 2 4 --seconds 30 --repeats 5
An interrupted run can resume with the same arguments plus --resume. Its completed
configurations are retained, and incomplete attempts remain available in their
original directories. Profile captures are separate from the timed repetitions.
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
import threading
import time

import psutil
from redis.crc import key_slot

from coordinator_reply_bench import Cluster
import thread_cpu


def busy_seconds(cpu):
    # Linux includes guest time in user/nice already.
    return sum(value for field, value in cpu._asdict().items()
               if field not in ('idle', 'iowait', 'guest', 'guest_nice'))


def seed_balanced(cluster, series, samples):
    n = len(cluster.connections)
    hosts = []
    for shard, conn in enumerate(cluster.connections):
        tag = next(str(i) for i in range(100000)
                   if shard * 16384 // n <= key_slot(str(i).encode()) < (shard + 1) * 16384 // n)
        first = shard * series // n
        hosts.append(f'h{first}')
        # Bound client-side memory while loading larger datasets.
        for index in range(first, (shard + 1) * series // n):
            key = f'bench:{{{tag}}}:series:{index}'
            labels = ['measurement', 'cpu', 'fieldname', 'usage_nice',
                      'host', f'h{index}', 'group', 'all']
            for j in range(12):
                labels += [f'tag{j}', f'value-{j}-abcdefghijklmnopqrstuv']
            conn.execute_command('TS.CREATE', key, 'LABELS', *labels)
            for start in range(0, samples, 1000):
                with conn.pipeline(transaction=False) as pipe:
                    for j in range(start, min(samples, start + 1000)):
                        pipe.execute_command('TS.ADD', key, j * 1000, (index + j) / 3.0)
                    pipe.execute()
    assert [c.dbsize() for c in cluster.connections] == [series // n] * n
    return hosts


def queries(hosts, samples):
    full = f'TS.MRANGE 0 {samples * 1000 - 1} AGGREGATION avg 60000 FILTER measurement=cpu'
    short = f'TS.MRANGE {(samples - 60) * 1000} {samples * 1000 - 1} AGGREGATION avg 60000 FILTER measurement=cpu'
    return {
        'heavy': [full + ' GROUPBY group REDUCE avg'] * len(hosts),
        'selective': [short + f' host={h}' for h in hosts],
        'empty': [short + ' host=absent'] * len(hosts),
    }


def verify(cluster, cases, series, samples):
    # Check against analytic expected values, including weighted group averages.
    for name, commands in cases.items():
        for conn, command in zip(cluster.connections, commands):
            reply = conn.execute_command(*command.split())
            if name == 'empty':
                assert reply == [], reply
                continue
            assert len(reply) == 1, (name, len(reply))
            data = reply[0][2]
            if name == 'heavy':
                assert len(data) == math.ceil(samples / 60)
                for bucket, value in data:
                    start = bucket // 1000
                    end = min(start + 59, samples - 1)
                    expected = ((series - 1) / 2 + (start + end) / 2) / 3
                    assert math.isclose(float(value), expected, rel_tol=1e-12), (bucket, value)
            else:
                assert len(data) == 1
                host = int(command.rsplit('=h', 1)[1])
                expected = (host + samples - 30.5) / 3
                assert math.isclose(float(data[0][1]), expected, rel_tol=1e-12)


def run_load(cluster, commands, args, stem, seconds, profile=False):
    before = cluster.metrics()
    host_before = psutil.cpu_times(percpu=True)
    cpu_before = time.monotonic()
    processes, logs, paths, profilers = [], [], [], []
    client_cpu = {}
    stop = threading.Event()
    monitor = None
    host_samples = []

    def observe():
        while not stop.wait(0.2):
            host_samples.append({'time': time.monotonic(),
                                 'busy_seconds': [busy_seconds(c) for c in psutil.cpu_times(percpu=True)]})
            for proc in processes:
                try:
                    cpu = psutil.Process(proc.pid).cpu_times()
                    client_cpu[proc.pid] = cpu.user + cpu.system
                except psutil.NoSuchProcess:
                    pass

    try:
        for i, (port, command) in enumerate(zip(cluster.ports, commands)):
            path = cluster.root / f'{stem}-coordinator{i}.json'
            paths.append(path)
            log = path.with_suffix('.log').open('w')
            logs.append(log)
            processes.append(subprocess.Popen([
                args.memtier, '-s', '127.0.0.1', '-p', str(port), '-t', '1',
                '-c', str(args.clients // len(cluster.ports)), '--pipeline', str(args.pipeline),
                '--test-time', str(seconds), '--command', command, '--hide-histogram',
                '--json-out-file', str(path)], stdout=log, stderr=subprocess.STDOUT))
        monitor = threading.Thread(target=observe, daemon=True)
        monitor.start()
        if profile:
            time.sleep(1)
            for i, proc in enumerate(cluster.processes):
                log = (cluster.root / f'{stem}-sample{i}.log').open('w')
                logs.append(log)
                profilers.append(subprocess.Popen([
                    '/usr/bin/sample', str(proc.pid), '3', '2', '-file',
                    str(cluster.root / f'{stem}-shard{i}.sample.txt')],
                    stdout=log, stderr=subprocess.STDOUT))
        for proc in processes + profilers:
            if proc.wait(timeout=seconds + 30):
                raise RuntimeError(f'Load/profile process failed: {stem}')
    finally:
        stop.set()
        if monitor:
            monitor.join()
        for proc in processes + profilers:
            if proc.poll() is None:
                proc.kill()
            proc.wait()
        for log in logs:
            log.close()
    elapsed = time.monotonic() - cpu_before
    host_after = psutil.cpu_times(percpu=True)
    after = cluster.metrics()
    endpoints = [json.loads(p.read_text())['ALL STATS'] for p in paths]
    for p in paths:
        if 'error' in p.with_suffix('.log').read_text().lower():
            raise RuntimeError(f'Load generator reported an error: {p}')
    overlap = min(s['Runtime']['Finish time'] for s in endpoints) - max(
        s['Runtime']['Start time'] for s in endpoints)
    # Startup skew in an unmeasured warm-up must not invalidate subsequent
    # steady-state runs; measured runs retain the strict 80% overlap guard.
    minimum_overlap = 1 if stem.endswith('-warmup') else seconds * 800
    assert overlap >= minimum_overlap, (stem, overlap)
    counts = [s['Totals']['Count'] for s in endpoints]
    assert min(counts) > 0
    duration = (max(s['Runtime']['Finish time'] for s in endpoints) -
                min(s['Runtime']['Start time'] for s in endpoints)) / 1000
    count = sum(counts)
    shards = []
    for old, new in zip(before['shards'], after['shards']):
        threads = thread_cpu.delta(old['threads'], new['threads'])
        for t in threads:
            t['cores'] = t['cpu_seconds'] / elapsed
            t['role'] = ('worker' if t['name'].startswith('timeseries-') and t['name'] != 'timeseries-el'
                         else 'event_loop' if t['name'] == 'timeseries-el'
                         else 'main' if t['name'] == '' or t['name'].startswith('redis-server')
                         else 'background')
        assert sum(t['role'] == 'main' for t in threads) == 1, threads
        shards.append({'threads': threads, 'memory_rss': new['memory_rss'],
                       'memory_used': new['memory_used'], 'memory_peak': new['memory_peak']})
    host_cores = sum(busy_seconds(b) - busy_seconds(a) for a, b in zip(host_before, host_after)) / elapsed
    redis_cores = (after['cpu'] - before['cpu']) / elapsed
    client_cores = sum(client_cpu.values()) / elapsed
    return {'case': stem.split('-')[0], 'count': count, 'ops_per_second': count / duration,
            'duration_seconds': duration, 'cpu_interval_seconds': elapsed, 'overlap_ms': overlap,
            'redis_cores': redis_cores, 'host_busy_cores': host_cores,
            'host_logical_cpus': len(host_before), 'host_cpu_samples': host_samples,
            'client_cores_sampled': client_cores,
            'client_max_cores_sampled': max(client_cpu.values(), default=0) / elapsed,
            'other_cores_estimate': max(0, host_cores - redis_cores - client_cores),
            'cpu_us_per_request': (after['cpu'] - before['cpu']) * 1e6 / count,
            'max_main_cores': max(t['cores'] for s in shards for t in s['threads'] if t['role'] == 'main'),
            'max_event_loop_cores': max((t['cores'] for s in shards for t in s['threads'] if t['role'] == 'event_loop'), default=0),
            'worker_cores': sum(t['cores'] for s in shards for t in s['threads'] if t['role'] == 'worker'),
            'max_endpoint_p99_ms': max(s['Totals']['Percentile Latencies']['p99.00'] for s in endpoints),
            'per_coordinator': [s['Totals'] for s in endpoints], 'shards': shards}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--module', required=True)
    parser.add_argument('--root', required=True)
    parser.add_argument('--resume', action='store_true', help='Keep completed configurations from an interrupted run')
    parser.add_argument('--redis-server', default=shutil.which('redis-server'))
    parser.add_argument('--memtier', default=shutil.which('memtier_benchmark'))
    parser.add_argument('--shards', nargs='+', type=int, default=[2, 4, 8])
    parser.add_argument('--workers', nargs='+', type=int, default=[1, 2, 4])
    parser.add_argument('--series', type=int, default=96)
    parser.add_argument('--samples', type=int, default=6000)
    parser.add_argument('--clients', type=int, default=48)
    parser.add_argument('--pipeline', type=int, default=2)
    parser.add_argument('--seconds', type=int, default=5)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--base-port', type=int, default=25100)
    parser.add_argument('--cases', nargs='+', choices=['heavy', 'selective', 'empty'], default=['heavy', 'selective', 'empty'])
    parser.add_argument('--profile-case', choices=['heavy', 'selective', 'empty'])
    args = parser.parse_args()
    if not args.redis_server or not args.memtier:
        parser.error('Redis and memtier are required')
    if min(args.shards + args.workers + [args.series, args.samples, args.clients, args.pipeline, args.seconds, args.repeats]) < 1:
        parser.error('numeric parameters must be positive')
    if min(args.shards) < 2 or args.samples < 60 or args.samples % 60:
        parser.error('use at least two shards and a positive multiple of 60 samples')
    if any(args.clients % n or args.series % n for n in args.shards):
        parser.error('total clients and series must be divisible by every shard count')
    if args.base_port < 1024 or args.base_port + max(args.shards) + 10000 > 65535:
        parser.error('invalid port range')
    if args.profile_case and platform.system() != 'Darwin':
        parser.error('stack capture uses macOS sample; CPU measurements also support Linux')
    root = pathlib.Path(args.root).resolve()
    root.mkdir(parents=True, exist_ok=args.resume)
    module = pathlib.Path(args.module).resolve()
    evidence = {'method': vars(args), 'platform': platform.platform(),
                'logical_cpus': psutil.cpu_count(), 'physical_cpus': psutil.cpu_count(logical=False),
                'memory_bytes': psutil.virtual_memory().total,
                'module_sha256': hashlib.sha256(module.read_bytes()).hexdigest(),
                'redis_version': subprocess.check_output([args.redis_server, '--version'], text=True).strip(),
                'limitations': ['Shared host; CPU/core frequency, memory bandwidth and competing work can limit scaling.',
                                'Read-only fixed-size dataset; no production-capacity or unbounded-scaling claim.',
                                'Client CPU is sampled and excludes up to 200 ms at process exit.',
                                'P99 is the slowest endpoint percentile, not a merged global percentile.'],
                'configurations': []}
    if args.resume:
        saved = json.loads((root / 'evidence.json').read_text())
        for key, value in vars(args).items():
            if key != 'resume' and saved['method'].get(key) != value:
                parser.error(f'resume parameter changed: {key}')
        if saved['module_sha256'] != evidence['module_sha256']:
            parser.error('resume module binary changed')
        evidence = saved
        evidence['configurations'] = [c for c in saved['configurations']
                                      if set(c['summary']) == set(args.cases)
                                      and (not args.profile_case or c.get('profile_run'))]
    completed = {(c['shards'], c['workers']) for c in evidence['configurations']}
    configs = [(s, w) for s in args.shards for w in args.workers]
    random.Random(184104).shuffle(configs)
    for n, workers in configs:
        if (n, workers) in completed:
            continue
        config_root = root / f's{n}-w{workers}'
        if config_root.exists():
            config_root = root / f's{n}-w{workers}-retry-{time.time_ns()}'
        cluster = Cluster(str(module), n, config_root, args.redis_server, args.base_port, workers)
        try:
            hosts = seed_balanced(cluster, args.series, args.samples)
            cases = queries(hosts, args.samples)
            verify(cluster, cases, args.series, args.samples)
            cfg = {'shards': n, 'workers': workers, 'series_per_shard': args.series // n, 'root': str(config_root),
                   'runs': [], 'summary': {}}
            evidence['configurations'].append(cfg)
            names = list(args.cases)
            random.Random(n * 100 + workers).shuffle(names)
            for case in names:
                run_load(cluster, cases[case], args, case + '-warmup', 2)
                for repeat in range(args.repeats):
                    row = run_load(cluster, cases[case], args, f'{case}-{repeat}', args.seconds)
                    cfg['runs'].append(row)
                    print(json.dumps({'shards': n, 'workers': workers, 'case': case,
                                      'repeat': repeat, 'qps': round(row['ops_per_second']),
                                      'host_cores': round(row['host_busy_cores'], 2),
                                      'redis_cores': round(row['redis_cores'], 2),
                                      'main_max': round(row['max_main_cores'], 2),
                                      'event_loop_max': round(row['max_event_loop_cores'], 2)}), flush=True)
                rows = [r for r in cfg['runs'] if r['case'] == case]
                cfg['summary'][case] = {k: statistics.median(r[k] for r in rows) for k in
                    ['ops_per_second', 'redis_cores', 'host_busy_cores', 'other_cores_estimate',
                     'client_cores_sampled', 'client_max_cores_sampled', 'max_main_cores',
                     'max_event_loop_cores', 'worker_cores', 'max_endpoint_p99_ms', 'cpu_us_per_request']}
                cfg['summary'][case]['qps_min'] = min(r['ops_per_second'] for r in rows)
                cfg['summary'][case]['qps_max'] = max(r['ops_per_second'] for r in rows)
                (root / 'evidence.json').write_text(json.dumps(evidence, indent=2) + '\n')
            if args.profile_case:
                cfg['profile_run'] = run_load(cluster, cases[args.profile_case], args,
                                              args.profile_case + '-profile', 6, profile=True)
                (root / 'evidence.json').write_text(json.dumps(evidence, indent=2) + '\n')
        finally:
            cluster.close()
    print(root / 'evidence.json', flush=True)


if __name__ == '__main__':
    main()
