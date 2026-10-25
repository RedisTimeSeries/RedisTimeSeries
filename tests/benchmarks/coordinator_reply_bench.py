"""Compare distributed query reply overhead using disposable local Redis processes.

Requires redis-py and memtier_benchmark. Run once per module build with a fresh --root:
  python coordinator_reply_bench.py --module /path/to/redistimeseries.so --root /tmp/ts-before

The dataset and total client concurrency stay fixed as --shards changes. By default,
clients target every shard simultaneously; --coordinators single isolates one coordinator.
All processes run on one machine. Do not compare these numbers with distributed hardware.
"""

import argparse
import json
import math
import pathlib
import random
import shutil
import subprocess
import time

import thread_cpu

import redis
from redis.crc import key_slot


def equivalent(a, b):
    if a == b:
        return True
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        return len(a) == len(b) and all(equivalent(x, y) for x, y in zip(a, b))
    try:
        return math.isclose(float(a), float(b), rel_tol=1e-12, abs_tol=1e-12)
    except (TypeError, ValueError):
        return False


class Cluster:
    def __init__(self, module, n, root, redis_server, base_port=23100, workers=3):
        self.root = pathlib.Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.processes = []
        self.connections = []
        self.ports = list(range(base_port, base_port + n))
        try:
            for i, port in enumerate(self.ports):
                node_module = module[i] if isinstance(module, (list, tuple)) else module
                node = self.root / str(i)
                node.mkdir()
                config = [f'port {port}', 'bind 127.0.0.1', 'protected-mode no',
                          'save ""', 'appendonly no', f'dir {json.dumps(str(node))}',
                          f'logfile {json.dumps(str(node / "redis.log"))}', 'enable-debug-command yes',
                          f'loadmodule {json.dumps(str(pathlib.Path(node_module).resolve()))}',
                          'ts-libmr-protocol internal', f'ts-num-threads {workers}']
                if n > 1:
                    config += ['cluster-enabled yes', 'cluster-node-timeout 5000',
                               'cluster-config-file nodes.conf']
                (node / 'redis.conf').write_text('\n'.join(config) + '\n')
                proc = subprocess.Popen([redis_server, str(node / 'redis.conf')],
                                        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
                self.processes.append(proc)
                conn = redis.Redis(port=port, socket_timeout=20)
                self.connections.append(conn)
                self.wait(lambda: conn.ping(), proc)
                # Never configure an unrelated server if a requested port was already in use.
                if conn.info('server')['process_id'] != proc.pid:
                    raise RuntimeError(f'Port {port} belongs to another Redis process')
            if n > 1:
                for i, conn in enumerate(self.connections):
                    conn.execute_command('CLUSTER', 'ADDSLOTSRANGE', i * 16384 // n,
                                         (i + 1) * 16384 // n - 1)
                    if i:
                        self.connections[0].execute_command('CLUSTER', 'MEET', '127.0.0.1', self.ports[i])
                self.wait(lambda: all(c.cluster('info')['cluster_state'] == 'ok'
                                      for c in self.connections))
                for conn in self.connections:
                    conn.execute_command('timeseries.REFRESHCLUSTER')
                    conn.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
                    conn.execute_command('timeseries.FORCESHARDSCONNECTION')
                self.wait(lambda: all(self.connected(c, n) for c in self.connections))
        except BaseException:
            self.close()
            raise

    @staticmethod
    def wait(fn, process=None):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if process and process.poll() is not None:
                raise RuntimeError('Redis exited; inspect node logs')
            try:
                if fn():
                    return
            except (redis.ConnectionError, redis.ResponseError):
                pass
            time.sleep(0.05)
        raise TimeoutError('Cluster did not become ready')

    @staticmethod
    def connected(conn, n):
        conn.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
        data = conn.execute_command('timeseries.INFOCLUSTER')
        return len(data[4]) == n and all(node[-1] == b'connected' for node in data[4])

    def owner(self, key):
        slot = key_slot(key.encode())
        n = len(self.connections)
        return next(c for i, c in enumerate(self.connections)
                    if i * 16384 // n <= slot < (i + 1) * 16384 // n)

    def seed(self, series=96, samples=60):
        pipes = {id(c): c.pipeline(transaction=False) for c in self.connections}
        for i in range(series):
            key = f'bench:series:{i}'
            pipe = pipes[id(self.owner(key))]
            labels = ['measurement', 'cpu', 'fieldname', 'usage_nice', 'host', f'h{i}',
                      'group', 'all']
            for j in range(12):
                labels += [f'tag{j}', f'value-{j}-abcdefghijklmnopqrstuv']
            pipe.execute_command('TS.CREATE', key, 'LABELS', *labels)
            for j in range(samples):
                pipe.execute_command('TS.ADD', key, 6000000 + j * 1000, (i + j) / 3.0)
        for pipe in pipes.values():
            pipe.execute()

    def metrics(self):
        stats = [c.info() for c in self.connections]
        return {'cpu': sum(s['used_cpu_user'] + s['used_cpu_sys'] for s in stats),
                'output': sum(s['total_net_output_bytes'] for s in stats),
                'shards': [{'cpu': s['used_cpu_user'] + s['used_cpu_sys'],
                            'main_cpu': (s['used_cpu_user_main_thread'] + s['used_cpu_sys_main_thread']
                                         if 'used_cpu_user_main_thread' in s else None),
                            'memory_used': s['used_memory'], 'memory_peak': s['used_memory_peak'],
                            'memory_rss': s['used_memory_rss'],
                            'threads': thread_cpu.snapshot(p.pid)}
                           for p, s in zip(self.processes, stats)]}

    def close(self):
        for proc in self.processes:
            if proc.poll() is None:
                proc.terminate()
        for proc in self.processes:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        for conn in self.connections:
            conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--module', required=True)
    parser.add_argument('--root', required=True)
    parser.add_argument('--redis-server', default=shutil.which('redis-server'))
    parser.add_argument('--memtier', default=shutil.which('memtier_benchmark'))
    parser.add_argument('--base-port', type=int, default=23100)
    parser.add_argument('--shards', type=int, default=3)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--seconds', type=int, default=3)
    parser.add_argument('--coordinators', choices=['all', 'single'], default='all')
    parser.add_argument('--clients', type=int, default=48, help='Total connections across coordinators')
    parser.add_argument('--workers', type=int, default=3)
    parser.add_argument('--series', type=int, default=96)
    parser.add_argument('--pipeline', type=int, default=2)
    parser.add_argument('--cases', nargs='+', choices=['narrow_avg', 'narrow_balanced', 'broad_avg', 'multi_avg_sum',
                                                     'group_avg', 'mget', 'raw'])
    args = parser.parse_args()
    if not args.redis_server or not args.memtier:
        parser.error('redis-server and memtier_benchmark must be installed or supplied explicitly')
    if min(args.shards, args.repeats, args.seconds, args.clients, args.workers, args.series, args.pipeline) < 1:
        parser.error('numeric workload parameters must be positive')
    coordinators = args.shards if args.coordinators == 'all' else 1
    if args.clients % coordinators:
        parser.error('clients must be divisible by the number of coordinators')
    if args.base_port < 1024 or args.base_port + args.shards + 10000 > 65535:
        parser.error('base-port must leave room for all client and cluster-bus ports')
    if not pathlib.Path(args.module).is_file():
        parser.error('module must be an existing file')
    cluster = Cluster(args.module, args.shards, args.root, args.redis_server, args.base_port, args.workers)
    try:
        cluster.seed(series=args.series)
        (cluster.root / 'workload.json').write_text(json.dumps(vars(args), indent=2))
        cases = {
            'narrow_avg': 'TS.MRANGE 6000000 6059999 AGGREGATION avg 60000 FILTER measurement=cpu fieldname=usage_nice host=h1',
            'narrow_balanced': 'TS.MRANGE 6000000 6059999 AGGREGATION avg 60000 FILTER measurement=cpu fieldname=usage_nice host={host}',
            'broad_avg': 'TS.MRANGE 6000000 6059999 AGGREGATION avg 60000 FILTER measurement=cpu',
            'multi_avg_sum': 'TS.MRANGE 6000000 6059999 AGGREGATION avg,sum 60000 FILTER measurement=cpu',
            'group_avg': 'TS.MRANGE 6000000 6059999 AGGREGATION avg 60000 FILTER measurement=cpu GROUPBY group REDUCE avg',
            'mget': 'TS.MGET FILTER measurement=cpu',
            'raw': 'TS.MRANGE 6000000 6059999 FILTER measurement=cpu',
        }
        if args.cases:
            cases = {k: cases[k] for k in args.cases}
        hosts = [next((f'h{i}' for i in range(args.series)
                       if cluster.owner(f'bench:series:{i}') is conn), None)
                 for conn in cluster.connections]
        if 'narrow_balanced' in cases and any(host is None for host in hosts):
            raise RuntimeError('Increase --series so every shard owns a series')
        commands = {name: [command.format(host=host) for host in hosts]
                    for name, command in cases.items()}
        # Verify every query before measuring, and preserve replies for before/after comparison.
        replies = {name: [c.execute_command(*command.split())
                         for c, command in zip(cluster.connections, node_commands)]
                   for name, node_commands in commands.items()}
        for name, results in replies.items():
            canonical = [sorted(reply, key=lambda row: row[0]) for reply in results]
            if name != 'narrow_balanced' and any(not equivalent(reply, canonical[0])
                                                for reply in canonical[1:]):
                raise RuntimeError(f'Coordinators disagree on {name}')
        (cluster.root / 'replies.json').write_text(json.dumps(replies, default=lambda b: b.decode()))
        runs = [(name, command, repeat) for repeat in range(args.repeats)
                for name, command in cases.items()]
        random.Random(1).shuffle(runs)
        rows = []
        for name, command, repeat in runs:
            before = cluster.metrics()
            clients, logs, result_paths = [], [], []
            try:
                # Start every load generator before waiting for any of them.
                for i in range(coordinators):
                    stem = cluster.root / f'{name}-{repeat}-coordinator{i}'
                    result_paths.append(stem.with_suffix('.json'))
                    log = stem.with_suffix('.log').open('w')
                    logs.append(log)
                    clients.append(subprocess.Popen(
                        [args.memtier, '-s', '127.0.0.1', '-p', str(cluster.ports[i]),
                         '-t', '1', '-c', str(args.clients // coordinators),
                         '--pipeline', str(args.pipeline), '--test-time', str(args.seconds),
                         '--command', commands[name][i], '--hide-histogram',
                         '--json-out-file', str(result_paths[-1])], stdout=log, stderr=subprocess.STDOUT))
                for proc in clients:
                    if proc.wait(timeout=args.seconds + 30):
                        raise RuntimeError(f'memtier failed for {name}; inspect coordinator logs')
            finally:
                for proc in clients:
                    if proc.poll() is None:
                        proc.kill()
                    proc.wait()
                for log in logs:
                    log.close()
            after = cluster.metrics()
            measurements = [json.loads(path.read_text())['ALL STATS'] for path in result_paths]
            if any('error' in path.with_suffix('.log').read_text().lower() for path in result_paths):
                raise RuntimeError(f'memtier reported errors for {name}; inspect coordinator logs')
            counts = [m['Totals']['Count'] for m in measurements]
            if min(counts) == 0:
                raise RuntimeError(f'No completed requests on a coordinator for {name}')
            start = min(m['Runtime']['Start time'] for m in measurements)
            finish = max(m['Runtime']['Finish time'] for m in measurements)
            overlap = min(m['Runtime']['Finish time'] for m in measurements) - max(
                m['Runtime']['Start time'] for m in measurements)
            if overlap < args.seconds * 1000 * 0.8:
                raise RuntimeError('Load generators did not overlap sufficiently')
            totals = {'Count': sum(counts), 'Ops/sec': sum(counts) * 1000 / (finish - start)}
            row = {'case': name, 'repeat': repeat, 'shards': args.shards,
                   'coordinators': coordinators, 'clients': args.clients, 'workers': args.workers,
                   'overlap_ms': overlap, 'per_coordinator': [m['Totals'] for m in measurements],
                   'commands': commands[name][:coordinators],
                   'totals': totals, 'cpu_seconds': after['cpu'] - before['cpu'],
                   'output_bytes': after['output'] - before['output'],
                   'per_shard': [{'cpu_seconds': a['cpu'] - b['cpu'],
                                  'main_cpu_seconds': (a['main_cpu'] - b['main_cpu']
                                                       if a['main_cpu'] is not None else None),
                                  'memory_used_before': b['memory_used'], 'memory_used_after': a['memory_used'],
                                  'memory_peak': a['memory_peak'],
                                  'memory_rss_before': b['memory_rss'], 'memory_rss_after': a['memory_rss'],
                                  'threads': thread_cpu.delta(b['threads'], a['threads'])}
                                 for b, a in zip(before['shards'], after['shards'])]}
            rows.append(row)
            (cluster.root / 'summary.json').write_text(json.dumps(rows, indent=2))
            print(f"{name}: {totals['Ops/sec']:.0f} ops/sec, "
                  f"{row['cpu_seconds'] / totals['Count'] * 1e6:.1f} CPU us/request", flush=True)
    finally:
        cluster.close()


if __name__ == '__main__':
    main()
