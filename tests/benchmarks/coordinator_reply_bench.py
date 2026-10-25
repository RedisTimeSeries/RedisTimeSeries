"""Compare distributed query reply overhead using disposable local Redis processes.

Requires redis-py and memtier_benchmark. Run once per module build with a fresh --root:
  python coordinator_reply_bench.py --module /path/to/redistimeseries.so --root /tmp/ts-before

The dataset and client concurrency stay fixed as --shards changes. Clients target one
coordinator deliberately; this measures coordinator overhead, not production scale-out.
All processes run on one machine. Do not compare these numbers with distributed hardware.
"""

import argparse
import json
import pathlib
import random
import shutil
import subprocess
import time

import redis
from redis.crc import key_slot


class Cluster:
    def __init__(self, module, n, root, redis_server, base_port=23100):
        self.root = pathlib.Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.processes = []
        self.connections = []
        self.ports = list(range(base_port, base_port + n))
        try:
            for i, port in enumerate(self.ports):
                node = self.root / str(i)
                node.mkdir()
                config = [f'port {port}', 'bind 127.0.0.1', 'protected-mode no',
                          'save ""', 'appendonly no', f'dir {json.dumps(str(node))}',
                          f'logfile {json.dumps(str(node / "redis.log"))}', 'enable-debug-command yes',
                          f'loadmodule {json.dumps(str(pathlib.Path(module).resolve()))}',
                          'ts-libmr-protocol internal']
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
                'output': sum(s['total_net_output_bytes'] for s in stats)}

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
    parser.add_argument('--cases', nargs='+', choices=['narrow_avg', 'broad_avg', 'multi_avg_sum',
                                                     'group_avg', 'mget', 'raw'])
    args = parser.parse_args()
    if not args.redis_server or not args.memtier:
        parser.error('redis-server and memtier_benchmark must be installed or supplied explicitly')
    if min(args.shards, args.repeats, args.seconds) < 1:
        parser.error('shards, repeats and seconds must be positive')
    if args.base_port < 1024 or args.base_port + args.shards + 10000 > 65535:
        parser.error('base-port must leave room for all client and cluster-bus ports')
    if not pathlib.Path(args.module).is_file():
        parser.error('module must be an existing file')
    cluster = Cluster(args.module, args.shards, args.root, args.redis_server, args.base_port)
    try:
        cluster.seed()
        cases = {
            'narrow_avg': 'TS.MRANGE 6000000 6059999 AGGREGATION avg 60000 FILTER measurement=cpu fieldname=usage_nice host=h1',
            'broad_avg': 'TS.MRANGE 6000000 6059999 AGGREGATION avg 60000 FILTER measurement=cpu',
            'multi_avg_sum': 'TS.MRANGE 6000000 6059999 AGGREGATION avg,sum 60000 FILTER measurement=cpu',
            'group_avg': 'TS.MRANGE 6000000 6059999 AGGREGATION avg 60000 FILTER measurement=cpu GROUPBY group REDUCE avg',
            'mget': 'TS.MGET FILTER measurement=cpu',
            'raw': 'TS.MRANGE 6000000 6059999 FILTER measurement=cpu',
        }
        if args.cases:
            cases = {k: cases[k] for k in args.cases}
        # Verify every query before measuring, and preserve replies for before/after comparison.
        replies = {name: cluster.connections[0].execute_command(*command.split())
                   for name, command in cases.items()}
        (cluster.root / 'replies.json').write_text(json.dumps(replies, default=lambda b: b.decode()))
        runs = [(name, command, repeat) for repeat in range(args.repeats)
                for name, command in cases.items()]
        random.Random(1).shuffle(runs)
        rows = []
        for name, command, repeat in runs:
            result = cluster.root / f'{name}-{repeat}.json'
            before = cluster.metrics()
            with (cluster.root / f'{name}-{repeat}.log').open('w') as log:
                subprocess.run([args.memtier, '-s', '127.0.0.1',
                                '-p', str(cluster.ports[0]), '-t', '2', '-c', '8', '--pipeline', '2',
                                '--test-time', str(args.seconds), '--command', command,
                                '--hide-histogram', '--json-out-file', str(result)],
                               stdout=log, stderr=subprocess.STDOUT, check=True,
                               timeout=args.seconds + 30)
            after = cluster.metrics()
            result_data = json.loads(result.read_text())
            totals = result_data['ALL STATS']['Totals']
            if totals['Count'] == 0:
                raise RuntimeError(f'No completed requests for {name}; inspect its log')
            row = {'case': name, 'repeat': repeat, 'shards': args.shards,
                   'totals': totals, 'cpu_seconds': after['cpu'] - before['cpu'],
                   'output_bytes': after['output'] - before['output']}
            rows.append(row)
            (cluster.root / 'summary.json').write_text(json.dumps(rows, indent=2))
            print(f"{name}: {totals['Ops/sec']:.0f} ops/sec, "
                  f"{row['cpu_seconds'] / totals['Count'] * 1e6:.1f} CPU us/request", flush=True)
    finally:
        cluster.close()


if __name__ == '__main__':
    main()
