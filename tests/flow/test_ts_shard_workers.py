"""Exercise detached shard inputs, including writes while queries are in flight."""
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
import math
import time
import redis

from includes import Env, RLEC_CLUSTER, verifyClusterInitialized
from redis.crc import key_slot


def worker_env(protocol=2):
    if RLEC_CLUSTER:
        Env().skip()
    env = Env(shardsCount=3, protocol=protocol,
              moduleArgs='LIBMR_PROTOCOL INTERNAL NUM_THREADS 3')
    if not env.isCluster():
        env.skip()
    for shard in range(1, 4):
        conn = env.getConnection(shard)
        conn.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
        conn.execute_command('timeseries.FORCESHARDSCONNECTION')
    verifyClusterInitialized(env)
    return env


def entries(reply):
    return ({key: value[-1] for key, value in reply.items()} if isinstance(reply, dict)
            else {row[0]: row[2] for row in reply})


def samples_equal(actual, expected):
    if isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(
            samples_equal(actual[key], expected[key]) for key in expected)
    if isinstance(expected, (list, tuple)):
        return len(actual) == len(expected) and all(samples_equal(a, b) for a, b in zip(actual, expected))
    return (actual == expected or math.isclose(float(actual), float(expected), abs_tol=1e-12, rel_tol=1e-12)
            or (math.isnan(float(actual)) and math.isnan(float(expected))))


def tags_by_shard(env):
    return [next(str(i) for i in range(100000) if lo <= key_slot(str(i).encode()) <= hi)
            for lo, hi, *_ in env.getConnection(1).execute_command('CLUSTER', 'SLOTS')]


def check_worker_ranges(protocol):
    env = worker_env(protocol)
    with env.getClusterConnectionIfNeeded() as client:
        keys = []
        for tag in tags_by_shard(env):
            for encoding in ('COMPRESSED', 'UNCOMPRESSED'):
                key = f'worker:{{{tag}}}:{encoding}'
                keys.append(key)
                client.execute_command('TS.CREATE', key, 'ENCODING', encoding,
                                       'CHUNK_SIZE', 128, 'RETENTION', 15000,
                                       'LABELS', 'test', 'worker', 'group', 'all')
                pipe = client.pipeline(transaction=False)
                for i in range(6000):
                    pipe.execute_command('TS.ADD', key, i * 3, (i % 11) - 5)
                pipe.execute()
        options = [[], ['COUNT', 7], ['FILTER_BY_VALUE', -2, 2],
                   ['FILTER_BY_TS', 999, 1200, 3000], ['AGGREGATION', 'avg', 53],
                   ['AGGREGATION', 'min,max', 53], ['AGGREGATION', 'twa', 53],
                   ['AGGREGATION', 'last', 53, 'EMPTY']]
        cases = []
        for reverse in (False, True):
            local = 'TS.REVRANGE' if reverse else 'TS.RANGE'
            multi = 'TS.MREVRANGE' if reverse else 'TS.MRANGE'
            for lo, hi in ((0, 18000), (2100, 14800), (18000, 18100)):
                for opts in options:
                    expected = {key.encode(): client.execute_command(local, key, lo, hi, *opts)
                                for key in keys}
                    cases.append(([multi, lo, hi, *opts, 'SELECTED_LABELS', 'group',
                                   'FILTER', 'test=worker'], expected))

        def check(shard):
            with env.getConnection(shard) as conn:
                for command, expected in cases:
                    assert samples_equal(entries(conn.execute_command(*command)), expected), command

        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(check, range(1, 4)))


def test_worker_ranges_resp2():
    check_worker_ranges(2)


def test_worker_ranges_resp3():
    check_worker_ranges(3)


def test_worker_snapshots_during_replacement():
    env = worker_env()
    keys = [f'replace:{{{tag}}}' for tag in tags_by_shard(env)]
    barrier = Barrier(9)

    def replace(client, key, generation):
        # A transaction makes one key's samples and generation label change together.
        owner = client.get_redis_connection(client.get_node_from_key(key))
        pipe = owner.pipeline(transaction=True)
        pipe.delete(key)
        pipe.execute_command('TS.CREATE', key, 'LABELS', 'test', 'replacement',
                             'generation', generation)
        for i in range(9000):
            pipe.execute_command('TS.ADD', key, i, generation)
        pipe.execute()

    with env.getClusterConnectionIfNeeded() as client:
        for key in keys:
            replace(client, key, 0)

    def writer(key):
        with env.getClusterConnectionIfNeeded() as client:
            barrier.wait(timeout=10)
            for generation in range(1, 16):
                replace(client, key, generation)

    def reader(shard):
        with env.getConnection(shard) as conn:
            barrier.wait(timeout=10)
            for _ in range(25):
                reply = conn.execute_command('TS.MRANGE', '-', '+', 'WITHLABELS',
                                             'FILTER', 'test=replacement')
                env.assertEqual(len(reply), 3)
                for key, labels, samples in reply:
                    generation = float(dict(labels)[b'generation'])
                    env.assertEqual(len(samples), 9000)
                    env.assertTrue(all(ts == i and float(value) == generation
                                       for i, (ts, value) in enumerate(samples)))

    with ThreadPoolExecutor(max_workers=9) as pool:
        tasks = [pool.submit(writer, key) for key in keys]
        tasks += [pool.submit(reader, shard) for shard in (1, 2, 3, 1, 2, 3)]
        for task in tasks:
            task.result()


def test_worker_connection_recovery():
    env = worker_env()
    with env.getClusterConnectionIfNeeded() as client:
        for tag in tags_by_shard(env):
            key = f'reconnect:{{{tag}}}'
            client.execute_command('TS.CREATE', key, 'LABELS', 'test', 'reconnect')
            pipe = client.pipeline(transaction=False)
            for i in range(24000):
                pipe.execute_command('TS.ADD', key, i, 1)
            pipe.execute()

    barrier = Barrier(4)
    command = ['TS.MRANGE', '-', '+', 'AGGREGATION', 'sum', 24000,
               'FILTER', 'test=reconnect']

    def check(reply):
        env.assertEqual(len(reply), 3)
        env.assertTrue(all(samples == [[0, b'24000']] for samples in entries(reply).values()))

    def reader(shard):
        with env.getConnection(shard) as conn:
            barrier.wait(timeout=10)
            for _ in range(200):
                try:
                    check(conn.execute_command(*command))
                except redis.ResponseError:
                    # A connection loss can fail its distributed execution. The
                    # next request must recover, and successful replies must be complete.
                    pass

    killed = 0
    observed = set()
    with ThreadPoolExecutor(max_workers=3) as pool:
        tasks = [pool.submit(reader, shard) for shard in range(1, 4)]
        barrier.wait(timeout=10)
        for _ in range(50):
            for shard in range(1, 4):
                conn = env.getConnection(shard)
                for client in conn.client_list():
                    observed.add((client['cmd'], client['flags']))
                    if client['cmd'].lower() == 'timeseries.innercommunication' and 'b' in client['flags']:
                        killed += conn.execute_command('CLIENT', 'KILL', 'ID', client['id'])
                        break
            time.sleep(0.002)
        for task in tasks:
            task.result()
    assert killed > 0, observed
    for shard in range(1, 4):
        check(env.getConnection(shard).execute_command(*command))
