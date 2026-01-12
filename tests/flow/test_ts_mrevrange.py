# from utils import Env
from test_helper_classes import _insert_data
from includes import *


def test_mrevrange():
    start_ts = 1511885909
    samples_count = 50
    env = Env()
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        assert r.execute_command('TS.CREATE', 'tester1', 'LABELS', 'name', 'bob', 'class', 'middle', 'generation', 'x')
        assert r.execute_command('TS.CREATE', 'tester2', 'LABELS', 'name', 'rudy', 'class', 'junior', 'generation', 'x')
        assert r.execute_command('TS.CREATE', 'tester3', 'LABELS', 'name', 'fabi', 'class', 'top', 'generation', 'x')
        _insert_data(r, 'tester1', start_ts, samples_count, 5)
        _insert_data(r, 'tester2', start_ts, samples_count, 15)
        _insert_data(r, 'tester3', start_ts, samples_count, 25)

        expected_result = [[start_ts + i, str(5).encode('ascii')] for i in range(samples_count)]
        expected_result.reverse()
        actual_result = r1.execute_command('TS.mrevrange', start_ts, start_ts + samples_count, 'FILTER', 'name=bob')
        assert [[b'tester1', [], expected_result]] == actual_result

        actual_result = r1.execute_command('TS.mrevrange', start_ts, start_ts + samples_count, 'COUNT', '5', 'FILTER',
                                           'generation=x')
        actual_result.sort(key=lambda x:x[0])
        assert actual_result == [[b'tester1', [],
                                  [[1511885958, b'5'], [1511885957, b'5'], [1511885956, b'5'], [1511885955, b'5'],
                                   [1511885954, b'5']]],
                                 [b'tester2', [],
                                  [[1511885958, b'15'], [1511885957, b'15'], [1511885956, b'15'], [1511885955, b'15'],
                                   [1511885954, b'15']]],
                                 [b'tester3', [],
                                  [[1511885958, b'25'], [1511885957, b'25'], [1511885956, b'25'], [1511885955, b'25'],
                                   [1511885954, b'25']]]]

        agg_result = r1.execute_command('TS.mrange', 0, '+', 'AGGREGATION', 'sum', 50, 'FILTER', 'name=bob')[0][2]
        rev_agg_result = r1.execute_command('TS.mrevrange', 0, '+', 'AGGREGATION', 'sum', 50, 'FILTER', 'name=bob')[0][2]
        rev_agg_result.reverse()
        assert rev_agg_result == agg_result
        last_results = list(agg_result)
        last_results.reverse()
        last_results = last_results[0:3]
        assert r1.execute_command('TS.mrevrange', 0, '+', 'AGGREGATION', 'sum', 50, 'COUNT', 3, 'FILTER', 'name=bob')[0][
                   2] == last_results


def test_mrevrange_pipelined_load_does_not_hang():
    """
    Regression test: multi-shard TS.MREVRANGE under multi-connection + pipeline load must not hang.

    This test targets failures where internal multi-shard aggregation/cleanup can deadlock/stall,
    causing clients to time out and (in some environments) shards to be watchdog-killed.
    """
    env = Env()
    if env.shardsCount < 2:
        env.skip()
    if not env.is_cluster():
        env.skip()

    # Make the test resilient to transient local FS / BGSAVE issues (which can surface as MISCONF
    # and block writes). We don't want a persistence issue to mask a multi-shard hang regression.
    for c in shardsConnections(env):
        try:
            c.execute_command('CONFIG', 'SET', 'save', '')
            c.execute_command('CONFIG', 'SET', 'stop-writes-on-bgsave-error', 'no')
        except Exception:
            # If CONFIG is disabled by the Redis build/environment, best-effort only.
            pass

    # Keep dataset small to avoid adding real load, while still forcing multi-shard fanout.
    start_ts = 1
    samples_count = 20
    series_per_shard = 4

    with env.getClusterConnectionIfNeeded() as r:
        for i in range(env.shardsCount * series_per_shard):
            # Different hash-tags => different slots => more likely to spread across shards.
            key = f"mrevrange_load{{{i}}}"
            r.execute_command('TS.CREATE', key, 'LABELS', 'user_id', '754', 'metric', 'x')
            _insert_data(r, key, start_ts, samples_count, 1)

    # Use a direct socket connection with short timeouts so a hang turns into a fast test failure.
    base = env.getConnection(1)
    kw = dict(base.connection_pool.connection_kwargs)
    host = kw.get('host', '127.0.0.1')
    port = kw['port']
    password = kw.get('password', None)

    cmd = [
        'TS.MREVRANGE',
        str(start_ts),
        str(start_ts + samples_count),
        'FILTER',
        'user_id=754',
        'metric=x',
    ]

    # Simulate memtier-like load: multiple connections, pipelined commands.
    n_conns = 4
    pipeline_len = 25

    conns = []
    try:
        for _ in range(n_conns):
            c = redis.connection.Connection(
                host=host,
                port=port,
                password=password,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
            c.connect()
            conns.append(c)

        with TimeLimit(30):
            # Send a pipeline burst on all connections first...
            for c in conns:
                for _ in range(pipeline_len):
                    c.send_command(*cmd)

            # ...then drain all replies.
            for c in conns:
                for _ in range(pipeline_len):
                    resp = c.read_response()
                    # For OSS cluster, a successful TS.MREVRANGE reply is an array.
                    assert isinstance(resp, list)
    finally:
        for c in conns:
            try:
                c.disconnect()
            except Exception:
                pass

    # Sanity: cluster still responds after the load burst.
    with env.getClusterConnectionIfNeeded() as r:
        r.ping()
