import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import redis

from includes import Env, VALGRIND, SANITIZER
from utils import slot_table
from test_asm import fill_some_data, migrate_slots_back_and_forth, MIGRATION_CYCLES


# MOD-16951 regression guard.
#
# The "topology-change notifications" feature (MOD-16382) makes the module
# subscribe to the server's cluster-topology-change event and keep the LibMR
# inter-shard topology in sync from the callback (MR_UpdateClusterTopology()).
#
# Rebuilding the LibMR topology tears down the inter-shard connections and
# installs a fresh cluster. The bug (MOD-16951) was that the rebuild did NOT
# re-warm those connections (unlike the initial MR_ClusterInit()), so they were
# left cold. The next cross-shard fan-out (TS.MRANGE / TS.MREVRANGE / TS.MGET /
# TS.QUERYINDEX) logged "message was not sent because status is not connected",
# queued its map requests, and relied on the lazy connect + resend-on-HELLO
# path; when that path stranded the queued messages the reduce step never
# received shard replies and the execution idled out ("execution max idle
# reached") WITHOUT a timely reply -- the client hung until its own read
# timeout.
#
# Aborting an in-flight multi-shard command when the topology genuinely changes
# (replying with an error so the client can retry) is expected behavior. The bug
# is the *hang* -- the command returning nothing at all. The fix re-establishes
# the inter-shard connections eagerly as part of the topology update
# (MR_UpdateClusterTopologyIfNeeded now calls MR_ClusterConnectToShards()), so a
# cross-shard command after a topology change always gets a timely reply.

# Errors that are an acceptable *transient* reply during slot migration -- the
# client simply retries. A hang (no reply) is never acceptable.
EXPECTED_TRANSIENT_ERRORS = (
    "Query requires unavailable slots",
    "A multi-shard command failed because the cluster topology has changed",
)


def _topology_events_value(conn):
    res = conn.execute_command('CONFIG', 'GET', 'ts-topology-events')
    # CONFIG GET -> [name, value]; normalize bytes/str across RESP2/RESP3.
    value = res[1] if isinstance(res, (list, tuple)) else res['ts-topology-events']
    if isinstance(value, bytes):
        value = value.decode()
    return value


def _connection_with_timeout(env, shard, timeout):
    """A dedicated client with a socket timeout, so a server-side hang surfaces
    as a TimeoutError instead of blocking the test forever."""
    kwargs = dict(env.getConnection(shard).get_connection_kwargs())
    kwargs['socket_timeout'] = timeout
    kwargs['decode_responses'] = True
    return redis.Redis(**kwargs)


def test_topology_events_enabled_by_default():
    """The topology-change subscription ships enabled."""
    env = Env(decodeResponses=True)
    env.assertEqual(_topology_events_value(env.getConnection()), 'yes')


def test_topology_events_toggleable():
    """The subscription can be turned off explicitly."""
    env = Env(decodeResponses=True, moduleArgs="ts-topology-events no")
    env.assertEqual(_topology_events_value(env.getConnection()), 'no')


def test_cluster_fanout_works_with_default_config():
    """
    MOD-16951: with the default configuration (topology events on) a cross-shard
    fan-out must reply promptly instead of hanging. Runs on OSS cluster only.
    """
    env = Env(decodeResponses=True)
    if not env.is_cluster():
        env.skip()
    env.skipOnSlave()
    env.skipOnAOF()

    env.assertEqual(_topology_events_value(env.getConnection(0)), 'yes')

    number_of_keys = 60 if not (VALGRIND or SANITIZER) else 20
    with env.getClusterConnectionIfNeeded() as rc:
        for i in range(number_of_keys):
            hslot = i * (2 ** 14 - 1) // (number_of_keys - 1)
            key = f"ts:{{{slot_table[hslot]}}}"
            rc.execute_command('TS.CREATE', key, 'LABELS', 'name', 'bob')
            rc.execute_command('TS.ADD', key, 1, 1)

    deadline = time.time() + (60 if (VALGRIND or SANITIZER) else 15)

    qi = env.getConnection(0).execute_command('TS.QUERYINDEX', 'name=bob')
    env.assertEqual(len(qi), number_of_keys)

    result = env.getConnection(0).execute_command(
        'TS.MRANGE', '-', '+', 'FILTER', 'name=bob')
    env.assertEqual(len(result), number_of_keys)

    env.assertTrue(time.time() < deadline)


def test_cluster_fanout_does_not_hang_during_topology_churn():
    """
    MOD-16951 hang guard, with the topology-change subscription ENABLED (the
    default).

    A multi-shard query issued repeatedly while slots migrate back and forth
    must always get a *timely reply* -- either a valid result or one of the
    expected transient errors (retryable). It must never hang: the query runs on
    a client with a socket timeout, so the "execution max idle reached" silent
    hang (no reply) surfaces as a TimeoutError and fails the test.

    Before the fix, a topology rebuild left the inter-shard connections cold and
    the following fan-out could strand its map requests and idle out without a
    reply. The fix re-warms the connections as part of the rebuild.
    """
    env = Env(shardsCount=2, decodeResponses=True, noLog=False)
    if env.env != "oss-cluster":
        env.skip()
    env.skipOnSlave()
    env.skipOnAOF()

    # The subscription is on -- this is what makes the test meaningful.
    env.assertEqual(_topology_events_value(env.getConnection(0)), 'yes')

    number_of_keys = 1000 if not (VALGRIND or SANITIZER) else 100
    samples_per_key = 150
    fill_some_data(env, number_of_keys, samples_per_key, label1=17, label2=19)

    command = ("TS.MRANGE", "-", "+", "FILTER", "label1=17", "GROUPBY", "label1", "REDUCE", "count")
    hang_timeout = 30 if (VALGRIND or SANITIZER) else 10
    conn = _connection_with_timeout(env, 0, hang_timeout)

    def validate_result(result):
        ((filtered_by, withlabels, samples),) = result
        env.assertEqual(filtered_by, "label1=17")
        env.assertEqual(withlabels, [])
        env.assertEqual(len(samples), samples_per_key)
        env.assertTrue(all(int(sample[1]) == number_of_keys for sample in samples))

    validate_result(conn.execute_command(*command))

    done = threading.Event()

    def query_in_a_loop():
        while not done.is_set():
            try:
                result = conn.execute_command(*command)
            except redis.exceptions.TimeoutError:
                # No reply within hang_timeout -> the MOD-16951 hang.
                env.assertTrue(False, message="cross-shard query hung (no reply) during topology churn")
                return
            except redis.exceptions.ResponseError as x:
                # Retryable transient during migration -- expected.
                env.assertTrue(str(x) in EXPECTED_TRANSIENT_ERRORS, message=str(x))
                continue
            validate_result(result)

    def migrate_slots():
        for _ in range(MIGRATION_CYCLES):
            if done.is_set():
                break
            migrate_slots_back_and_forth(env)

    with ThreadPoolExecutor() as executor:
        futures = list(map(executor.submit, [query_in_a_loop, migrate_slots]))
        for future in as_completed(futures):
            done.set()
            future.result()

    validate_result(conn.execute_command(*command))
