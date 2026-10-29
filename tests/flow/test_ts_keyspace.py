import time

from includes import Env
import time
from includes import *


def get_keyspace_message(pubsub):
    # get_message can return None before the socket timeout (e.g. while
    # establishing the subscription). Wait for the next message, without
    # skipping unexpected messages or changing their order.
    deadline = time.monotonic() + WAKE_TIMEOUT_SECS
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(f"No keyspace Pub/Sub message within {WAKE_TIMEOUT_SECS}s")
        msg = pubsub.get_message(timeout=remaining)
        if msg is not None:
            return msg


def subscribe_to_keyspace(env, conn):
    conn.execute_command('config', 'set', 'notify-keyspace-events', 'KEA')
    pubsub = conn.pubsub()
    pubsub.psubscribe('__key*')
    env.assertEqual('psubscribe', get_keyspace_message(pubsub)['type'])
    return pubsub


def assert_event(env, pubsub, event, key):
    # KEA emits a keyspace notification followed by a keyevent notification.
    for expected_data in (event, key):
        msg = get_keyspace_message(pubsub)
        env.assertEqual('pmessage', msg['type'])
        env.assertEqual(expected_data, msg['data'])

# Skip all tests on cluster because for each node we will need to configure to enable keyspace and then have a dedicated
# connection that will handle the pubsub from that specific shard since keyspace notification are not broadcasted
# across all nodes.

def test_keyspace():
    Env().skipOnCluster()
    Env().skipOnVersionSmaller("6.2.0")
    skip_on_rlec()
    sample_len = 1024
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        pubsub = subscribe_to_keyspace(env, r)

        r.execute_command('ts.add', 'tester{2}', 100, 1.1)
        assert_event(env, pubsub, b'ts.add', b'tester{2}')

        # Test MADD generate events for each key updated
        r.execute_command("ts.madd", 'tester{2}', "*", 10, 'test_key2{2}', 2000, 20)
        assert_event(env, pubsub, b'ts.add', b'tester{2}')
        assert_event(env, pubsub, b'ts.add', b'test_key2{2}')

        # Test INCRBY generate event on key
        r.execute_command("ts.INCRBY", 'tester{2}', "100")
        assert_event(env, pubsub, b'ts.incrby', b'tester{2}')

        # Test DECRBY generate event on key
        r.execute_command("ts.DECRBY", 'tester{2}', "13")
        assert_event(env, pubsub, b'ts.decrby', b'tester{2}')

        # Test DEL generate event on key
        r.execute_command("ts.DEL", 'tester{2}', "100", "100")
        assert_event(env, pubsub, b'ts.del', b'tester{2}')

        # Test ALTER generate event on key
        r.execute_command("ts.ALTER", 'tester{2}', "LABELS", "abc", 123)
        assert_event(env, pubsub, b'ts.alter', b'tester{2}')

def test_keyspace_create_rules():
    Env().skipOnCluster()
    Env().skipOnVersionSmaller("6.2.0")
    skip_on_rlec()
    sample_len = 1024
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        pubsub = subscribe_to_keyspace(env, r)

        r.execute_command('TS.CREATE', 'tester_src{2}')
        assert_event(env, pubsub, b'ts.create', b'tester_src{2}')

        r.execute_command('TS.CREATE', 'tester_dest{2}')
        assert_event(env, pubsub, b'ts.create', b'tester_dest{2}')

        r.execute_command('TS.CREATERULE', 'tester_src{2}', 'tester_dest{2}', 'AGGREGATION', 'COUNT', 10)
        assert_event(env, pubsub, b'ts.createrule:src', b'tester_src{2}')

        assert_event(env, pubsub, b'ts.createrule:dest', b'tester_dest{2}')

        r.execute_command('TS.DELETERULE', 'tester_src{2}', 'tester_dest{2}')
        assert_event(env, pubsub, b'ts.deleterule:src', b'tester_src{2}')

        assert_event(env, pubsub, b'ts.deleterule:dest', b'tester_dest{2}')


def test_keyspace_rules_send():
    Env().skipOnCluster()
    Env().skipOnVersionSmaller("6.2.0")
    skip_on_rlec()
    sample_len = 1024
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        pubsub = subscribe_to_keyspace(env, r)

        r.execute_command('TS.CREATE', 'tester_src{2}')
        assert_event(env, pubsub, b'ts.create', b'tester_src{2}')

        r.execute_command('TS.CREATE', 'tester_dest{2}')
        assert_event(env, pubsub, b'ts.create', b'tester_dest{2}')

        r.execute_command('TS.CREATERULE', 'tester_src{2}', 'tester_dest{2}', 'AGGREGATION', 'MAX', 1)
        assert_event(env, pubsub, b'ts.createrule:src', b'tester_src{2}')

        assert_event(env, pubsub, b'ts.createrule:dest', b'tester_dest{2}')

        r.execute_command('ts.add', 'tester_src{2}', 100, 1.1)
        assert_event(env, pubsub, b'ts.add', b'tester_src{2}')

        for i in range(1000):
            r.execute_command('ts.add', 'tester_src{2}', 101 + i, 1.1)

            # First getting the event from the dest on the previous window
            assert_event(env, pubsub, b'ts.add:dest', b'tester_dest{2}')

            assert_event(env, pubsub, b'ts.add', b'tester_src{2}')

        r.execute_command('ts.incrby', 'tester_src{2}', 3)

        assert_event(env, pubsub, b'ts.add:dest', b'tester_dest{2}')

        assert_event(env, pubsub, b'ts.incrby', b'tester_src{2}')
