import time

from RLTest import Env
import time

def assert_msg(env, msg, expected_type, expected_data):
    env.assertEqual(expected_type, msg['type']) 
    env.assertEqual(expected_data, msg['data']) 

# Skip all tests on cluster because for each node we will need to configure to enable keyspace and then have a dedicated
# connection that will handle the pubsub from that specific shard since keyspace notification are not broadcasted
# across all nodes.

def test_keyspace():
    Env().skipOnCluster()
    sample_len = 1024
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('config', 'set', 'notify-keyspace-events', 'KEA')

        pubsub = r.pubsub()
        pubsub.psubscribe('__key*')
       
        time.sleep(1)
        env.assertEqual('psubscribe', pubsub.get_message(timeout=1)['type']) 

        r.execute_command('ts.add', 'tester{2}', 100, 1.1)
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.add')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester{2}')

        # Test MADD generate events for each key updated 
        r.execute_command("ts.madd", 'tester{2}', "*", 10, 'test_key2{2}', 2000, 20)
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.add')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester{2}')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.add')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'test_key2{2}')

        # Test INCRBY generate event on key
        r.execute_command("ts.INCRBY", 'tester{2}', "100")
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.incrby')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester{2}')

        # Test DECRBY generate event on key
        r.execute_command("ts.DECRBY", 'tester{2}', "13")
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.decrby')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester{2}')

        # Test DEL generate event on key
        r.execute_command("ts.DEL", 'tester{2}', "100", "100")
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.del')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester{2}')

def test_keyspace_create_rules():
    Env().skipOnCluster()
    sample_len = 1024
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('config', 'set', 'notify-keyspace-events', 'KEA')

        pubsub = r.pubsub()
        pubsub.psubscribe('__key*')
       
        time.sleep(1)
        env.assertEqual('psubscribe', pubsub.get_message(timeout=1)['type']) 

        r.execute_command('TS.CREATE', 'tester_src{2}')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.create')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester_src{2}')

        r.execute_command('TS.CREATE', 'tester_dest{2}')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.create')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester_dest{2}')

        r.execute_command('TS.CREATERULE', 'tester_src{2}', 'tester_dest{2}', 'AGGREGATION', 'COUNT', 10)
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.createrule:src')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester_src{2}')

        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.createrule:dest')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester_dest{2}')

        r.execute_command('TS.DELETERULE', 'tester_src{2}', 'tester_dest{2}')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.deleterule:src')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester_src{2}')

        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.deleterule:dest')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester_dest{2}')


def test_keyspace_rules_send():
    Env().skipOnCluster()

    sample_len = 1024
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('config', 'set', 'notify-keyspace-events', 'KEA')

        pubsub = r.pubsub()
        pubsub.psubscribe('__key*')
       
        time.sleep(1)
        env.assertEqual('psubscribe', pubsub.get_message(timeout=1)['type']) 

        r.execute_command('TS.CREATE', 'tester_src{2}')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.create')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester_src{2}')

        r.execute_command('TS.CREATE', 'tester_dest{2}')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.create')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester_dest{2}')

        r.execute_command('TS.CREATERULE', 'tester_src{2}', 'tester_dest{2}', 'AGGREGATION', 'MAX', 1)
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.createrule:src')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester_src{2}')

        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.createrule:dest')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester_dest{2}')

        r.execute_command('ts.add', 'tester_src{2}', 100, 1.1)
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.add')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester_src{2}')

        for i in range(1000):
            r.execute_command('ts.add', 'tester_src{2}', 101 + i, 1.1)

            # First getting the event from the dest on the previous window
            assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.add:dest')
            assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester_dest{2}')

            assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.add')
            assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester_src{2}')

        r.execute_command('ts.incrby', 'tester_src{2}', 3)

        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.add:dest')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester_dest{2}')

        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'ts.incrby')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', b'tester_src{2}')
