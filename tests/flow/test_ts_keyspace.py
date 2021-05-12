import time

from RLTest import Env
import time

def assert_msg(env, msg, expected_type, expected_data):
    env.assertEqual(expected_type, msg['type']) 
    env.assertEqual(expected_data, msg['data']) 

def test_keyspace():
    sample_len = 1024
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('config', 'set', 'notify-keyspace-events', 'KEA')

        pubsub = r.pubsub()
        pubsub.psubscribe('__key*')
       
        time.sleep(1)
        env.assertEqual('psubscribe', pubsub.get_message()['type']) 

        r.execute_command('ts.add', 'tester{2}', 100, 1.1)
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.add')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester{2}')

        # Test MADD generate events for each key updated 
        r.execute_command("ts.madd", 'tester{2}', "*", 10, 'test_key2{2}', 2000, 20)
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.add')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester{2}')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.add')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'test_key2{2}')

        # Test INCRBY generate event on key
        r.execute_command("ts.INCRBY", 'tester{2}', "100")
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.incrby')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester{2}')

        # Test DECRBY generate event on key
        r.execute_command("ts.DECRBY", 'tester{2}', "13")
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.decrby')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester{2}')

def test_keyspace_create_rules():
    sample_len = 1024
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('config', 'set', 'notify-keyspace-events', 'KEA')

        pubsub = r.pubsub()
        pubsub.psubscribe('__key*')
       
        time.sleep(1)
        env.assertEqual('psubscribe', pubsub.get_message()['type']) 

        r.execute_command('TS.CREATE', 'tester_src{2}')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.create')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_src{2}')

        r.execute_command('TS.CREATE', 'tester_dest{2}')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.create')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_dest{2}')

        r.execute_command('TS.CREATERULE', 'tester_src{2}', 'tester_dest{2}', 'AGGREGATION', 'COUNT', 10)
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.createrule:src')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_src{2}')

        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.createrule:dest')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_dest{2}')

        r.execute_command('TS.DELETERULE', 'tester_src{2}', 'tester_dest{2}')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.deleterule:src')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_src{2}')

        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.deleterule:dest')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_dest{2}')


def test_keyspace_rules_send():
    sample_len = 1024
    env = Env()
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('config', 'set', 'notify-keyspace-events', 'KEA')

        pubsub = r.pubsub()
        pubsub.psubscribe('__key*')
       
        time.sleep(1)
        env.assertEqual('psubscribe', pubsub.get_message()['type']) 

        r.execute_command('TS.CREATE', 'tester_src{2}')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.create')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_src{2}')

        r.execute_command('TS.CREATE', 'tester_dest{2}')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.create')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_dest{2}')

        r.execute_command('TS.CREATERULE', 'tester_src{2}', 'tester_dest{2}', 'AGGREGATION', 'MAX', 1)
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.createrule:src')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_src{2}')

        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.createrule:dest')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_dest{2}')

        r.execute_command('ts.add', 'tester_src{2}', 100, 1.1)
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.add')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_src{2}')

        r.execute_command('ts.add', 'tester_src{2}', 101, 1.1)

        # First getting the event from the dest on the previous window 
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.add:dest')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_dest{2}')

        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.add')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_src{2}')

        r.execute_command('ts.incrby', 'tester_src{2}', 3)

        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.add:dest')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_dest{2}')

        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.incrby')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_src{2}')
