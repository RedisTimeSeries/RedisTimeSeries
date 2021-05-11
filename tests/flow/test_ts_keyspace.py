import time

from RLTest import Env
import time

def assert_msg(env, msg, expected_type, expected_data):
    env.assertEqual(expected_type, msg['type']) 
    env.assertEqual(expected_data, msg['data']) 

def test_keyspace():
    sample_len = 1024
    env = Env()
    with env.getConnection() as r:
        r.execute_command('config', 'set', 'notify-keyspace-events', 'KEA')

        pubsub = r.pubsub()
        pubsub.psubscribe('__key*')
       
        time.sleep(1)
        env.assertEqual('psubscribe', pubsub.get_message()['type']) 

        r.execute_command('ts.add', 'tester', 100, 1.1)
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.add')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester')

        # Test MADD generate events for each key updated 
        r.execute_command("ts.madd", 'tester', "*", 10, 'test_key2', 2000, 20)
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.add')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.add')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'test_key2')

        # Test INCRBY generate event on key
        r.execute_command("ts.INCRBY", 'tester', "100")
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.incrby')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester')

        # Test DECRBY generate event on key
        r.execute_command("ts.DECRBY", 'tester', "13")
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.decrby')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester')

def test_keyspace_rules():
    sample_len = 1024
    env = Env()
    with env.getConnection() as r:
        r.execute_command('config', 'set', 'notify-keyspace-events', 'KEA')

        pubsub = r.pubsub()
        pubsub.psubscribe('__key*')
       
        time.sleep(1)
        env.assertEqual('psubscribe', pubsub.get_message()['type']) 

        r.execute_command('TS.CREATE', 'tester_src')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.create')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_src')

        r.execute_command('TS.CREATE', 'tester_dest')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.create')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_dest')

        r.execute_command('TS.CREATERULE', 'tester_src', 'tester_dest', 'AGGREGATION', 'MAX', 1)
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.createrule:src')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_src')

        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.createrule:dest')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_dest')

        r.execute_command('ts.add', 'tester_src', 100, 1.1)
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.add')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_src')

        r.execute_command('ts.add', 'tester_src', 101, 1.1)

        # First getting the event from the dest on the previous window 
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.add:dest')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_dest')

        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.add')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_src')

        r.execute_command('ts.incrby', 'tester_src', 3)
        
        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.add:dest')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_dest')

        assert_msg(env, pubsub.get_message(), 'pmessage', b'ts.incrby')
        assert_msg(env, pubsub.get_message(), 'pmessage', b'tester_src')
