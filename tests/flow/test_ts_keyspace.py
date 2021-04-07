import time

from RLTest import Env
import time

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
        msg = pubsub.get_message()
        env.assertEqual('pmessage', msg['type']) 
        env.assertEqual(b'ts.add', msg['data']) 

        msg = pubsub.get_message()
        env.assertEqual('pmessage', msg['type']) 
        env.assertEqual(b'tester', msg['data']) 

        # Test MADD generate events for each key updated 
        r.execute_command("ts.madd", 'tester', "*", 10, 'test_key2', 2000, 20)
        msg = pubsub.get_message()
        env.assertEqual('pmessage', msg['type']) 
        env.assertEqual(b'ts.madd', msg['data']) 

        msg = pubsub.get_message()
        env.assertEqual('pmessage', msg['type']) 
        env.assertEqual(b'tester', msg['data']) 

        msg = pubsub.get_message()
        env.assertEqual('pmessage', msg['type']) 
        env.assertEqual(b'ts.madd', msg['data']) 

        msg = pubsub.get_message()
        env.assertEqual('pmessage', msg['type']) 
        env.assertEqual(b'test_key2', msg['data']) 

        # Test INCRBY generate event on key
        r.execute_command("ts.INCRBY", 'tester', "100")
        msg = pubsub.get_message()

        env.assertEqual('pmessage', msg['type']) 
        env.assertEqual(b'ts.incrby', msg['data']) 

        msg = pubsub.get_message()
        env.assertEqual('pmessage', msg['type']) 
        env.assertEqual(b'tester', msg['data']) 

        # Test DECRBY generate event on key
        r.execute_command("ts.DECRBY", 'tester', "13")
        msg = pubsub.get_message()

        env.assertEqual('pmessage', msg['type']) 
        env.assertEqual(b'ts.decrby', msg['data']) 

        msg = pubsub.get_message()
        env.assertEqual('pmessage', msg['type']) 
        env.assertEqual(b'tester', msg['data']) 

