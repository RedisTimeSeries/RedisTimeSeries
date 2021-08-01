import pytest
import redis
import time 
import _thread
from RLTest import Env

def test_bad_get(self):
    with Env().getClusterConnectionIfNeeded() as r:
        
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("TS.GET", "X")

        self.assertTrue(r.execute_command("SET", "BAD_X", "NOT_TS"))
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("TS.GET", "BAD_X")

        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("TS.GET", "BAD_X", "TIMESTAMP", "20")

        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("TS.GET", "BAD_X", "BLOCK", "10")

        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("TS.GET", "BAD_X", "TIMESTAMP", "21" , "BLOCK", "40")

def test_get_timestamp(self):
    with Env().getClusterConnectionIfNeeded() as r:
        
        self.assertTrue(r.execute_command("TS.CREATE", "X"))
        self.assertEqual(r.execute_command("TS.GET", "X"), [])
        self.assertEqual(r.execute_command("TS.GET", "X", "TIMESTAMP", "0"), [])
        
        self.assertTrue(r.execute_command("TS.ADD", "X" ,"2" ,"1.2"))
        self.assertEqual(r.execute_command("TS.GET", "X"), [2, b'1.2'])
        self.assertEqual(r.execute_command("TS.GET", "X"), [2 ,b'1.2'])
        self.assertEqual(r.execute_command("TS.GET", "X", "TIMESTAMP", "0"), [2, b'1.2'])
        self.assertEqual(r.execute_command("TS.GET", "X", "TIMESTAMP", "1"), [2, b'1.2'])
        self.assertEqual(r.execute_command("TS.GET", "X", "TIMESTAMP", "2"), [])
        
        self.assertTrue(r.execute_command("TS.ADD", "X" ,"3" ,"2.1"))
        self.assertEqual(r.execute_command("TS.GET", "X"), [3, b'2.1'])
        self.assertEqual(r.execute_command("TS.GET", "X", "TIMESTAMP", "1"), [2, b'1.2'])
        self.assertEqual(r.execute_command("TS.GET", "X", "TIMESTAMP", "2"), [3, b'2.1'])
        self.assertEqual(r.execute_command("TS.GET", "X", "TIMESTAMP", "3"), [])
        
def test_bad_timestamp(self):
    with Env().getClusterConnectionIfNeeded() as r:
        
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("TS.GET", "bad_x", "TIMESTAMP", "0")        
        
        self.assertTrue(r.execute_command("TS.CREATE", "bad_x"))
        self.assertTrue(r.execute_command("TS.ADD", "bad_x" ,"6" ,"4.5"))
        self.assertEqual(r.execute_command("TS.GET", "bad_x", "TIMESTAMP", "4"), [6, b'4.5'])
        
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("TS.GET", "bad_x", "TIMESTAMP", "12d")        

        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("TS.GET", "bad_x", "TIMESTAMP", "-5")        
        
def async_add(r, key, event_time, event_value, block=1):
    time.sleep(block)
    r.execute_command("TS.ADD", key, event_time, event_value)

def test_block(self):
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command("TS.CREATE", "blocked_key")        
        self.assertEqual([], r.execute_command("TS.GET", "blocked_key", "BLOCK", "1"))        
       
        # test blocked forever 
        _thread.start_new_thread( async_add, (r, "blocked_key", 100, 22, 0.2) )
        self.assertEqual([100, b'22'], r.execute_command("TS.GET", "blocked_key", "BLOCK", "0"))

        _thread.start_new_thread( async_add, (r, "blocked_key", 102, 31, 0.1) )
        self.assertEqual([102, b'31'], r.execute_command("TS.GET", "blocked_key", "BLOCK", "200"))
        
        # test block timed out
        _thread.start_new_thread( async_add, (r, "blocked_key", 103, 32, 0.2) )
        self.assertEqual([], r.execute_command("TS.GET", "blocked_key", "BLOCK", "10"))

        # wrong block time
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("TS.GET", "blocked_key", "BLOCK", "-1")
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command("TS.GET", "blocked_key", "BLOCK", "32d")

def test_block_none_existing_key(self):
    with Env().getClusterConnectionIfNeeded() as r:

         # test block key doesn't exist
        _thread.start_new_thread( async_add, (r, "blocked_new_key1", "*", 32, 0.5) )
        res = r.execute_command("TS.GET", "blocked_new_key1", "BLOCK", "2000")
        self.assertEqual(b'32', res[1])

        _thread.start_new_thread( async_add, (r, "blocked_new_key2", "10", 32.4, 0.5) )
        self.assertEqual([10, b'32.4'], r.execute_command("TS.GET", "blocked_new_key2", "BLOCK", "2000"))

        # test block key doesn't exist with timestamp
        _thread.start_new_thread( async_add, (r, "blocked_new_key3", 1001, 55, 0.5) )
        self.assertEqual([1001, b'55'], r.execute_command("TS.GET", "blocked_new_key3", "TIMESTAMP", 1000, "BLOCK", "1000"))

def async_madd(r, block, *events):
    time.sleep(block)
    r.execute_command(*["TS.MADD", *events])

def test_block_madd(self):
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command("TS.CREATE", "blocked_madd_key")        
        _thread.start_new_thread( async_madd, (r, 0.5, "blocked_madd_key",  10, 32, "blocked_madd_key", 12, 44, "blocked_madd_key", 21, 19))
        self.assertEqual([21, b'19'], r.execute_command("TS.GET", "blocked_madd_key", "BLOCK", "1000"))

        _thread.start_new_thread( async_madd, (r, 0.5, "blocked_madd_key",  30, 12, "blocked_madd_key", 56, 77, "blocked_madd_key", 60, 77))
        self.assertEqual([60, b'77'], r.execute_command("TS.GET", "blocked_madd_key", "TIMESTAMP", 55, "BLOCK", "1000"))

def test_block_timestamp(self):
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command("TS.CREATE", "blocked_ts_key")        
        r.execute_command("TS.ADD", "blocked_ts_key", 201, 22.2)
        r.execute_command("TS.ADD", "blocked_ts_key", 202, 39.1)
        r.execute_command("TS.ADD", "blocked_ts_key", 203, -78.1)
       
        self.assertEqual([201, b'22.2'], r.execute_command("TS.GET", "blocked_ts_key", "TIMESTAMP", 200, "BLOCK", "200"))
        self.assertEqual([202, b'39.1'], r.execute_command("TS.GET", "blocked_ts_key", "TIMESTAMP", 201, "BLOCK", "200"))
        self.assertEqual([203, b'-78.1'], r.execute_command("TS.GET", "blocked_ts_key", "TIMESTAMP", 202, "BLOCK", "200"))

        _thread.start_new_thread( async_add, (r, "blocked_ts_key", 204, 33.5, 0.1) )
        self.assertEqual([204, b'33.5'], r.execute_command("TS.GET", "blocked_ts_key", "TIMESTAMP", 203, "BLOCK", "200"))