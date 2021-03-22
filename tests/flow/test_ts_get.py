import pytest
import redis
import time 
from RLTest import Env

def test_get_timestamp(self):
    with Env().getConnection() as r:
        
        self.assertTrue(r.execute_command("TS.CREATE", "X"))
        self.assertEqual(r.execute_command("TS.GET", "X"), [])
        
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
        
# TODO add negative timestamp test         

# TODO add blocking test with result

# TODO add blocking test with no result (before timeout)

# TODO add blocking test with (timeout passed)

# TODO add negative blocking test

