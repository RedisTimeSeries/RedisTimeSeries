import pytest
import redis
import time 
from RLTest import Env

def test_get_timestamp():
    with Env().getConnection() as r:
        assert r.execute_command("TS.GET", "X") == []
        assert r.execute_command("TS.ADD", "X" ,"2" ,"1.2")
        assert r.execute_command("TS.GET", "X") == [2,"1.2"]
        assert r.execute_command("TS.GET", "X", "TIMESTAMP", "0") == [2,"1.2"]
        assert r.execute_command("TS.GET", "X", "TIMESTAMP", "1") == [2,"1.2"]
        assert r.execute_command("TS.GET", "X", "TIMESTAMP", "2") == []
        
        assert r.execute_command("TS.ADD", "X" ,"3" ,"2.1")
        assert r.execute_command("TS.GET", "X") == [3,"2.1"]
        assert r.execute_command("TS.GET", "X", "TIMESTAMP", "1") == [2,"1.2"]
        assert r.execute_command("TS.GET", "X", "TIMESTAMP", "2") == [3,"2.1"]
        assert r.execute_command("TS.GET", "X", "TIMESTAMP", "3") == []
        
# TODO add negative timestamp test         

# TODO add blocking test with result

# TODO add blocking test with no result (before timeout)

# TODO add blocking test with (timeout passed)

# TODO add negative blocking test

