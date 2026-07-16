import math
import threading
import time

# import pytest
# import redis
# from utils import Env
from includes import *


def test_incrby():
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'tester')

        start_incr_time = int(time.time() * 1000)
        for i in range(20):
            r.execute_command('ts.incrby', 'tester', '5')
            time.sleep(0.001)

        start_decr_time = int(time.time() * 1000)
        for i in range(20):
            r.execute_command('ts.decrby', 'tester', '1.5')
            time.sleep(0.001)

        now = int(time.time() * 1000)
        result = r.execute_command('TS.RANGE', 'tester', 0, now)
        assert result[-1][1] == b'70'
        assert result[-1][0] <= now
        assert result[0][0] >= start_incr_time
        assert len(result) <= 40


def test_incrby_with_timestamp():
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'tester')

        for i in range(20):
            assert r.execute_command('ts.incrby', 'tester', '5', 'TIMESTAMP', i) == i
        result = r.execute_command('TS.RANGE', 'tester', 0, 20)
        assert len(result) == 20
        assert result[19][1] == b'100'

        query_res = r.execute_command('ts.incrby', 'tester', '5', 'TIMESTAMP', '*')
        query_res = math.floor(query_res / 1000)  # To seconds
        assert time.time() >= query_res

        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('ts.incrby', 'tester', '5', 'TIMESTAMP', '10')


def test_incrby_with_update_latest():
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'tester')
        for i in range(1, 21):
            assert r.execute_command('ts.incrby', 'tester', '5', 'TIMESTAMP', i) == i

        result = r.execute_command('TS.RANGE', 'tester', 0, 20)
        assert len(result) == 20
        assert result[19] == [20, b'100']

        assert r.execute_command('ts.incrby', 'tester', '5', 'TIMESTAMP', 20) == i
        result = r.execute_command('TS.RANGE', 'tester', 0, 20)
        assert len(result) == 20
        assert result[19] == [20, b'105']

        assert r.execute_command('ts.decrby', 'tester', '10', 'TIMESTAMP', 20) == i
        result = r.execute_command('TS.RANGE', 'tester', 0, 20)
        assert len(result) == 20
        assert result[19] == [20, b'95']


def test_incrby_error_cases():
    """Minimal test for TS.INCRBY error handling (cluster-compatible)"""
    with Env().getClusterConnectionIfNeeded() as r:
        # Test with wrong number of arguments - cluster-compatible approach
        if hasattr(r, 'nodes_manager'):  # Redis cluster
            with pytest.raises(redis.ResponseError):
                r.execute_command('TS.INCRBY', target_nodes=r.get_default_node())
        else:  # Single node Redis
            with pytest.raises(redis.ResponseError):
                r.execute_command('TS.INCRBY')
        
        # Test with invalid addend value
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.INCRBY', 'test_key', 'not_a_number')

def test_ts_incrby_NaN():
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'tester')
        r.execute_command('ts.add', 'tester', 1, 'nan')

        # Add a number to a NaN value, error expected
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.incrby', 'tester', '1')
            r.execute_command('TS.decrby', 'tester', '1')
        
        r.execute_command('ts.add', 'tester', 2,  1)
        # Add a NaN value to a number, error expected
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.incrby', 'tester', 'nan')
            r.execute_command('TS.decrby', 'tester', 'nan')

def test_ts_incrby_arg_validation_before_creation():
    # This test ensures that the key is not created if validation fails (MOD-8167)
    with Env().getClusterConnectionIfNeeded() as r:
        # Test 1: Invalid value should not create the key
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.INCRBY', 'test_invalid_value', 'foo')
        # Key should not exist
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.GET', 'test_invalid_value')
        
        # Test 2: Invalid timestamp should not create the key
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.INCRBY', 'test_invalid_ts', '5', 'TIMESTAMP', 'invalid')
        # Key should not exist
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.GET', 'test_invalid_ts')
        
        # Test 3: Valid command should create the key
        r.execute_command('TS.INCRBY', 'test_valid', '5')
        # Key should exist
        result = r.execute_command('TS.GET', 'test_valid')
        assert result is not None


def test_incrby_no_timestamp_replicates_diverging_timestamp():
    # TS.INCRBY/TS.DECRBY without an explicit TIMESTAMP resolve the timestamp
    # via RedisModule_Milliseconds() on the primary, but TSDB_incrby then
    # calls RedisModule_ReplicateVerbatim(), propagating the original argv
    # (still lacking a concrete timestamp) instead of a rewritten one like
    # TS.ADD/TS.MADD do. A replica that applies the propagated command later
    # re-evaluates RedisModule_Milliseconds() at its own, later, wall-clock
    # instant, so it stores the sample under a different timestamp than the
    # primary did for the same logical write.
    if not Env().useSlaves:
        Env().skip()
    Env().skipOnCluster()
    env = Env()
    key = 'incrby_repl_divergence'
    with env.getConnection() as master:
        master.execute_command('ts.create', key)
    # Make sure the initial replica sync has caught up with the key creation
    # before we start controlling timing below, so the only delay measured
    # is the one we inject, not leftover initial-sync latency.
    for _ in range(100):
        with env.getSlaveConnection() as slave:
            if slave.execute_command('exists', key):
                break
        time.sleep(0.1)
    else:
        raise Exception('replica never caught up with initial ts.create')
    slave_sleep_secs = 2
    def block_slave_main_thread():
        # DEBUG SLEEP blocks the replica's single event loop entirely, so it
        # cannot apply the replicated TS.INCRBY until the sleep elapses -
        # forcing a deterministic, multi-second gap between when the primary
        # resolves "now" and when the replica would resolve it.
        with env.getSlaveConnection() as slave:
            slave.execute_command('DEBUG', 'SLEEP', slave_sleep_secs)
    blocker = threading.Thread(target=block_slave_main_thread)
    blocker.start()
    time.sleep(0.3)  # let DEBUG SLEEP actually start running on the slave
    with env.getConnection() as master:
        master_ts = master.execute_command('ts.incrby', key, 1)
    blocker.join()
    time.sleep(1)  # give the replica time to apply the now-unblocked command
    with env.getSlaveConnection() as slave:
        slave_sample = slave.execute_command('ts.get', key)
    env.assertEqual(master_ts, slave_sample[0])