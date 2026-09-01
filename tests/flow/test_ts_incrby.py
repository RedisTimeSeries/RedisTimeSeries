import math
import threading
import time

# import pytest
# import redis
# from utils import Env
from includes import *
from test_helper_classes import _get_ts_info


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


def test_incrby_create_with_labels_replicates_correctly():
    # When TIMESTAMP is omitted, replication rewrites the command with a
    # resolved "TIMESTAMP <ts>" (see
    # test_incrby_no_timestamp_replicates_diverging_timestamp). Because
    # LABELS greedily consumes every token that follows it, TIMESTAMP has to
    # be inserted *before* a trailing LABELS clause - appending it would be
    # swallowed as a bogus label/value pair on replicas and in AOF replay.
    if not Env().useSlaves:
        Env().skip()
    Env().skipOnCluster()
    env = Env()
    warmup_key = 'incrby_create_with_labels_warmup'
    with env.getConnection() as master:
        master.execute_command('ts.create', warmup_key)
    # Make sure the replica is already online and streaming live-propagated
    # commands (rather than picking up our key via a fresh full RDB sync)
    # before we issue the create-on-write TS.INCRBY below.
    for _ in range(100):
        with env.getSlaveConnection() as slave:
            if slave.execute_command('exists', warmup_key):
                break
        time.sleep(0.1)
    else:
        raise Exception('replica never caught up with warmup key')

    for cmd in ('ts.incrby', 'ts.decrby'):
        key = 'incrby_create_with_labels_%s' % cmd
        with env.getConnection() as master:
            master.execute_command(cmd, key, 1, 'LABELS', 'region', 'us')
            master_sample = master.execute_command('ts.get', key)
        for _ in range(100):
            with env.getSlaveConnection() as slave:
                if slave.execute_command('exists', key):
                    break
            time.sleep(0.1)
        else:
            raise Exception('replica never caught up with initial %s' % cmd)
        with env.getSlaveConnection() as slave:
            info = _get_ts_info(slave, key)
            slave_sample = slave.execute_command('ts.get', key)
        env.assertEqual(info.labels, {b'region': b'us'})
        env.assertEqual(master_sample, slave_sample)

    # Also exercise the AOF-replay path (not just the live replication
    # stream): restart master and replica and reload from AOF/RDB, then
    # verify the data survived intact.
    env.restartAndReload()
    for cmd in ('ts.incrby', 'ts.decrby'):
        key = 'incrby_create_with_labels_%s' % cmd
        with env.getConnection() as master:
            master_info = _get_ts_info(master, key)
            master_sample = master.execute_command('ts.get', key)
        with env.getSlaveConnection() as slave:
            slave_info = _get_ts_info(slave, key)
            slave_sample = slave.execute_command('ts.get', key)
        env.assertEqual(master_info.labels, {b'region': b'us'})
        env.assertEqual(slave_info.labels, {b'region': b'us'})
        env.assertEqual(master_sample, slave_sample)


def test_incrby_create_with_key_named_labels_replicates_correctly():
    # RMUtil_ArgIndex scans the whole argv, so a key literally named
    # "LABELS" (case-insensitively) must not be mistaken for a LABELS
    # clause when deciding where to insert the resolved TIMESTAMP.
    if not Env().useSlaves:
        Env().skip()
    Env().skipOnCluster()
    env = Env()
    key = 'LABELS'
    with env.getConnection() as master:
        master.execute_command('ts.incrby', key, 1)
        master_sample = master.execute_command('ts.get', key)
    for _ in range(100):
        with env.getSlaveConnection() as slave:
            if slave.execute_command('exists', key):
                break
        time.sleep(0.1)
    else:
        raise Exception('replica never caught up with initial ts.incrby')
    with env.getSlaveConnection() as slave:
        slave_sample = slave.execute_command('ts.get', key)
    env.assertEqual(master_sample, slave_sample)


def test_incrby_labels_key_named_timestamp_replicates_correctly():
    # A label whose key is literally "TIMESTAMP" collides with the reserved
    # keyword: RMUtil_ArgIndex's naive scan for "TIMESTAMP" would otherwise
    # match this label key instead of a real TIMESTAMP directive, and the
    # resulting rewritten command must still parse to the same labels on
    # replicas as it did on the primary.
    if not Env().useSlaves:
        Env().skip()
    Env().skipOnCluster()
    env = Env()
    key = 'incrby_labels_key_named_timestamp'
    with env.getConnection() as master:
        master.execute_command('ts.incrby', key, 1, 'LABELS', 'a', 'b', 'TIMESTAMP', '*')
        master_info = _get_ts_info(master, key)
        master_sample = master.execute_command('ts.get', key)
    for _ in range(100):
        with env.getSlaveConnection() as slave:
            if slave.execute_command('exists', key):
                break
        time.sleep(0.1)
    else:
        raise Exception('replica never caught up with initial ts.incrby')
    with env.getSlaveConnection() as slave:
        slave_info = _get_ts_info(slave, key)
        slave_sample = slave.execute_command('ts.get', key)
    env.assertEqual(master_info.labels, slave_info.labels)
    env.assertEqual(master_sample, slave_sample)


def test_incrby_timestamp_missing_value_returns_error():
    # TIMESTAMP as the very last token has no value after it; this must be
    # rejected cleanly instead of reading one RedisModuleString past the end
    # of argv.
    with Env().getClusterConnectionIfNeeded() as r:
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.INCRBY', 'incrby_timestamp_missing_value', '5', 'TIMESTAMP')
