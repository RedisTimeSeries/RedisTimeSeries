import math
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


def test_incrby_create_new_series():
    """Test that TS.INCRBY can create a new series with all creation options"""
    with Env().getClusterConnectionIfNeeded() as r:
        # Test creating new series with all options
        key = 'test_incrby_create'
        r.delete(key)
        
        # Create series with TS.INCRBY and all creation parameters
        result = r.execute_command('TS.INCRBY', key, '10.5', 
                                 'TIMESTAMP', '1000',
                                 'RETENTION', '86400000',  # 1 day
                                 'ENCODING', 'COMPRESSED',
                                 'CHUNK_SIZE', '4096',
                                 'DUPLICATE_POLICY', 'LAST',
                                 'LABELS', 'sensor', 'temperature', 'location', 'room1')
        assert result == 1000
        
        # Verify series was created with correct properties
        info = r.execute_command('TS.INFO', key)
        info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}
        
        assert info_dict[b'retentionTime'] == 86400000
        assert info_dict[b'chunkSize'] == 4096
        assert info_dict[b'duplicatePolicy'] == b'last'
        assert info_dict[b'labels'] == [[b'sensor', b'temperature'], [b'location', b'room1']]
        
        # Verify the value
        result = r.execute_command('TS.GET', key)
        assert result == [1000, b'10.5']


def test_incrby_with_ignore_policy():
    """Test TS.INCRBY with IGNORE policy for duplicate samples"""
    with Env().getClusterConnectionIfNeeded() as r:
        key = 'test_incrby_ignore'
        r.delete(key)
        
        # Create series with IGNORE policy
        r.execute_command('TS.INCRBY', key, '5.0', 
                         'TIMESTAMP', '1000',
                         'DUPLICATE_POLICY', 'LAST',
                         'IGNORE', '100', '1.0')  # ignore if time diff <= 100ms and value diff <= 1.0
        
        # Add another increment that should be ignored (same timestamp, small value diff)
        # The increment of 0.5 is smaller than the ignore threshold of 1.0, so it should be ignored
        r.execute_command('TS.INCRBY', key, '0.5', 'TIMESTAMP', '1000')
        
        # Verify only one sample exists and value remains 5.0 (not incremented)
        result = r.execute_command('TS.RANGE', key, 0, 2000)
        assert len(result) == 1
        assert result[0] == [1000, b'5.0']  # Original value, increment was ignored
        
        # Now add an increment larger than the ignore threshold
        r.execute_command('TS.INCRBY', key, '1.5', 'TIMESTAMP', '1000')
        
        # This should not be ignored and should increment the value
        result = r.execute_command('TS.RANGE', key, 0, 2000)
        assert len(result) == 1
        assert result[0] == [1000, b'6.5']  # 5.0 + 1.5


def test_incrby_duplicate_policies():
    """Test TS.INCRBY with different duplicate policies"""
    with Env().getClusterConnectionIfNeeded() as r:
        # Test BLOCK policy
        key_block = 'test_incrby_block'
        r.delete(key_block)
        r.execute_command('TS.INCRBY', key_block, '10', 'TIMESTAMP', '1000', 'DUPLICATE_POLICY', 'BLOCK')
        
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command('TS.INCRBY', key_block, '5', 'TIMESTAMP', '1000')
        assert 'DUPLICATE_POLICY' in str(excinfo.value) or 'duplicate' in str(excinfo.value).lower()
        
        # Test SUM policy
        key_sum = 'test_incrby_sum'
        r.delete(key_sum)
        r.execute_command('TS.INCRBY', key_sum, '10', 'TIMESTAMP', '1000', 'DUPLICATE_POLICY', 'SUM')
        r.execute_command('TS.INCRBY', key_sum, '5', 'TIMESTAMP', '1000')
        
        result = r.execute_command('TS.GET', key_sum)
        assert result == [1000, b'25']  # 10 + (10+5) = 25
        
        # Test MIN policy
        key_min = 'test_incrby_min'
        r.delete(key_min)
        r.execute_command('TS.INCRBY', key_min, '10', 'TIMESTAMP', '1000', 'DUPLICATE_POLICY', 'MIN')
        r.execute_command('TS.INCRBY', key_min, '5', 'TIMESTAMP', '1000')  # results in 15
        
        result = r.execute_command('TS.GET', key_min)
        assert result == [1000, b'10']  # min(10, 15) = 10
        
        # Test MAX policy
        key_max = 'test_incrby_max'
        r.delete(key_max)
        r.execute_command('TS.INCRBY', key_max, '10', 'TIMESTAMP', '1000', 'DUPLICATE_POLICY', 'MAX')
        r.execute_command('TS.INCRBY', key_max, '5', 'TIMESTAMP', '1000')  # results in 15
        
        result = r.execute_command('TS.GET', key_max)
        assert result == [1000, b'15']  # max(10, 15) = 15


def test_incrby_empty_series_behavior():
    """Test TS.INCRBY behavior with empty series"""
    with Env().getClusterConnectionIfNeeded() as r:
        key = 'test_incrby_empty'
        r.delete(key)
        
        # First increment on empty series should set value to addend
        result = r.execute_command('TS.INCRBY', key, '42.5', 'TIMESTAMP', '1000')
        assert result == 1000
        
        sample = r.execute_command('TS.GET', key)
        assert sample == [1000, b'42.5']
        
        # Second increment should add to the existing value
        result = r.execute_command('TS.INCRBY', key, '7.5', 'TIMESTAMP', '2000')
        assert result == 2000
        
        sample = r.execute_command('TS.GET', key)
        assert sample == [2000, b'50']  # 42.5 + 7.5


def test_incrby_negative_values():
    """Test TS.INCRBY with negative addend values"""
    with Env().getClusterConnectionIfNeeded() as r:
        key = 'test_incrby_negative'
        r.delete(key)
        
        # Start with positive increment
        r.execute_command('TS.INCRBY', key, '100', 'TIMESTAMP', '1000')
        
        # Add negative increment (effectively decrement)
        r.execute_command('TS.INCRBY', key, '-30', 'TIMESTAMP', '2000')
        
        result = r.execute_command('TS.GET', key)
        assert result == [2000, b'70']  # 100 + (-30) = 70


def test_incrby_floating_point_precision():
    """Test TS.INCRBY with floating point values"""
    with Env().getClusterConnectionIfNeeded() as r:
        key = 'test_incrby_float'
        r.delete(key)
        
        # Test with floating point increments
        r.execute_command('TS.INCRBY', key, '0.1', 'TIMESTAMP', '1000')
        r.execute_command('TS.INCRBY', key, '0.2', 'TIMESTAMP', '2000')
        r.execute_command('TS.INCRBY', key, '0.3', 'TIMESTAMP', '3000')
        
        result = r.execute_command('TS.GET', key)
        # Note: floating point precision might cause small differences
        assert abs(float(result[1]) - 0.6) < 0.0001


def test_incrby_timestamp_validation():
    """Test TS.INCRBY timestamp validation"""
    with Env().getClusterConnectionIfNeeded() as r:
        key = 'test_incrby_timestamp'
        r.delete(key)
        
        # Add sample with timestamp 2000
        r.execute_command('TS.INCRBY', key, '10', 'TIMESTAMP', '2000')
        
        # Try to add sample with earlier timestamp - should fail
        with pytest.raises(redis.ResponseError) as excinfo:
            r.execute_command('TS.INCRBY', key, '5', 'TIMESTAMP', '1000')
        assert 'timestamp must be equal to or higher' in str(excinfo.value).lower()
        
        # Adding with same timestamp should work (updates existing)
        result = r.execute_command('TS.INCRBY', key, '5', 'TIMESTAMP', '2000')
        assert result == 2000
        
        sample = r.execute_command('TS.GET', key)
        assert sample == [2000, b'15']  # 10 + 5


def test_incrby_automatic_timestamp():
    """Test TS.INCRBY with automatic timestamp generation"""
    with Env().getClusterConnectionIfNeeded() as r:
        key = 'test_incrby_auto_ts'
        r.delete(key)
        
        # Test without TIMESTAMP parameter (should use current time)
        start_time = int(time.time() * 1000)
        result = r.execute_command('TS.INCRBY', key, '25')
        end_time = int(time.time() * 1000)
        
        # Timestamp should be within reasonable range of current time
        assert start_time <= result <= end_time + 1000  # Allow 1 second tolerance
        
        # Test with TIMESTAMP '*' (should also use current time)
        start_time = int(time.time() * 1000)
        result = r.execute_command('TS.INCRBY', key, '15', 'TIMESTAMP', '*')
        end_time = int(time.time() * 1000)
        
        assert start_time <= result <= end_time + 1000


def test_incrby_with_compaction_rules():
    """Test TS.INCRBY with compaction rules"""
    with Env().getClusterConnectionIfNeeded() as r:
        source_key = 'test_incrby_source'
        dest_key = 'test_incrby_dest'
        r.delete(source_key, dest_key)
        
        # Create source series and destination with compaction rule
        r.execute_command('TS.CREATE', source_key)
        r.execute_command('TS.CREATE', dest_key)
        r.execute_command('TS.CREATERULE', source_key, dest_key, 'AGGREGATION', 'sum', '1000')
        
        # Add increments to source series
        r.execute_command('TS.INCRBY', source_key, '10', 'TIMESTAMP', '1000')
        r.execute_command('TS.INCRBY', source_key, '20', 'TIMESTAMP', '1500')
        r.execute_command('TS.INCRBY', source_key, '30', 'TIMESTAMP', '2000')  # This should trigger compaction
        
        # Check that compaction rule was applied
        time.sleep(0.1)  # Give compaction time to process
        dest_result = r.execute_command('TS.RANGE', dest_key, 0, 3000)
        
        # Should have compacted data in destination
        assert len(dest_result) >= 1


def test_incrby_error_cases():
    """Test TS.INCRBY error handling"""
    with Env().getClusterConnectionIfNeeded() as r:
        # Test with wrong number of arguments
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.INCRBY')
        
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.INCRBY', 'key')
        
        # Test with invalid addend value
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.INCRBY', 'key', 'not_a_number')
        
        # Test with invalid timestamp
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.INCRBY', 'key', '10', 'TIMESTAMP', 'not_a_timestamp')
        
        # Test with wrong key type
        r.set('string_key', 'value')
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.INCRBY', 'string_key', '10')
