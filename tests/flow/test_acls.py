import math
import time

import pytest
import redis
from RLTest import Env
import redis.exceptions
from includes import *


def test_acl_with_ts_mget_mrange():
    env = Env()
    conn = env.getConnection()

    # Create two time series with labels
    conn.execute_command('TS.CREATE', 'series1', 'LABELS', 'group', 'test', 'name', 'series1')
    conn.execute_command('TS.CREATE', 'series2', 'LABELS', 'group', 'test', 'name', 'series2')

    # Add data to both series
    conn.execute_command('TS.ADD', 'series1', '*', 1)
    conn.execute_command('TS.ADD', 'series2', '*', 2)

    # Create a user with read permissions only for 'series1'
    conn.execute_command(
        'ACL', 'SETUSER', 'testuser',
        'on', '>password',
        '+ACL', '+TS.MGET', '+TS.MRANGE', '+TS.MREVRANGE', '+TS.INFO',
        '+TS.CREATERULE', '+TS.DELETERULE', '+TS.QUERYINDEX',
        '+@read',
        '~series1'
    )

    # Authenticate as 'testuser'
    conn.execute_command('AUTH', 'testuser', 'password')

    result = conn.execute_command('TS.INFO', 'series1')
    env.assertGreaterEqual(len(result), 1)

    with pytest.raises(redis.exceptions.NoPermissionError):
        result = conn.execute_command('TS.INFO', 'series2')

    with pytest.raises(redis.exceptions.NoPermissionError):
        result = conn.execute_command('TS.QUERYINDEX', 'group=test')

    with pytest.raises(redis.exceptions.NoPermissionError):
        result = conn.execute_command('TS.MGET', 'FILTER', 'group=test')

    with pytest.raises(redis.exceptions.NoPermissionError):
        result = conn.execute_command('TS.MRANGE', '-', '+', 'FILTER', 'group=test')

    with pytest.raises(redis.exceptions.NoPermissionError):
        result = conn.execute_command('TS.MRANGE', '-', '+', 'FILTER', 'group=test', 'GROUPBY', 'name', 'REDUCE', 'MAX')

    with pytest.raises(redis.exceptions.NoPermissionError):
        result = conn.execute_command('TS.MREVRANGE', '-', '+', 'FILTER', 'group=test')

    with pytest.raises(redis.exceptions.NoPermissionError):
        result = conn.execute_command('TS.MREVRANGE', '-', '+', 'FILTER', 'group=test', 'GROUPBY', 'name', 'REDUCE', 'MAX')

    with pytest.raises(redis.exceptions.NoPermissionError):
        result = conn.execute_command('TS.CREATERULE', 'series1', 'series2', 'AGGREGATION', 'AVG', '1')

    with pytest.raises(redis.exceptions.NoPermissionError):
        result = conn.execute_command('TS.CREATERULE', 'series2', 'series1', 'AGGREGATION', 'AVG', '1')

    # Now change the ACL to allow access to both series
    conn.execute_command('ACL', 'SETUSER', 'testuser', '+@read', '~series1', '~series2')

    result = conn.execute_command('TS.INFO', 'series1')
    env.assertGreaterEqual(len(result), 1)
    result = conn.execute_command('TS.INFO', 'series2')
    env.assertGreaterEqual(len(result), 1)

    result = conn.execute_command('TS.MGET', 'FILTER', 'group=test')
    env.assertEqual(len(result), 2, message="TS.MGET should have returned two series")

    result = conn.execute_command('TS.MRANGE', '-', '+', 'FILTER', 'group=test')
    env.assertEqual(len(result), 2, message="TS.MRANGE should have returned two series")
    result = conn.execute_command('TS.MRANGE', '-', '+', 'FILTER', 'group=test', 'GROUPBY', 'name', 'REDUCE', 'MAX')
    env.assertEqual(len(result), 2, message="TS.MRANGE should have returned two series")

    result = conn.execute_command('TS.MREVRANGE', '-', '+', 'FILTER', 'group=test')
    env.assertEqual(len(result), 2, message="TS.MREVRANGE should have returned two series")
    result = conn.execute_command('TS.MREVRANGE', '-', '+', 'FILTER', 'group=test', 'GROUPBY', 'name', 'REDUCE', 'MAX')
    env.assertEqual(len(result), 2, message="TS.MREVRANGE should have returned two series")

    result = conn.execute_command('TS.QUERYINDEX', 'group=test')
    env.assertEqual(len(result), 2, message="TS.QUERYINDEX should have returned two series")

    result = conn.execute_command('TS.CREATERULE', 'series1', 'series2', 'AGGREGATION', 'AVG', '1')
    env.assertEqual(result, b'OK')
    result = conn.execute_command('TS.DELETERULE', 'series1', 'series2')
    env.assertEqual(result, b'OK')

    result = conn.execute_command('TS.CREATERULE', 'series2', 'series1', 'AGGREGATION', 'AVG', '1')
    env.assertEqual(result, b'OK')
    result = conn.execute_command('TS.DELETERULE', 'series2', 'series1')
    env.assertEqual(result, b'OK')

    # Clean up
    conn.execute_command('AUTH', 'default', '')
    conn.execute_command('ACL', 'DELUSER', 'testuser')
