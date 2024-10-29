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
        '+TS.MGET', '+TS.MRANGE', '+TS.MREVRANGE', '+@read',
        '~series1'
    )

    # Authenticate as 'testuser'
    conn.execute_command('AUTH', 'testuser', 'password')

    # TS.MGET with FILTER that includes both series
    result = conn.execute_command('TS.MGET', 'FILTER', 'group=test')
    env.assertEqual(len(result), 1, message="TS.MGET should have returned only one series due to ACL restrictions")
    env.assertEqual(result[0][0], b'series1', message="TS.MGET returned the wrong series")

    with pytest.raises(redis.exceptions.NoPermissionError):
        # TS.MRANGE with FILTER that includes both series
        result = conn.execute_command('TS.MRANGE', '-', '+', 'FILTER', 'group=test')
        env.assertEqual(len(result), 1, message="TS.MRANGE should have returned only one series due to ACL restrictions")
        env.assertEqual(result[0][0], b'series1', message="TS.MRANGE returned the wrong series")

    with pytest.raises(redis.exceptions.NoPermissionError):
        # TS.MREVRANGE with FILTER that includes both series
        result = conn.execute_command('TS.MREVRANGE', '-', '+', 'FILTER', 'group=test')
        env.assertEqual(len(result), 1, message="TS.MREVRANGE should have returned only one series due to ACL restrictions")
        env.assertEqual(result[0][0], b'series1', message="TS.MREVRANGE returned the wrong series")

    # Now change the ACL to allow access to both series
    conn.execute_command('ACL', 'SETUSER', 'testuser', '+@read', '~series1', '~series2')

    # Try TS.MGET again
    result = conn.execute_command('TS.MGET', 'FILTER', 'group=test')
    env.assertEqual(len(result), 2, message="TS.MGET should have returned two series")

    # Clean up
    conn.execute_command('AUTH', 'default', '')
    conn.execute_command('ACL', 'DELUSER', 'testuser')
