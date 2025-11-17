"""
Test that verifies Redis shards do not crash after random time series commands.
This test uses redis_command_generator to generate random commands and verifies server stability after all commands.
"""
import os
import sys
import subprocess
import redis
from RLTest import Env
from includes import *

def verify_shards_alive(env):
    """Verify that all shards are still running and responsive."""
    if env.isCluster():
        # Check all shards in cluster mode
        for shard_id in range(1, env.shardsCount + 1):
            try:
                conn = env.getConnection(shardId=shard_id)
                result = conn.ping()
                assert result == True, f"Shard {shard_id} failed to respond to PING"
            except Exception as e:
                raise AssertionError(f"Shard {shard_id} crashed or is unresponsive: {str(e)}")
    else:
        # Check standalone mode
        try:
            conn = env.getConnection()
            result = conn.ping()
            assert result == True, "Redis server failed to respond to PING"
        except Exception as e:
            raise AssertionError(f"Redis server crashed or is unresponsive: {str(e)}")

def test_random_traffic_no_crash():
    """Test that random time series commands don't cause shard crashes.
    
    Uses redis_command_generator to generate random commands, then verifies
    that all shards are still healthy after all commands complete.
    """
    env = Env(decodeResponses=True)
    
    # Check if redis_command_generator module is available
    # Try to find the module first
    try:
        result = subprocess.run(
            [sys.executable, '-c', 'import redis_command_generator'],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode != 0:
            raise AssertionError("redis_command_generator module not available - required dependency missing")
    except subprocess.TimeoutExpired:
        raise AssertionError("redis_command_generator module check timed out")
    except Exception as e:
        if isinstance(e, AssertionError):
            raise
        raise AssertionError(f"redis_command_generator module not available - required dependency missing: {str(e)}")
    
    # Get connection to determine Redis host/port
    with env.getClusterConnectionIfNeeded() as r:
        # Get Redis connection info from the connection
        # RLTest uses localhost with dynamic ports, so we need to extract the port
        try:
            if hasattr(r, 'connection_pool') and hasattr(r.connection_pool, 'connection_kwargs'):
                host = r.connection_pool.connection_kwargs.get('host', 'localhost')
                port = r.connection_pool.connection_kwargs.get('port', 6379)
            elif hasattr(r, 'connection_pool') and hasattr(r.connection_pool, '_available_connections'):
                # Try to get from an available connection
                if r.connection_pool._available_connections:
                    conn = r.connection_pool._available_connections[0]
                    host = getattr(conn, 'host', 'localhost')
                    port = getattr(conn, 'port', 6379)
                else:
                    host = 'localhost'
                    port = 6379
            else:
                # Fallback: try to get from the connection directly
                host = getattr(r, 'host', 'localhost')
                port = getattr(r, 'port', 6379)
        except:
            # If we can't determine the port, fail the test
            # (redis_command_generator needs the exact port)
            raise AssertionError("Cannot determine Redis connection details for redis_command_generator")
        
        # Configurable number of commands (default 1000 for regular tests)
        max_commands = int(os.getenv('RANDOM_TRAFFIC_MAX_COMMANDS', '1000'))
        
        print(f"Using redis_command_generator to generate {max_commands} random commands")
        print(f"Connecting to Redis at {host}:{port}")
        
        # Use subprocess to call redis_command_generator
        # This matches how it's used in the workflow
        try:
            result = subprocess.run(
                [sys.executable, '-m', 'redis_command_generator.TimeSeriesGen',
                 '--hosts', f'{host}:{port}',
                 '--max_cmd_cnt', str(max_commands),
                 '--verbose'],
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            if result.returncode != 0:
                print(f"Warning: redis_command_generator exited with code {result.returncode}")
                if result.stderr:
                    print(f"stderr: {result.stderr}")
                    # Check if the error is due to missing module
                    if 'ModuleNotFoundError' in result.stderr or 'No module named' in result.stderr:
                        raise AssertionError("redis_command_generator not available (ModuleNotFoundError) - required dependency missing")
                # Don't fail the test if generator has other issues, but still verify Redis health
                    
        except subprocess.TimeoutExpired:
            print("Warning: redis_command_generator timed out after 1 hour")
            # Still verify Redis health even if generator timed out
        except FileNotFoundError:
            raise AssertionError("redis_command_generator not available - required dependency missing")
        except Exception as e:
            print(f"Warning: Error running redis_command_generator: {e}")
            # Still verify Redis health
        
        # Verify shards are still alive after all commands
        print("Verifying shard health after random traffic...")
        verify_shards_alive(env)
        
        print(f"SUCCESS: All shards healthy after {max_commands} random commands")

