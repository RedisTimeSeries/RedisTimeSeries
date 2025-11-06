"""
Test that verifies RDB load failure handling for corrupted/broken RDB files.
This test creates various types of broken RDB files and ensures they fail to load properly.
"""

from RLTest import Env
from includes import *


def test_broken_rdb_truncated(env):
    """
    Test that a truncated RDB file fails to load.
    This simulates a scenario where the RDB file is incomplete.
    """
    env.skipOnCluster()

    env.cmd('TS.CREATE', 'test_key', 'RETENTION', '1000', 'CHUNK_SIZE', '1024',
            'LABELS', 'name', 'test', 'type', 'broken_rdb')
    env.cmd('TS.ADD', 'test_key', 100, 10.5)
    env.cmd('TS.ADD', 'test_key', 200, 20.5)
    env.cmd('TS.ADD', 'test_key', 300, 30.5)

    valid_dump = env.cmd('DUMP', 'test_key')

    corrupted_dump = valid_dump[:len(valid_dump)//2]

    env.cmd('DEL', 'test_key')

    env.expect('RESTORE', 'test_key', 0, corrupted_dump).error().contains("DUMP payload version or checksum are wrong")


def test_broken_rdb_corrupted_data(env):
    """
    Test that an RDB file with corrupted data fails to load.
    This simulates bit flips or data corruption.
    """
    env.skipOnCluster()

    env.cmd('TS.CREATE', 'test_key', 'CHUNK_SIZE', '128')

    for i in range(100):
        env.cmd('TS.ADD', 'test_key', 1000 + i * 10, float(i))

    valid_dump = env.cmd('DUMP', 'test_key')

    dump_bytes = bytearray(valid_dump)
    corruption_start = len(dump_bytes) // 2
    corruption_end = corruption_start + 20
    for i in range(corruption_start, min(corruption_end, len(dump_bytes))):
        dump_bytes[i] = (dump_bytes[i] + 1) % 256
    corrupted_dump = bytes(dump_bytes)

    env.cmd('DEL', 'test_key')

    env.expect('RESTORE', 'test_key', 0, corrupted_dump).error().contains("DUMP payload version or checksum are wrong")


def test_broken_rdb_invalid_chunk_count(env):
    """
    Test that an RDB file with invalid chunk count fails to load.
    This simulates corruption in the metadata.
    """
    env.skipOnCluster()

    env.cmd('TS.CREATE', 'test_key', 'UNCOMPRESSED')
    env.cmd('TS.ADD', 'test_key', 100, 1.0)
    env.cmd('TS.ADD', 'test_key', 200, 2.0)

    valid_dump = env.cmd('DUMP', 'test_key')

    dump_bytes = bytearray(valid_dump)
    if len(dump_bytes) > 10:
        dump_bytes[-10] = 255
        dump_bytes[-9] = 255
    corrupted_dump = bytes(dump_bytes)

    env.cmd('DEL', 'test_key')

    env.expect('RESTORE', 'test_key', 0, corrupted_dump).error().contains("DUMP payload version or checksum are wrong")


def test_broken_rdb_empty_dump(env):
    """
    Test that an empty dump fails to restore.
    """
    env.skipOnCluster()

    env.expect('RESTORE', 'test_key', 0, b'').error().contains("DUMP payload version or checksum are wrong")


def test_broken_rdb_with_rules(env):
    """
    Test that a corrupted RDB with compaction rules fails properly.
    """
    env.skipOnCluster()

    env.cmd('TS.CREATE', 'test_key', 'CHUNK_SIZE', '256')
    env.cmd('TS.CREATE', 'dest')
    env.cmd('TS.CREATERULE', 'test_key', 'dest', 'AGGREGATION', 'AVG', 100)

    for i in range(20):
        env.cmd('TS.ADD', 'test_key', 1000 + i * 10, float(i))

    valid_dump = env.cmd('DUMP', 'test_key')

    dump_bytes = bytearray(valid_dump)
    if len(dump_bytes) > 30:
        for i in range(20, 30):
            dump_bytes[i] = (dump_bytes[i] ^ 0xFF) % 256
    corrupted_dump = bytes(dump_bytes)

    env.cmd('DEL', 'test_key')

    env.expect('RESTORE', 'test_key', 0, corrupted_dump).error().contains("DUMP payload version or checksum are wrong")


def test_broken_rdb_invalid_encoding_version(env):
    """
    Test that an RDB with invalid encoding version is rejected.
    """
    env.skipOnCluster()

    env.cmd('TS.CREATE', 'test_key')
    env.cmd('TS.ADD', 'test_key', 100, 1.0)

    valid_dump = env.cmd('DUMP', 'test_key')

    dump_bytes = bytearray(valid_dump)
    if len(dump_bytes) > 5:
        dump_bytes[0] = 255
        dump_bytes[1] = 255
    corrupted_dump = bytes(dump_bytes)

    env.cmd('DEL', 'test_key')

    env.expect('RESTORE', 'test_key', 0, corrupted_dump).error().contains("DUMP payload version or checksum are wrong")
