"""
Test that verifies RDB load failure handling for corrupted/broken RDB files.
This test creates various types of broken RDB files and ensures they fail to load properly.
"""

from RLTest import Env
from includes import *

def _crc_reflect64(x: int) -> int:
    x &= 0xFFFFFFFFFFFFFFFF
    # Reverse bits in 64-bit word.
    x = ((x >> 1) & 0x5555555555555555) | ((x & 0x5555555555555555) << 1)
    x = ((x >> 2) & 0x3333333333333333) | ((x & 0x3333333333333333) << 2)
    x = ((x >> 4) & 0x0F0F0F0F0F0F0F0F) | ((x & 0x0F0F0F0F0F0F0F0F) << 4)
    # byte-swap
    x = int.from_bytes(x.to_bytes(8, "little"), "big", signed=False)
    return x

def _redis_crc64(data: bytes) -> int:
    # Matches Redis' crc64(0, data, len) implementation in redis/src/crc64.c
    # (poly=0xad93d23594c935a9, no xor-in in the callsites, reflect-out).
    poly = 0xAD93D23594C935A9
    crc = 0
    for c in data:
        i = 0x01
        while i & 0xFF:
            bit = (crc >> 63) & 1
            if c & i:
                bit ^= 1
            crc = (crc << 1) & 0xFFFFFFFFFFFFFFFF
            if bit:
                crc ^= poly
            i <<= 1
    return _crc_reflect64(crc)

def _verify_dump_payload(buf: bytes) -> bool:
    # Matches Redis' verifyDumpPayload(): CRC is over payload+2-byte version (len-8).
    if len(buf) < 10:
        return False
    footer = buf[-10:]
    crc_payload = int.from_bytes(footer[2:10], byteorder="little", signed=False)
    if crc_payload == 0:
        return True
    crc = _redis_crc64(buf[:-8])
    return crc == crc_payload

def _patch_dump_crc(buf: bytearray) -> None:
    crc = _redis_crc64(bytes(buf[:-8]))
    buf[-8:] = crc.to_bytes(8, byteorder="little", signed=False)

def _rdb_load_len(buf: bytes, idx: int):
    # Implements Redis rdbLoadLenByRef() logic (only what's needed for module payload parsing).
    b0 = buf[idx]
    idx += 1
    t = (b0 & 0xC0) >> 6
    if t == 3:  # RDB_ENCVAL
        return (b0 & 0x3F), True, idx
    if t == 0:  # RDB_6BITLEN
        return (b0 & 0x3F), False, idx
    if t == 1:  # RDB_14BITLEN
        b1 = buf[idx]
        idx += 1
        return (((b0 & 0x3F) << 8) | b1), False, idx
    # RDB_32BITLEN / RDB_64BITLEN special markers
    if b0 == 0x80:  # RDB_32BITLEN
        v = int.from_bytes(buf[idx:idx+4], "big", signed=False)
        return v, False, idx + 4
    if b0 == 0x81:  # RDB_64BITLEN
        v = int.from_bytes(buf[idx:idx+8], "big", signed=False)
        return v, False, idx + 8
    raise AssertionError("Unknown RDB length encoding")

def _rdb_skip_string(buf: bytes, idx: int):
    """Skip an RDB string object payload starting at idx (right after the STRING opcode)."""
    strlen_or_enc, isenc, idx = _rdb_load_len(buf, idx)
    if not isenc:
        return idx + strlen_or_enc
    enc = strlen_or_enc
    if enc == 0:      # RDB_ENC_INT8
        return idx + 1
    if enc == 1:      # RDB_ENC_INT16
        return idx + 2
    if enc == 2:      # RDB_ENC_INT32
        return idx + 4
    if enc == 3:      # RDB_ENC_LZF
        clen, _, idx = _rdb_load_len(buf, idx)
        _len, _, idx = _rdb_load_len(buf, idx)
        return idx + clen
    raise AssertionError("Unknown RDB string encoding")

def _rdb_read_string_len_and_skip(buf: bytes, idx: int):
    """Read an RDB string object, returning (decoded_length_or_None, new_idx)."""
    strlen_or_enc, isenc, idx = _rdb_load_len(buf, idx)
    if not isenc:
        return strlen_or_enc, idx + strlen_or_enc
    enc = strlen_or_enc
    if enc == 0:      # RDB_ENC_INT8
        return None, idx + 1
    if enc == 1:      # RDB_ENC_INT16
        return None, idx + 2
    if enc == 2:      # RDB_ENC_INT32
        return None, idx + 4
    if enc == 3:      # RDB_ENC_LZF
        clen, _, idx = _rdb_load_len(buf, idx)
        outlen, _, idx = _rdb_load_len(buf, idx)
        return outlen, idx + clen
    raise AssertionError("Unknown RDB string encoding")

def _patch_first_uncompressed_chunk_num_samples(dump: bytes, new_num_samples: int) -> bytes:
    # We keep the same encoding width by ensuring new_num_samples fits in 6-bit len (0..63).
    assert 0 <= new_num_samples <= 63
    b = bytearray(dump)
    assert _verify_dump_payload(dump), "baseline DUMP payload should have valid checksum"

    # Parse: [1 byte object type] [moduleid (rdb len)] [module opcodes... EOF] [footer...]
    idx = 0
    idx += 1  # object type byte
    _, _, idx = _rdb_load_len(dump, idx)  # moduleid

    # Helpers to consume module opcodes.
    def read_opcode():
        nonlocal idx
        op, _, idx2 = _rdb_load_len(dump, idx)
        idx = idx2
        return op

    def read_uint_capture_offset():
        nonlocal idx
        op = read_opcode()
        assert op == 2  # RDB_MODULE_OPCODE_UINT
        # value is also rdb len; for 0..63 it's 1 byte and stored directly in that byte.
        val_start = idx
        val, _, idx2 = _rdb_load_len(dump, idx)
        idx = idx2
        return val, val_start

    def read_string_skip():
        nonlocal idx
        op = read_opcode()
        assert op == 5  # RDB_MODULE_OPCODE_STRING
        idx = _rdb_skip_string(dump, idx)

    def read_double_skip():
        nonlocal idx
        op = read_opcode()
        assert op == 4  # RDB_MODULE_OPCODE_DOUBLE
        idx += 8

    # series_rdb_save() fields (minimal path used by DUMP/RESTORE).
    read_string_skip()                 # keyName
    read_uint_capture_offset()         # retentionTime
    chunk_size_bytes, _ = read_uint_capture_offset()  # chunkSizeBytes
    options, _ = read_uint_capture_offset()           # options
    read_uint_capture_offset()         # lastTimestamp
    read_double_skip()                 # lastValue
    read_uint_capture_offset()         # totalSamples
    read_uint_capture_offset()         # duplicatePolicy
    has_src, _ = read_uint_capture_offset()
    assert has_src == 0
    if has_src:
        read_string_skip()
    read_uint_capture_offset()         # ignoreMaxTimeDiff
    read_double_skip()                 # ignoreMaxValDiff
    labels_count, _ = read_uint_capture_offset()
    assert labels_count == 0
    for _ in range(labels_count):
        read_string_skip()
        read_string_skip()
    rules_count, _ = read_uint_capture_offset()
    assert rules_count == 0
    for _ in range(rules_count):
        # Not expected in this test (DUMP saves 0 rules), but keep parser future-proof.
        read_string_skip()                 # destKey
        read_uint_capture_offset()         # bucketDuration
        read_uint_capture_offset()         # timestampAlignment
        read_uint_capture_offset()         # aggType
        read_uint_capture_offset()         # startCurrentTimeBucket
        # agg context is module-defined; can't parse generically here.
        raise AssertionError("Unexpected rules payload in DUMP for this test")

    num_chunks, _ = read_uint_capture_offset()
    assert num_chunks == 1

    # First uncompressed chunk: base_timestamp, num_samples, size, samples buffer.
    base_ts, _ = read_uint_capture_offset()
    assert base_ts == 1
    old_num_samples, num_samples_off = read_uint_capture_offset()
    assert old_num_samples == 1
    assert old_num_samples <= 63, "test assumes 1-byte rdb len encoding"
    size_bytes, _ = read_uint_capture_offset()
    assert size_bytes == chunk_size_bytes

    # samples string buffer
    op = read_opcode()
    assert op == 5  # RDB_MODULE_OPCODE_STRING
    decoded_len, idx = _rdb_read_string_len_and_skip(dump, idx)
    # samples buffer is binary and large enough that it should never be integer-encoded.
    assert decoded_len == size_bytes

    # Patch the single byte directly.
    b[num_samples_off] = new_num_samples & 0x3F

    _patch_dump_crc(b)
    assert _verify_dump_payload(bytes(b)), "patched DUMP payload should have valid checksum"
    return bytes(b)


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


def test_broken_rdb_invalid_uncompressed_chunk_metadata(env):
    env.skipOnCluster()
    rdb_payload = b'\x07\x81M \xc1\xf96\x0f\x10\x08\x05\x04zxcv\x02\x00\x02P\x00\x02\x01\x02\x01\x04\x00\x00\x00\x00\x00\x00\xf0?\x02\x01\x02\x00\x02\x00\x02\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x02\x00\x02\x01\x02\x00\x02\x80AAAA\x02\x01\x05B\xbbXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\x00\xff\x0c\x00\xf4\x02\x01#\x17\x97f\xae'
    env.expect('RESTORE', 'test_key', 0, rdb_payload, replace=True).error().contains("DUMP payload version or checksum are wrong")