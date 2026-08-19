"""
Test that verifies RDB load failure handling for corrupted/broken RDB files.
This test creates various types of broken RDB files and ensures they fail to load properly.
"""

from includes import Env
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

def _rdb_encode_len(v: int) -> bytes:
    # Implements Redis rdbSaveLen() encoding for non-encoded lengths/integers.
    assert v >= 0
    if v < (1 << 6):
        return bytes([v & 0x3F])
    if v < (1 << 14):
        return bytes([((v >> 8) & 0x3F) | 0x40, v & 0xFF])
    if v <= 0xFFFFFFFF:
        return bytes([0x80]) + int(v).to_bytes(4, "big", signed=False)
    return bytes([0x81]) + int(v).to_bytes(8, "big", signed=False)

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
    env.expect('RESTORE', 'test_key', 0, rdb_payload, replace=True).error().contains("Bad data format")


def test_broken_rdb_rejects_compressed_chunk_size_len_mismatch(env):
    env.skipOnCluster()

    rdb_payload = (b'\x07\x81M \xc1\xf96\x0f\x10\x08\x05\tts_retest\x02\x00\x02@@\x02\x00\x02C\xe8'
                   b'\x04\x00\x00\x00\x00\x00\x00E@\x02\x01\x02\x00\x02\x00\x02\x00'
                   b'\x04\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x02\x00\x02\x01\x02P\x00\x02\x00'
                   b'\x02\x00\x02\x81@E\x00\x00\x00\x00\x00\x00\x02C\xe8\x02C\xe8\x02\x00'
                   b'\x02\x81@E\x00\x00\x00\x00\x00\x00\x02 \x02 '
                   b'\x05\x10\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f'
                   b'\x00\x0c\x004\n\xe3@\x86\xf3\x15\xe1')

    env.expect('RESTORE', 'ts_retest', 0, rdb_payload).error()

    pong = env.cmd('PING')
    assert pong in (b'PONG', 'PONG', True)
    env.assertEqual(env.cmd('EXISTS', 'ts_retest'), 0)


def _patch_first_compressed_chunk_misalign_size(dump: bytes) -> bytes:
    """
    Shrinks the first compressed chunk's data buffer by 1 byte and updates
    chunk->size to match (so the size==len check still passes), producing a
    buffer length that isn't a multiple of 8.
    """
    b = bytearray(dump)
    assert _verify_dump_payload(dump), "baseline DUMP payload should have valid checksum"

    idx = 0
    idx += 1  # object type byte
    _, _, idx = _rdb_load_len(dump, idx)  # moduleid

    def read_opcode():
        nonlocal idx
        op, _, idx2 = _rdb_load_len(dump, idx)
        idx = idx2
        return op

    def read_uint_capture():
        nonlocal idx
        op = read_opcode()
        assert op == 2  # RDB_MODULE_OPCODE_UINT
        val_start = idx
        val, _, idx2 = _rdb_load_len(dump, idx)
        idx = idx2
        return val, val_start, idx

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

    read_string_skip()                 # keyName
    read_uint_capture()                # retentionTime
    read_uint_capture()                # chunkSizeBytes
    options, _, _ = read_uint_capture()
    assert options == 2, "test assumes default COMPRESSED encoding (SERIES_OPT_COMPRESSED_GORILLA)"
    read_uint_capture()                # lastTimestamp
    read_double_skip()                 # lastValue
    read_uint_capture()                # totalSamples
    read_uint_capture()                # duplicatePolicy
    has_src, _, _ = read_uint_capture()
    assert has_src == 0
    read_uint_capture()                # ignoreMaxTimeDiff
    read_double_skip()                 # ignoreMaxValDiff
    labels_count, _, _ = read_uint_capture()
    assert labels_count == 0
    rules_count, _, _ = read_uint_capture()
    assert rules_count == 0
    num_chunks, _, _ = read_uint_capture()
    assert num_chunks == 1

    size_val, size_start, size_end = read_uint_capture()  # chunk->size
    read_uint_capture()  # count
    read_uint_capture()  # idx
    read_uint_capture()  # baseValue
    read_uint_capture()  # baseTimestamp
    read_uint_capture()  # prevTimestamp
    read_uint_capture()  # prevTimestampDelta
    read_uint_capture()  # prevValue
    read_uint_capture()  # prevLeading
    read_uint_capture()  # prevTrailing

    string_field_start = idx
    op = read_opcode()
    assert op == 5  # RDB_MODULE_OPCODE_STRING
    assert size_val % 8 == 0, "test assumes an 8-aligned baseline chunk size"
    data_field_end = _rdb_skip_string(dump, idx)

    new_len = size_val - 1
    # Rebuild the whole data-string field as a plain (non-LZF, non-int-encoded)
    # buffer of the new length -- the real DUMP may have LZF-compressed this
    # field (it's mostly zero bytes), so we can't just truncate the raw bytes.
    # Content doesn't matter here, only the length/alignment.
    new_field = bytes([5]) + _rdb_encode_len(new_len) + bytes(new_len)

    # Replace data-string field first (rightmost span), then chunk->size
    # (leftmost span) -- editing right-to-left keeps the earlier offset valid.
    b[string_field_start:data_field_end] = new_field
    b[size_start:size_end] = _rdb_encode_len(new_len)

    _patch_dump_crc(b)
    assert _verify_dump_payload(bytes(b)), "patched DUMP payload should have valid checksum"
    return bytes(b)


def _patch_first_compressed_chunk_count(dump: bytes, new_count: int) -> bytes:
    """
    Inflates chunk->count on the first compressed chunk without touching
    idx/size/data. Regression test for the missing count-vs-idx consistency
    check in Compressed_LoadFromRDB.
    """
    b = bytearray(dump)
    assert _verify_dump_payload(dump), "baseline DUMP payload should have valid checksum"

    idx = 0
    idx += 1  # object type byte
    _, _, idx = _rdb_load_len(dump, idx)  # moduleid

    def read_opcode():
        nonlocal idx
        op, _, idx2 = _rdb_load_len(dump, idx)
        idx = idx2
        return op

    def read_uint_capture():
        nonlocal idx
        op = read_opcode()
        assert op == 2  # RDB_MODULE_OPCODE_UINT
        val_start = idx
        val, _, idx2 = _rdb_load_len(dump, idx)
        idx = idx2
        return val, val_start, idx

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

    read_string_skip()                 # keyName
    read_uint_capture()                # retentionTime
    read_uint_capture()                # chunkSizeBytes
    options, _, _ = read_uint_capture()
    assert options == 2, "test assumes default COMPRESSED encoding (SERIES_OPT_COMPRESSED_GORILLA)"
    read_uint_capture()                # lastTimestamp
    read_double_skip()                 # lastValue
    read_uint_capture()                # totalSamples
    read_uint_capture()                # duplicatePolicy
    has_src, _, _ = read_uint_capture()
    assert has_src == 0
    read_uint_capture()                # ignoreMaxTimeDiff
    read_double_skip()                 # ignoreMaxValDiff
    labels_count, _, _ = read_uint_capture()
    assert labels_count == 0
    rules_count, _, _ = read_uint_capture()
    assert rules_count == 0
    num_chunks, _, _ = read_uint_capture()
    assert num_chunks == 1

    read_uint_capture()                                      # chunk->size
    count_val, count_start, count_end = read_uint_capture()  # chunk->count

    b[count_start:count_end] = _rdb_encode_len(new_count)

    _patch_dump_crc(b)
    assert _verify_dump_payload(bytes(b)), "patched DUMP payload should have valid checksum"
    return bytes(b)


def test_broken_rdb_rejects_compressed_chunk_size_not_word_aligned(env):
    env.skipOnCluster()

    env.cmd('TS.CREATE', 'test_key', 'CHUNK_SIZE', '48')
    env.cmd('TS.ADD', 'test_key', 1000, 1.0)

    valid_dump = env.cmd('DUMP', 'test_key')
    malicious_dump = _patch_first_compressed_chunk_misalign_size(valid_dump)

    env.cmd('DEL', 'test_key')

    env.expect('RESTORE', 'test_key', 0, malicious_dump).error()


def test_broken_rdb_rejects_compressed_chunk_count_exceeds_encoded_bits(env):
    env.skipOnCluster()

    env.cmd('TS.CREATE', 'test_key', 'CHUNK_SIZE', '128')
    env.cmd('TS.ADD', 'test_key', 1000, 1.0)

    valid_dump = env.cmd('DUMP', 'test_key')
    malicious_dump = _patch_first_compressed_chunk_count(valid_dump, 1000000)

    env.cmd('DEL', 'test_key')

    env.expect('RESTORE', 'test_key', 0, malicious_dump).error()


def test_broken_rdb_rejects_compressed_chunk_count_overflow_bypass(env):
    env.skipOnCluster()

    env.cmd('TS.CREATE', 'test_key', 'CHUNK_SIZE', '128')
    env.cmd('TS.ADD', 'test_key', 1000, 1.0)

    valid_dump = env.cmd('DUMP', 'test_key')
    malicious_dump = _patch_first_compressed_chunk_count(valid_dump, (1 << 63) + 1)

    env.cmd('DEL', 'test_key')

    env.expect('RESTORE', 'test_key', 0, malicious_dump).error()

def _gorilla_wide_sample_bitstream(nbits: int) -> bytes:
    """
    Build a gorilla data buffer of `nbits` bits (rounded up to whole bytes) whose
    every decoded sample consumes 71 bits, so decoding drives `iter->idx` forward
    fast. Crucially it never takes the readFloat path, so it does NOT trip the
    DEBUG-only asserts in gorilla.c (which would abort the server on any build
    regardless of this fix) -- the test must stay portable across debug/release/asan.

    Per-sample bit layout consumed by Compressed_ChunkIteratorGetNext (LSB-first):
      1 : timestamp control bit = 1  -> not "off", so readInteger() is called
      5 : readInteger control bits = 1 x5 -> falls through to the 64-bit branch
      64: the delta value (zeros)
      1 : value control bit = 0 -> "off", uses prevValue, readFloat() NOT called
    => 71 bits/sample, all control paths assert-free.
    """
    unit = [1, 1, 1, 1, 1, 1] + [0] * 65  # 6 ones + 64 delta zeros + 1 value-control zero
    total_bits = ((nbits + 7) // 8) * 8
    bits = (unit * (total_bits // len(unit) + 1))[:total_bits]
    out = bytearray(total_bits // 8)
    for i, bit in enumerate(bits):
        if bit:
            out[i // 8] |= (1 << (i % 8))  # bit position p -> byte p//8, LSB-first
    return bytes(out)


def _patch_first_compressed_chunk_overread(dump: bytes):
    """
    Craft a poisoned compressed chunk that PASSES the load-time count/idx/size
    consistency checks but whose `count`-driven decoder would still walk off the
    data buffer.

    We set idx = size*8 (the max the load check allows) and count = size*4 (so
    count-1 <= idx/2 holds), and fill the data buffer with a pattern in which each
    decoded sample consumes 71 bits. Decoding `count` such samples requires far
    more than the size*8 bits the buffer holds, so without the decoder-time
    `idx >= size*8` bound the reader reads adjacent heap.

    Returns (poisoned_dump, inflated_count).
    """
    b = bytearray(dump)
    assert _verify_dump_payload(dump), "baseline DUMP payload should have valid checksum"

    idx = 0
    idx += 1  # object type byte
    _, _, idx = _rdb_load_len(dump, idx)  # moduleid

    def read_opcode():
        nonlocal idx
        op, _, idx2 = _rdb_load_len(dump, idx)
        idx = idx2
        return op

    def read_uint_capture():
        nonlocal idx
        op = read_opcode()
        assert op == 2  # RDB_MODULE_OPCODE_UINT
        val_start = idx
        val, _, idx2 = _rdb_load_len(dump, idx)
        idx = idx2
        return val, val_start, idx

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

    read_string_skip()                 # keyName
    read_uint_capture()                # retentionTime
    read_uint_capture()                # chunkSizeBytes
    options, _, _ = read_uint_capture()
    assert options == 2, "test assumes default COMPRESSED encoding (SERIES_OPT_COMPRESSED_GORILLA)"
    read_uint_capture()                # lastTimestamp
    read_double_skip()                 # lastValue
    read_uint_capture()                # totalSamples
    read_uint_capture()                # duplicatePolicy
    has_src, _, _ = read_uint_capture()
    assert has_src == 0
    read_uint_capture()                # ignoreMaxTimeDiff
    read_double_skip()                 # ignoreMaxValDiff
    labels_count, _, _ = read_uint_capture()
    assert labels_count == 0
    rules_count, _, _ = read_uint_capture()
    assert rules_count == 0
    num_chunks, _, _ = read_uint_capture()
    assert num_chunks == 1

    size_val, size_start, size_end = read_uint_capture()   # chunk->size
    _, count_start, count_end = read_uint_capture()        # chunk->count
    _, idx_start, idx_end = read_uint_capture()            # chunk->idx

    assert size_val % 8 == 0 and size_val > 0

    # Skip the remaining scalar fields to reach the data-string field.
    read_uint_capture()  # baseValue
    read_uint_capture()  # baseTimestamp
    read_uint_capture()  # prevTimestamp
    read_uint_capture()  # prevTimestampDelta
    read_uint_capture()  # prevValue
    read_uint_capture()  # prevLeading
    read_uint_capture()  # prevTrailing

    string_field_start = idx
    op = read_opcode()
    assert op == 5  # RDB_MODULE_OPCODE_STRING
    data_field_end = _rdb_skip_string(dump, idx)

    inflated_count = size_val * 4          # count-1 <= idx/2 == size*4, so this passes the load check
    poisoned_idx = size_val * 8            # max idx the load check permits

    # Rebuild the data-string field as a plain (non-encoded) buffer of the declared
    # size whose every decoded sample consumes 71 bits (assert-free, see helper).
    data = _gorilla_wide_sample_bitstream(size_val * 8)
    new_field = bytes([5]) + _rdb_encode_len(size_val) + data

    # Edit right-to-left so earlier offsets stay valid.
    b[string_field_start:data_field_end] = new_field
    b[idx_start:idx_end] = _rdb_encode_len(poisoned_idx)
    b[count_start:count_end] = _rdb_encode_len(inflated_count)

    _patch_dump_crc(b)
    assert _verify_dump_payload(bytes(b)), "patched DUMP payload should have valid checksum"
    return bytes(b), inflated_count


def test_broken_rdb_compressed_chunk_read_side_over_read_is_bounded(env):
    """
    Regression for the read-side heap over-read (MOD-17238 / VDP-4940): a chunk
    whose `count` claims more samples than its data encodes must not let the
    gorilla decoder read past the buffer.

    The poisoned chunk is internally consistent (passes the load-time checks) but
    each of its samples decodes to 71 bits, so an unbounded `count`-driven decode
    walks far past the `size*8`-bit buffer -- crashing under ASan / on a real
    over-read. With the fix the decoder stops at `size*8` bits (the data buffer is
    over-allocated so the last look-ahead stays in-bounds); the loop, still bounded
    by `count`, then re-emits the last in-bounds sample instead of leaking heap.

    So the server must survive and every returned sample must be the safe decoded
    value (baseTimestamp=1000, delta 0) -- never adjacent heap content.
    """
    env.skipOnCluster()

    env.cmd('TS.CREATE', 'test_key', 'CHUNK_SIZE', '64')
    env.cmd('TS.ADD', 'test_key', 1000, 1.0)

    valid_dump = env.cmd('DUMP', 'test_key')
    poisoned_dump, _inflated_count = _patch_first_compressed_chunk_overread(valid_dump)

    env.cmd('DEL', 'test_key')

    # The payload is internally consistent, so RESTORE may accept it; if it does,
    # decoding it must be bounded (no out-of-bounds read, no crash).
    try:
        env.cmd('RESTORE', 'test_key', 0, poisoned_dump)
    except Exception:
        # Rejected at load time is also a safe outcome.
        assert env.cmd('PING')
        return

    res = env.cmd('TS.RANGE', 'test_key', '-', '+')

    # Server must survive decoding the poisoned chunk (an over-read would crash it).
    assert env.cmd('PING')
    # No heap must leak: every sample stays at the in-bounds decoded timestamp.
    for ts, _val in res:
        env.assertEqual(int(ts), 1000)
