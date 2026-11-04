"""Malformed fixtures exercise diagnostics; they are not incident reproducers."""
from includes import *
from test_borken_rdb import (
    _patch_uncompressed_sample_counts,
    _patch_first_compressed_chunk_misalign_size,
    _patch_first_compressed_chunk_count,
    _patch_dump_crc,
    _rdb_load_len,
    _rdb_skip_string,
)


def test_rdb_load_diagnostics(env):
    env.skipOnCluster()
    env.cmd('CONFIG', 'SET', 'loglevel', 'warning')
    cases = [
        ('sample-total', ['UNCOMPRESSED'],
         lambda dump: _patch_uncompressed_sample_counts(dump, 1, 0),
         'totalSamples does not match loaded chunks: persisted=0 loaded=1 numChunks=1'),
        ('sample-capacity', ['UNCOMPRESSED', 'CHUNK_SIZE', 48],
         lambda dump: _patch_uncompressed_sample_counts(dump, 63),
         'uncompressed chunk: sample count exceeds capacity count=63 size=48'),
        ('word-alignment', [], _patch_first_compressed_chunk_misalign_size,
         'compressed chunk: buffer size is not aligned to 8-byte words'),
        ('bit-capacity', [], lambda dump: _patch_first_compressed_chunk_count(dump, 63),
         'compressed chunk: sample count exceeds encoded bit capacity'),
    ]
    for name, options, patch, reason in cases:
        source = 'source-' + name
        # RESTORE can rename the key. Log the destination from the IO context,
        # not the serialized source name. Include a NUL/newline to check escaping.
        target = ('target-' + name).encode() + b'\x00\ninjected-log-line'
        env.cmd('TS.CREATE', source, *options)
        env.cmd('TS.ADD', source, 1, 42)
        dump = patch(env.cmd('DUMP', source))
        env.expect('RESTORE', target, 0, dump).error()
        env.assertEqual(env.cmd('EXISTS', target), 0)
        assert env.cmd('PING')
        key_context = f'key_hex={target.hex()} key_len={len(target)} key_truncated=0'
        assert is_line_in_server_log(env, key_context + ' ' + reason)
        assert is_line_in_server_log(env, key_context + ' series rejected:')
        if name != 'sample-total':
            assert is_line_in_server_log(env, key_context + ' chunk rejected: chunk_index=0 numChunks=1')


def test_rdb_load_diagnostics_bound_key_length(env):
    env.skipOnCluster()
    source = 'source-long-key'
    env.cmd('TS.CREATE', source, 'UNCOMPRESSED')
    env.cmd('TS.ADD', source, 1, 42)
    dump = _patch_uncompressed_sample_counts(env.cmd('DUMP', source), 1, 0)
    target = b'x' * 512
    env.expect('RESTORE', target, 0, dump).error()
    assert is_line_in_server_log(env, f'key_hex={(b"x" * 128).hex()} key_len=512 key_truncated=1')


def test_rdb_load_diagnostics_read_error(env):
    env.skipOnCluster()
    env.cmd('CONFIG', 'SET', 'loglevel', 'warning')
    source = 'source-read-error'
    env.cmd('TS.CREATE', source, 'UNCOMPRESSED')
    dump = env.cmd('DUMP', source)
    _, _, offset = _rdb_load_len(dump, 1)  # module ID
    opcode, _, offset = _rdb_load_len(dump, offset)
    assert opcode == 5  # serialized key name
    offset = _rdb_skip_string(dump, offset)
    # End the module stream before the next unsigned field; retain a valid CRC
    # so RESTORE reaches the module reader rather than checksum validation.
    broken = bytearray(dump[:offset] + b'\x00' + dump[-10:])
    _patch_dump_crc(broken)
    target = b'target-read-error'
    env.expect('RESTORE', target, 0, bytes(broken)).error()
    prefix = f'key_hex={target.hex()} key_len={len(target)} key_truncated=0'
    assert is_line_in_server_log(env, prefix + ' I/O error: operation=LoadUnsigned source=')
    assert is_line_in_server_log(env, prefix + ' series rejected:')
    assert env.cmd('PING')
