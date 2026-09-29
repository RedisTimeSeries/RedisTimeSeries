# MOD-18948: per-write-command primary -> replica data-integrity verification.
#
# For every write command the module registers, run one representative
# invocation on the primary and assert the replica ends up holding the same
# series. Catches commands that are implemented correctly on the primary while
# their propagation to the replica is wrong or missing -- a divergence that
# stays silent (link up, offsets matching) until a failover promotes the
# replica. MOD-16050 is the canonical case, in RedisBloom; MOD-16873 is the
# TimeSeries one: TS.INCRBY without TIMESTAMP was replicated verbatim, so the
# replica stamped the sample with its own clock.

from includes import *

MODULE_NAME = 'timeseries'
WAIT_TIMEOUT_MS = 1000

READ_K = [['TS.RANGE', 'k', '-', '+'], ['TS.INFO', 'k']]

# src compacts into dst in 10ms buckets. Samples at 1 and 5 fill bucket [0,10);
# the next sample in a later bucket closes it and writes avg=2 into dst.
RULE = [['TS.CREATE', 'src'], ['TS.CREATE', 'dst'],
        ['TS.CREATERULE', 'src', 'dst', 'AGGREGATION', 'avg', '10']]

# (command, setup, write-under-test, reads compared on both sides)
#
# One invocation per command, no argument permutations -- but the invocation is
# the form most likely to diverge: '*' and TIMESTAMP-less forms (resolved from
# the server clock) over explicit timestamps, a sample that triggers compaction
# over a plain append, label changes that must re-index over retention tweaks.
ROWS = [
    # Labels feed a secondary index that TS.QUERYINDEX reads. No samples yet,
    # so TS.RANGE would be an empty -- trivially matching -- read.
    ('ts.create', None, ['TS.CREATE', 'k', 'LABELS', 'sensor', '1'],
     [['TS.INFO', 'k'], ['TS.QUERYINDEX', 'sensor=1']]),
    ('ts.alter', [['TS.CREATE', 'k', 'LABELS', 'sensor', '1']],
     ['TS.ALTER', 'k', 'LABELS', 'sensor', '2', 'DUPLICATE_POLICY', 'LAST'],
     [['TS.INFO', 'k'], ['TS.QUERYINDEX', 'sensor=2']]),
    ('ts.createrule', [['TS.CREATE', 'src'], ['TS.CREATE', 'dst']],
     ['TS.CREATERULE', 'src', 'dst', 'AGGREGATION', 'avg', '10'],
     [['TS.INFO', 'src'], ['TS.INFO', 'dst']]),
    ('ts.deleterule', RULE, ['TS.DELETERULE', 'src', 'dst'],
     [['TS.INFO', 'src'], ['TS.INFO', 'dst']]),
    # '*' is resolved on the primary, and the sample closes a compaction bucket.
    ('ts.add', RULE + [['TS.ADD', 'src', '1', '1'], ['TS.ADD', 'src', '5', '3']],
     ['TS.ADD', 'src', '*', '7'],
     [['TS.RANGE', 'src', '-', '+'], ['TS.RANGE', 'dst', '-', '+'], ['TS.INFO', 'dst']]),
    # No TIMESTAMP: the sample is stamped from the primary's clock (MOD-16873).
    ('ts.incrby', [['TS.ADD', 'k', '1000', '10']], ['TS.INCRBY', 'k', '5'], READ_K),
    ('ts.decrby', [['TS.ADD', 'k', '1000', '10']], ['TS.DECRBY', 'k', '5'], READ_K),
    ('ts.del', [['TS.CREATE', 'k']] + [['TS.ADD', 'k', str(t), str(t)] for t in range(1, 6)],
     ['TS.DEL', 'k', '2', '3'], READ_K),
    ('ts.madd', [['TS.CREATE', 'k1'], ['TS.CREATE', 'k2']],
     ['TS.MADD', 'k1', '*', '1', 'k2', '*', '2'],
     [['TS.RANGE', 'k1', '-', '+'], ['TS.RANGE', 'k2', '-', '+']]),
]

# Write commands deliberately not in ROWS, with the reason. Keep this empty.
KNOWN_EXCLUSIONS = set()


def _wait_link_up(env, timeout=10):
    """RLTest starts the replica with --slaveof but never waits for the sync."""
    slave = env.getSlaveConnection()
    deadline = time.time() + timeout
    while time.time() < deadline:
        if slave.execute_command('INFO', 'replication')['master_link_status'] == 'up':
            return
        time.sleep(0.1)
    env.assertTrue(False, message='replica link never came up')


def _is_trivial(val):
    """A read that returns the type default proves nothing -- both sides match."""
    if val is None or val == 0 or val == b'' or val == '':
        return True
    if isinstance(val, (list, tuple)):
        return len(val) == 0 or all(_is_trivial(v) for v in val)
    return False


def _read_slave(con, spec):
    """A divergence must be reported as a diff, not raised -- keep rows independent."""
    try:
        return con.execute_command(*spec)
    except redis.ResponseError as e:
        return f'error: {e}'


def _keys(con):
    return sorted(con.execute_command('KEYS', '*'))


def _dumps(con, keys):
    return [con.execute_command('DUMP', k) for k in keys]


def _verify_row(env, master, slave, cmd, setup, write, reads):
    master.execute_command('FLUSHALL')
    for spec in (setup or []):
        master.execute_command(*spec)
    master.execute_command(*write)

    acked = master.execute_command('WAIT', 1, WAIT_TIMEOUT_MS)
    env.assertEqual(acked, 1, message=f'{cmd}: replica did not ack the write')
    keys = _keys(master)
    env.assertEqual(keys, _keys(slave), message=f'{cmd}: key set diverged')
    env.assertEqual(_dumps(master, keys), _dumps(slave, keys), message=f'{cmd}: DUMP diverged')

    for spec in reads:
        got = master.execute_command(*spec)
        env.assertFalse(_is_trivial(got), message=f'{cmd}: master {spec[0]} returned a default value')
        env.assertEqual(got, _read_slave(slave, spec), message=f'{cmd}: {spec[0]} diverged')


def test_write_commands_replicate():
    skip_on_rlec()
    env = Env(useSlaves=True, protocol=2)
    env.skipOnCluster()  # a cluster env has no replica connection to read
    master, slave = env.getConnection(), env.getSlaveConnection()
    _wait_link_up(env)
    for cmd, setup, write, reads in ROWS:
        _verify_row(env, master, slave, cmd, setup, write, reads)


def test_every_write_command_is_covered():
    """The command table is only as good as its coverage of the real command set."""
    skip_on_rlec()
    env = Env(useSlaves=True, protocol=2)
    con = env.getConnection()
    if is_redis_version_lower_than(con, '7.0'):
        env.skip()
    # redis-py pipes every COMMAND * reply through its COMMAND INFO parser,
    # which cannot read a COMMAND LIST reply. Take the raw replies instead.
    con.set_response_callback('COMMAND', lambda r, **_: r)
    names = con.execute_command('COMMAND', 'LIST', 'FILTERBY', 'MODULE', MODULE_NAME)
    info = con.execute_command('COMMAND', 'INFO', *names)
    write_cmds = {c[0].decode().lower() for c in info if c and b'write' in c[2]}
    missing = write_cmds - {cmd for cmd, _, _, _ in ROWS} - KNOWN_EXCLUSIONS
    env.assertEqual(missing, set(), message=f'write commands with no replication test: {missing}')
