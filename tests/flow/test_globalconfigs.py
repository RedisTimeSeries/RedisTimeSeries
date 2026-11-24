# import pytest
# from utils import Env
from test_helper_classes import TSInfo, _get_ts_info
from includes import *

class testModuleLoadTimeArguments(object):
    def __init__(self):
        self.test_variations = [(True, 'CHUNK_SIZE_BYTES 2048'),
                                (True, 'COMPACTION_POLICY', 'max:1m:1d\\;min:10s:1h\\;avg:2h:10d\\;avg:3d:100d'),
                                (True, 'DUPLICATE_POLICY MAX'),
                                (True, 'RETENTION_POLICY 30'),
                                (True, 'IGNORE_MAX_TIME_DIFF 10'),
                                (True, 'IGNORE_MAX_VAL_DIFF 20')
                                ]

    def test(self):
        Env().skipOnCluster()
        skip_on_rlec()
        for variation in self.test_variations:
            should_ok = variation[0]
            if should_ok:
                env = Env(testName="Test load time args: {}".format(variation[1]),
                          moduleArgs=variation[1])
                r = env.getConnection()
                assert r.execute_command('PING') == True
            else:
                with pytest.raises(Exception) as excinfo:
                    assert Env(testName="Test load time args: {}".format(variation[1]), moduleArgs=variation[1])


def test_ignore():
    Env().skipOnCluster()
    skip_on_rlec()
    env = Env(moduleArgs='IGNORE_MAX_TIME_DIFF 10; IGNORE_MAX_VAL_DIFF 20')
    with env.getConnection() as r:
        # Verify module args work as the default config
        r.execute_command('TS.ADD', 'key1', '1000', '100', 'DUPLICATE_POLICY', 'LAST')
        r.execute_command('TS.ADD', 'key1', '1005', '110')
        assert r.execute_command('TS.RANGE', 'key1', '0', '+') == [[1000, b'100']]

        # Verify create arguments override module args config
        r.execute_command('TS.ADD', 'key2', '1000', '100', 'DUPLICATE_POLICY', 'LAST', 'IGNORE', '5', '3')
        r.execute_command('TS.ADD', 'key2', '1005', '110')
        r.execute_command('TS.ADD', 'key2', '1007', '112')
        assert r.execute_command('TS.RANGE', 'key2', '0', '+') == [[1000, b'100'], [1005, b'110']]

@skip(asan=True)
def test_ignore_invalid_module_args(env):
    env.skipOnCluster()
    skip_on_rlec()
    with pytest.raises(Exception):
        Env(moduleArgs='IGNORE_MAX_TIME_DIFF -10; IGNORE_MAX_VAL_DIFF 20')
    with pytest.raises(Exception):
        Env(moduleArgs='IGNORE_MAX_TIME_DIFF invalid; IGNORE_MAX_VAL_DIFF 20')
    with pytest.raises(Exception):
        Env(moduleArgs='IGNORE_MAX_TIME_DIFF 10; IGNORE_MAX_VAL_DIFF -20')
    with pytest.raises(Exception):
        Env(moduleArgs='IGNORE_MAX_TIME_DIFF 10; IGNORE_MAX_VAL_DIFF invalid')


def test_encoding_uncompressed():
    Env().skipOnCluster()
    skip_on_rlec()
    env = Env(moduleArgs='ENCODING UNCOMPRESSED; COMPACTION_POLICY max:1s:1m')
    with env.getConnection() as r:
        r.execute_command('FLUSHALL')
        r.execute_command('TS.ADD', 't1', '1', 1.0)
        assert TSInfo(r.execute_command('TS.INFO', 't1_MAX_1000')).chunk_type == b'uncompressed'


def test_encoding_compressed():
    Env().skipOnCluster()
    skip_on_rlec()
    env = Env(moduleArgs='ENCODING compressed; COMPACTION_POLICY max:1s:1m')
    with env.getConnection() as r:
        r.execute_command('FLUSHALL')
        r.execute_command('TS.ADD', 't1', '1', 1.0)
        assert TSInfo(r.execute_command('TS.INFO', 't1_MAX_1000')).chunk_type == b'compressed'

def test_uncompressed():
    Env().skipOnCluster()
    skip_on_rlec()
    env = Env(moduleArgs='CHUNK_TYPE UNCOMPRESSED; COMPACTION_POLICY max:1s:1m')
    with env.getConnection() as r:
        r.execute_command('FLUSHALL')
        r.execute_command('TS.ADD', 't1', '1', 1.0)
        assert TSInfo(r.execute_command('TS.INFO', 't1_MAX_1000')).chunk_type == b'uncompressed'


def test_compressed():
    Env().skipOnCluster()
    skip_on_rlec()
    env = Env(moduleArgs='CHUNK_TYPE compressed; COMPACTION_POLICY max:1s:1m')
    with env.getConnection() as r:
        r.execute_command('FLUSHALL')
        r.execute_command('TS.ADD', 't1', '1', 1.0)
        assert TSInfo(r.execute_command('TS.INFO', 't1_MAX_1000')).chunk_type == b'compressed'

def test_compressed_debug():
    Env().skipOnCluster()

    env = Env(moduleArgs='CHUNK_TYPE compressed COMPACTION_POLICY max:1s:1m')
    skip_on_rlec()
    with env.getConnection() as r:
        r.execute_command('FLUSHALL')
        r.execute_command('TS.ADD', 't1', '1', 1.0)
        r.execute_command('TS.ADD', 't1', '3000', 1.0)
        r.execute_command('TS.ADD', 't1', '5000', 1.0)

        assert TSInfo(r.execute_command('TS.INFO', 't1_MAX_1000', 'DEBUG')).chunks == [[b'startTimestamp', 0, b'endTimestamp', 3000, b'samples', 2, b'size', 4096, b'bytesPerSample', b'2048']]

def test_timestamp_alignment():
    Env().skipOnCluster()
    skip_on_rlec()
    env = Env(moduleArgs='CHUNK_TYPE UNCOMPRESSED; COMPACTION_POLICY max:1s:0:500m')
    with env.getConnection() as r:
        r.execute_command('FLUSHALL')
        r.execute_command('TS.ADD', 't1', '1', 1.0)
        r.execute_command('TS.ADD', 't1', '3000', 1.0)
        r.execute_command('TS.ADD', 't1', '5000', 1.0)
        res = r.execute_command('KEYS *')
        res.sort()
        assert res == [b't1', b't1_MAX_1000_500']

        info = _get_ts_info(r, 't1_MAX_1000_500')
        assert info == TSInfo([b'totalSamples', 2, b'firstTimestamp', 0, b'lastTimestamp', 2500, b'retentionTime', 0, b'chunkCount', 1, b'chunkSize', 4096, b'chunkType', b'uncompressed', b'duplicatePolicy', None, b'labels', [[b'aggregation', b'MAX'], [b'time_bucket', b'1000']], b'sourceKey', b't1', b'rules', [],  b'ignoreMaxTimeDiff', 0, b'ignoreMaxValDiff', b'0'])

        info = _get_ts_info(r, 't1', 'DEBUG')
        assert info == TSInfo([b'totalSamples', 3, b'firstTimestamp', 1, b'lastTimestamp', 5000, b'retentionTime', 0, b'chunkCount', 1, b'chunkSize', 4096, b'chunkType', b'compressed', b'duplicatePolicy', None, b'labels', [], b'sourceKey', None, b'rules', [[b't1_MAX_1000_500', 1000, b'MAX', 500]], b'ignoreMaxTimeDiff', 0, b'ignoreMaxValDiff', b'0', b'keySelfName', b't1', b'Chunks', [[b'startTimestamp', 1, b'endTimestamp', 5000, b'samples', 3, b'size', 4096, b'bytesPerSample', b'1365.3333740234375']]])

        info = _get_ts_info(r, 't1')
        assert info == TSInfo([b'totalSamples', 3, b'firstTimestamp', 1, b'lastTimestamp', 5000, b'retentionTime', 0, b'chunkCount', 1, b'chunkSize', 4096, b'chunkType', b'compressed', b'duplicatePolicy', None, b'labels', [], b'sourceKey', None, b'rules', [[b't1_MAX_1000_500', 1000, b'MAX', 500]], b'ignoreMaxTimeDiff', 0, b'ignoreMaxValDiff', b'0'])

class testGlobalConfigTests():

    def __init__(self):
        Env().skipOnCluster()
        skip_on_rlec()
        self.env = Env(moduleArgs='COMPACTION_POLICY max:1m:1d\\;min:10s:1h\\;avg:2h:10d\\;avg:3d:100d')

    def test_autocreate(self):
        with self.env.getConnection() as r:
            assert r.execute_command('TS.ADD tester 1980 0 LABELS name',
                                     'brown color pink') == 1980
            keys = r.execute_command('keys *')
            keys = sorted(keys)
            assert keys == [b'tester', b'tester_AVG_259200000', b'tester_AVG_7200000', b'tester_MAX_1',
                            b'tester_MIN_10000']
            r.execute_command('TS.ADD tester 1981 1')

            r.execute_command('set exist_MAX_1 foo')
            r.execute_command('TS.ADD exist 1980 0')
            keys = r.execute_command('keys *')
            keys = sorted(keys)
            assert keys == [b'exist', b'exist_AVG_259200000', b'exist_AVG_7200000', b'exist_MAX_1', b'exist_MIN_10000',
                            b'tester', b'tester_AVG_259200000', b'tester_AVG_7200000', b'tester_MAX_1',
                            b'tester_MIN_10000']
            r.execute_command('TS.ADD exist 1981 0')

    def test_big_compressed_chunk_reverserange(self):
        with self.env.getConnection() as r:
            r.execute_command('del tester')
            start_ts = 1599941160000
            last_ts = 0
            samples = []
            for i in range(4099):
                last_ts = start_ts + i * 60000
                samples.append([last_ts, '1'.encode('ascii')])
                r.execute_command('TS.ADD', 'tester', last_ts, 1)
            rev_samples = list(samples)
            rev_samples.reverse()
            assert r.execute_command('TS.GET', 'tester') == [last_ts, b'1']
            assert r.execute_command('TS.RANGE', 'tester', '-', '+') == samples
            assert r.execute_command('TS.REVRANGE', 'tester', '-', '+') == rev_samples

    def test_561_compressed(self):
        self.verify_561('')

    def test_561_uncompressed(self):
        self.verify_561('UNCOMPRESSED')

    def verify_561(self, chunk_type):
        with self.env.getConnection() as r:
            r.execute_command('TS.CREATE', 'tester', chunk_type, 'DUPLICATE_POLICY', 'Last', 'RETENTION', '86400000')
            r.execute_command('TS.CREATE', 'tester_agg', chunk_type, 'DUPLICATE_POLICY', 'Last')
            r.execute_command('TS.CREATERULE', 'tester', 'tester_agg', 'AGGREGATION', 'sum', '10000')

            r.execute_command('TS.ADD', 'tester', 1602166828000, 1)
            r.execute_command('TS.ADD', 'tester', 1602151165000, 1)

            assert r.execute_command('TS.RANGE', 'tester', '-', '+', 'AGGREGATION', 'sum', '10000')[:1] == \
                   r.execute_command('TS.RANGE', 'tester_agg', '-', '+')

            r.execute_command('DEL', 'tester')
            r.execute_command('DEL', 'tester_agg')


def test_negative_configuration():
    Env().skipOnCluster()
    skip_on_rlec()
    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_SIZE_BYTES 80; DUPLICATE_POLICY abc')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_SIZE_BYTES 80; DUPLICATE_POLICY')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_SIZE_BYTES -1')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_SIZE_BYTES 100;')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_SIZE_BYTES 80; CHUNK_TYPE')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_SIZE_BYTES 80; ENCODING')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='ENCODING; CHUNK_SIZE_BYTES 80')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='ENCODING abc; CHUNK_SIZE_BYTES 80')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_TYPE; CHUNK_SIZE_BYTES 88')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_TYPE compressed; COMPACTION_POLICY')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_TYPE compressed; COMPACTION_POLICY NOT_A_REAL_POLICY')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_TYPE compressed; RETENTION_POLICY')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_TYPE compressed; CHUNK_SIZE_BYTES')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_TYPE compressed; OSS_GLOBAL_PASSWORD')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_TYPE compressed; global-password')

@skip(onVersionLowerThan='6.2', onVersionHigherThan='7.0', on_cluster=True)
def test_module_config_api_is_unused_on_old_versions(env):
    env = Env(noLog=False)
    '''
    Tests that the module configuration API is not attempted to be used
    on Redis versions older than 7.0 and no deprecation warnings are
    emitted.
    '''
    skip_on_rlec()

    with env.getConnection() as conn:
        # It should return an empty array as the option is not supported.
        # The command should not raise an exception and no deprecation
        # warnings should be emitted.
        env.expectEqual(len(conn.execute_command('CONFIG', 'GET', 'ts-global-user')), 0)

    assert is_line_in_server_log(env, f"{arg[0]} is deprecated")
    assert is_line_in_server_log(env, 'Deprecated load-time configuration options were used')

def test_module_config_api_is_used_on_recent_redis_versions():
    '''
    Tests that the module configuration API is used on Redis versions
    starting 7.0 and the deprecation warnings are emitted.
    '''
    env = Env(noLog=False)
    if is_redis_version_lower_than(env, '7.0') or env.isCluster():
        env.skip()
    skip_on_rlec()

    # All these options are expected to return a valid value, so just
    # check that no exception is raised, no crashes observed and so on.
    with env.getConnection() as conn:
        # String or floating-point value options:
        conn.execute_command('CONFIG', 'GET', 'ts-compaction-policy')
        conn.execute_command('CONFIG', 'SET', 'ts-compaction-policy', 'max:1m:1d;min:10s:1h;avg:2h:10d;avg:3d:100d')

        conn.execute_command('CONFIG', 'GET', 'ts-global-user')

        conn.execute_command('CONFIG', 'GET', 'ts-duplicate-policy')
        conn.execute_command('CONFIG', 'SET', 'ts-duplicate-policy', 'last')
        conn.execute_command('CONFIG', 'SET', 'ts-duplicate-policy', 'LAST')

        conn.execute_command('CONFIG', 'GET', 'ts-encoding')
        conn.execute_command('CONFIG', 'SET', 'ts-encoding', 'compressed')

        conn.execute_command('CONFIG', 'GET', 'ts-ignore-max-val-diff')
        conn.execute_command('CONFIG', 'SET', 'ts-ignore-max-val-diff', '10')
        conn.execute_command('CONFIG', 'SET', 'ts-ignore-max-val-diff', '10.0')

        # Integer value options:
        conn.execute_command('CONFIG', 'GET', 'ts-num-threads')
        conn.execute_command('CONFIG', 'SET', 'ts-num-threads', '2')
        env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-num-threads')[1], b'2')

        conn.execute_command('CONFIG', 'GET', 'ts-retention-policy')
        conn.execute_command('CONFIG', 'SET', 'ts-retention-policy', '1')

        conn.execute_command('CONFIG', 'GET', 'ts-chunk-size-bytes')
        conn.execute_command('CONFIG', 'SET', 'ts-chunk-size-bytes', '2048')

        conn.execute_command('CONFIG', 'GET', 'ts-ignore-max-time-diff')
        conn.execute_command('CONFIG', 'SET', 'ts-ignore-max-time-diff', '5')

        assert not is_line_in_server_log(env, 'is deprecated, please use')

def test_module_config_from_module_arguments_raises_deprecation_messages():
    '''
    Tests that using the deprecated module configuration options
    (module arguments) while the module configuration API is used at the
    same time, leads to the deprecation messages.
    '''
    env = Env(noLog=False)
    if is_redis_version_lower_than(env, '7.0') or env.isCluster():
        env.skip()
    skip_on_rlec()

    # All these options are expected to return a valid value, so just
    # check that no exception is raised, no crashes observed and so on.
    args = [
        ['CHUNK_TYPE', 'compressed'],
        ['ENCODING', 'compressed'],
        ['COMPACTION_POLICY', 'max:1m:1d\\;min:10s:1h\\;avg:2h:10d\\;avg:3d:100d'],
        ['DUPLICATE_POLICY', 'MAX'],
        ['IGNORE_MAX_TIME_DIFF', '10'],
        ['IGNORE_MAX_VAL_DIFF', '10'],
        ['RETENTION_POLICY', '30'],
        ['CHUNK_SIZE_BYTES', '2048'],
        ['OSS_GLOBAL_PASSWORD', 'test'],
    ]

    for arg in args:
        env = Env(moduleArgs=f"{arg[0]} {arg[1]}", noLog=False)
        assert is_line_in_server_log(env, f"{arg[0]} is deprecated")
        assert is_line_in_server_log(env, 'Deprecated load-time configuration options were used')

def test_module_config_takes_precedence_over_module_arguments():
    '''
    Tests that using the deprecated module configuration options
    (module arguments) while also using the the module configuration API
    leads to the latter taking precedence.
    '''
    env = Env(noLog=False)
    if is_redis_version_lower_than(env, '7.0') or env.isCluster():
        env.skip()
    skip_on_rlec()

    # All these options are expected to return a valid value, so just
    # check that no exception is raised, no crashes observed and so on.
    args = [
        'CHUNK_TYPE', 'compressed',
        'ENCODING', 'compressed',
        'COMPACTION_POLICY', 'max:1m:1d\\;min:10s:1h\\;avg:2h:10d\\;avg:3d:100d',
        'DUPLICATE_POLICY', 'MAX',
        'IGNORE_MAX_TIME_DIFF', '10',
        'IGNORE_MAX_VAL_DIFF', '10',
        'RETENTION_POLICY', '30',
        'CHUNK_SIZE_BYTES', '2048',
        'OSS_GLOBAL_PASSWORD', 'test',
        'NUM_THREADS', '2',
        'LIBMR_PROTOCOL', 'INTERNAL',
    ]

    configFileContent = """
    ts-ignore-max-time-diff 20
    ts-chunk-size-bytes 4096
    ts-retention-policy 40
    ts-duplicate-policy last
    ts-compaction-policy max:1m:1d
    ts-encoding uncompressed
    ts-num-threads 4
    ts-libmr-protocol GEARS
    """

    env = Env(moduleArgs=args, redisConfigFileContent=configFileContent, noLog=False)
    assert is_line_in_server_log(env, " is deprecated")
    assert is_line_in_server_log(env, 'Deprecated load-time configuration options were used')

    with env.getConnection() as conn:
        env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-ignore-max-time-diff')[1], b'20')
        env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-chunk-size-bytes')[1], b'4096')
        env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-retention-policy')[1], b'40')
        env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-duplicate-policy')[1], b'last')
        env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-compaction-policy')[1], b'max:1m:1d')
        env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-encoding')[1], b'uncompressed')
        env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-num-threads')[1], b'4')
        # Immutable at runtime, but redis.conf still sets it at startup.
        env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-libmr-protocol')[1], b'GEARS')

def test_ts_num_threads_module_arg():
    '''
    Tests that ts-num-threads is accepted as a module-load argument,
    can be updated via CONFIG SET before LibMR runs, no deprecation
    warning is emitted, and the configured number of LibMR worker
    threads is actually spawned.

    Needs cluster mode (worker threads are created lazily on the
    first libmr execution, which only happens in cluster mode), so it
    forces an oss-cluster env in every lane. Also needs Redis >= 7.9
    (module config API, see RTS_RedisSupportsModuleConfigApi).

    Thread names are read from /proc on Linux or via ``sample`` on macOS.
    '''
    skip_on_rlec()
    # Check the server on the lane's own env before starting the cluster: a Redis without
    # cluster-bus-port-protected-mode rejects the directive and doesn't start.
    env = Env()
    if is_redis_version_lower_than(env, '7.9'):
        env.skip()
    bus_mode = os.getenv('CLUSTER_BUS_PROTECTED_MODE', 'no') or None
    if not env.cmd('CONFIG', 'GET', 'cluster-bus-port-protected-mode'):
        bus_mode = None
    # Non-cluster lanes don't pass tests.sh's cluster-bus flag, so set it here.
    env = Env(env='oss-cluster', shardsCount=2, useSlaves=False, useAof=False,
              clusterBusPortProtectedMode=bus_mode, moduleArgs="ts-num-threads 5", noLog=False)

    conn = env.getConnection(1)
    env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-num-threads')[1], b'5')
    conn.execute_command('CONFIG', 'SET', 'ts-num-threads', '6')
    env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-num-threads')[1], b'6')

    # TS.MRANGE triggers libmr, which forces the lazy thread pool to start.
    conn.execute_command('TS.MRANGE', '-', '+', 'FILTER', 'name=ts')

    names = get_worker_thread_names(conn)
    if names is None:
        assert False, "Failed to get worker thread names"
    # Worker threads are named timeseries-0 … timeseries-[n-1];
    env.assertEqual(len(names), 6,
                        message="Expected 6 LibMR worker threads")

    # Once the pool has started, a resize is refused and the old value stays.
    with env.assertResponseError(contained='Cannot set ts-num-threads after the LibMR worker pool has started'):
        conn.execute_command('CONFIG', 'SET', 'ts-num-threads', '8')
    env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-num-threads')[1], b'6')


def _module_names(conn):
    # RESP3 replies a map per module; RESP2 a flat [name, <name>, ver, ...] list.
    return [m[b'name'] if isinstance(m, dict) else m[1] for m in conn.execute_command('MODULE', 'LIST')]


def _timeseries_loaded(conn):
    return b'timeseries' in _module_names(conn)


class testRuntimeConfigErrors:
    """CONFIG SET rejections in src/config.c.

    No case changes a value (each one checks that), so all methods share one env.
    """

    def __init__(self):
        skip_on_rlec()
        self.env = Env(enableModuleCommand=True)
        # The ts-* configs need the module config API (RTS_RedisSupportsModuleConfigApi).
        if is_redis_version_lower_than(self.env, '7.9'):
            self.env.skip()
        self.conn = self.env.getConnection()

    def _get(self, name):
        return self.conn.execute_command('CONFIG', 'GET', name)[1]

    def _assert_rejected(self, cases):
        for name, value, msg in cases:
            before = self._get(name)
            with self.env.assertResponseError(contained=msg):
                self.conn.execute_command('CONFIG', 'SET', name, value)
            self.env.assertEqual(self._get(name), before, message=name)

    def test_invalid_values(self):
        self._assert_rejected([
            ('ts-compaction-policy', 'bogus', 'Invalid compaction policy: bogus'),
            ('ts-duplicate-policy', 'bogus', 'Invalid duplicate policy: bogus'),
            ('ts-encoding', 'bogus', 'Invalid encoding: bogus'),
            ('ts-ignore-max-val-diff', 'abc', 'Invalid value for `ts-ignore-max-val-diff`'),
            ('ts-ignore-max-val-diff', '-1', 'Value must be in the range [0.000000 .. '),
            # Inside the registered [48 .. 1048576] bounds, but not a multiple of 8.
            ('ts-chunk-size-bytes', '50', 'CHUNK_SIZE value must be a multiple of 8'),
            # Redis checks the registered bounds before calling the module, so the module's
            # own negative-value check for ts-ignore-max-time-diff is unreachable.
            ('ts-ignore-max-time-diff', '-1', 'argument must be between 0 and'),
        ])
        # The rejected requests left nothing behind: a valid one still works.
        before = self._get('ts-duplicate-policy')
        try:
            self.conn.execute_command('CONFIG', 'SET', 'ts-duplicate-policy', 'max')
            self.env.assertEqual(self._get('ts-duplicate-policy'), b'max')
        finally:
            self.conn.execute_command('CONFIG', 'SET', 'ts-duplicate-policy', before)

    def test_immutable(self):
        self.env.assertEqual(self._get('ts-topology-events'), b'yes')
        self.env.assertEqual(self._get('ts-libmr-protocol'), b'INTERNAL')
        self._assert_rejected([
            ('ts-topology-events', 'no', "can't set immutable config"),
            ('ts-libmr-protocol', 'GEARS', "can't set immutable config"),
        ])

    def test_load_twice(self):
        if self.env.isCluster():
            self.env.skip()
        # A second copy is refused in RedisModule_Init: the module name is taken.
        with self.env.assertResponseError(contained='Error loading the extension'):
            self.conn.execute_command('MODULE', 'LOAD', self.env.module[0])
        self.env.assertEqual(_module_names(self.conn).count(b'timeseries'), 1)
        self.env.assertEqual(self.conn.execute_command('TS.QUERYINDEX', 'a=b'), [])


def test_module_load_errors_and_forced_unload():
    """Module-argument errors and the unload path, on a server started without the module.

    A failed MODULE LOAD leaves no trace, so one server takes every error case. The forced
    unload must come last: it leaves the module's ACL category behind (see below).
    """
    skip_on_rlec()
    env = Env(module=[], enableModuleCommand=True, noLog=False)
    if env.isCluster() or is_redis_version_lower_than(env, '7.9'):
        env.skip()
    so = Defaults.module[0] if isinstance(Defaults.module, list) else Defaults.module
    conn = env.getConnection()
    # ParseNumThreadsArg logs at level "error", which Redis prints only at verbose.
    conn.execute_command('CONFIG', 'SET', 'loglevel', 'verbose')

    # Every option check starts with `argc > 1`, so a lone valueless option is ignored and
    # the module loads. A valid option in front makes the missing value an error.
    valid = ['RETENTION_POLICY', '0']
    for args, msg in [
        (['NUM_THREADS', 'abc'], 'Unable to parse argument after NUM_THREADS'),
        (['NUM_THREADS', '0'], 'Invalid value for NUM_THREADS. Must be between 1 and 16'),
        (['ts-num-threads', '17'], 'Invalid value for ts-num-threads. Must be between 1 and 16'),
        (valid + ['ts-topology-events'], 'Unable to parse argument after ts-topology-events'),
        (['ts-topology-events', 'maybe'], "Invalid value for ts-topology-events, must be 'yes' or 'no': maybe"),
        (valid + ['LIBMR_PROTOCOL'], 'Unable to parse argument after LIBMR_PROTOCOL'),
        (['LIBMR_PROTOCOL', 'bogus'], 'unknown LIBMR_PROTOCOL: bogus'),
        (valid + ['DEUBG_FORCE_RULE_DUMP'], 'Unable to parse argument after DEUBG_FORCE_RULE_DUMP'),
        (valid + ['DONT_ASSERT_ON_FAILIURE'], 'Unable to parse argument after DONT_ASSERT_ON_FAILIURE'),
    ]:
        with env.assertResponseError(contained='Error loading the extension'):
            conn.execute_command('MODULE', 'LOAD', so, *args)
        env.assertTrue(is_line_in_server_log(env, msg), message=msg)
        env.assertFalse(_timeseries_loaded(conn), message=msg)

    # ts-libmr-protocol is immutable at runtime, but LOADEX sets it during the load.
    with env.assertResponseError(contained='Error loading the extension'):
        conn.execute_command('MODULE', 'LOADEX', so, 'CONFIG', 'ts-libmr-protocol', 'bogus')
    env.assertTrue(is_line_in_server_log(env, 'Failed to load the RedisTimeSeries configurations'))
    env.assertFalse(_timeseries_loaded(conn))

    if SANITIZER or VALGRIND:
        # RedisModule_OnUnload frees only the config and the static context, so the
        # forced unload below leaks the rest of what OnLoad allocated.
        env.skip()

    # MODULE UNLOAD is refused because the module exports a data type, so the only way to
    # reach RedisModule_OnUnload is an unknown LOADEX config: Redis then unloads the module
    # right after a successful OnLoad. The password checks that FreeConfig releases it.
    with env.assertResponseError(contained='Error loading the extension'):
        conn.execute_command('MODULE', 'LOADEX', so, 'CONFIG', 'ts-no-such-config', '1',
                             'ARGS', 'global-password', 'secret')
    env.assertTrue(is_line_in_server_log(env, 'Loadex configurations were not applied'))
    env.assertTrue(is_line_in_server_log(env, 'Module timeseries unloaded'))
    env.assertFalse(_timeseries_loaded(conn))

    # Redis does not remove a module's ACL categories when it unloads it, so a reload
    # fails in RedisModule_AddACLCategory until the server restarts.
    env.assertContains(b'timeseries', conn.execute_command('ACL', 'CAT'))
    with env.assertResponseError(contained='Error loading the extension'):
        conn.execute_command('MODULE', 'LOAD', so)
    env.assertTrue(is_line_in_server_log(env, 'Failed to add ACL category'))
    env.assertFalse(_timeseries_loaded(conn))
    env.assertTrue(conn.ping())


def test_legacy_module_args_values():
    """Legacy module arguments that config.c parsed but no test passed yet."""
    skip_on_rlec()
    env = Env(moduleArgs='ts-num-threads 3; NUM_THREADS 2; ts-topology-events yes; LIBMR_PROTOCOL GEARS; '
                         'DEUBG_FORCE_RULE_DUMP disable; DONT_ASSERT_ON_FAILIURE disable',
              noLog=False)
    if env.isCluster() or is_redis_version_lower_than(env, '7.9'):
        env.skip()
    conn = env.getConnection()
    env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-num-threads'), [b'ts-num-threads', b'3'])
    env.assertTrue(is_line_in_server_log(
        env, 'Both ts-num-threads and NUM_THREADS were provided. Using ts-num-threads.'))
    env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-topology-events'), [b'ts-topology-events', b'yes'])
    env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-libmr-protocol'), [b'ts-libmr-protocol', b'GEARS'])
    env.assertTrue(is_line_in_server_log(env, 'LIBMR_PROTOCOL is deprecated'))
