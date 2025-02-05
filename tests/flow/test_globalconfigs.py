# import pytest
# from utils import Env
from test_helper_classes import TSInfo
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

        info = r.execute_command('TS.INFO', 't1_MAX_1000_500')
        assert info == [b'totalSamples', 2, b'memoryUsage', 4201, b'firstTimestamp', 0, b'lastTimestamp', 2500, b'retentionTime', 0, b'chunkCount', 1, b'chunkSize', 4096, b'chunkType', b'uncompressed', b'duplicatePolicy', None, b'labels', [[b'aggregation', b'MAX'], [b'time_bucket', b'1000']], b'sourceKey', b't1', b'rules', [],  b'ignoreMaxTimeDiff', 0, b'ignoreMaxValDiff', b'0']

        info = r.execute_command('TS.INFO', 't1', 'DEBUG')
        assert info == [b'totalSamples', 3, b'memoryUsage', 4248, b'firstTimestamp', 1, b'lastTimestamp', 5000, b'retentionTime', 0, b'chunkCount', 1, b'chunkSize', 4096, b'chunkType', b'compressed', b'duplicatePolicy', None, b'labels', [], b'sourceKey', None, b'rules', [[b't1_MAX_1000_500', 1000, b'MAX', 500]], b'ignoreMaxTimeDiff', 0, b'ignoreMaxValDiff', b'0', b'keySelfName', b't1', b'Chunks', [[b'startTimestamp', 1, b'endTimestamp', 5000, b'samples', 3, b'size', 4096, b'bytesPerSample', b'1365.3333740234375']]]

        info = r.execute_command('TS.INFO', 't1')
        assert info == [b'totalSamples', 3, b'memoryUsage', 4248, b'firstTimestamp', 1, b'lastTimestamp', 5000, b'retentionTime', 0, b'chunkCount', 1, b'chunkSize', 4096, b'chunkType', b'compressed', b'duplicatePolicy', None, b'labels', [], b'sourceKey', None, b'rules', [[b't1_MAX_1000_500', 1000, b'MAX', 500]], b'ignoreMaxTimeDiff', 0, b'ignoreMaxValDiff', b'0']

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

    assert is_line_in_server_log(env, f"{arg[0]} is deprecated, please use")
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

        conn.execute_command('CONFIG', 'GET', 'ts-global-password')

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

        # Can't set an immutable config value.
        with pytest.raises(redis.exceptions.ResponseError):
            conn.execute_command('CONFIG', 'SET', 'ts-num-threads', '2')

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
        assert is_line_in_server_log(env, f"{arg[0]} is deprecated, please use")
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
    ]

    configFileContent = """
    ts-ignore-max-time-diff 20
    ts-chunk-size-bytes 4096
    ts-retention-policy 40
    ts-duplicate-policy last
    ts-compaction-policy max:1m:1d
    ts-encoding uncompressed
    ts-global-password test2
    """

    env = Env(moduleArgs=args, redisConfigFileContent=configFileContent, noLog=False)
    assert is_line_in_server_log(env, " is deprecated, please use")
    assert is_line_in_server_log(env, 'Deprecated load-time configuration options were used')

    with env.getConnection() as conn:
        env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-ignore-max-time-diff')[1], b'20')
        env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-chunk-size-bytes')[1], b'4096')
        env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-retention-policy')[1], b'40')
        env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-duplicate-policy')[1], b'last')
        env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-compaction-policy')[1], b'max:1m:1d')
        env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-encoding')[1], b'uncompressed')
        env.assertEqual(conn.execute_command('CONFIG', 'GET', 'ts-global-password')[1], b'test2')
