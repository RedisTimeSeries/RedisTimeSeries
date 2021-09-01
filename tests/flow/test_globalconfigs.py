import pytest
from RLTest import Env
from test_helper_classes import TSInfo
from includes import *

class testModuleLoadTimeArguments(object):
    def __init__(self):
        self.test_variations = [(True, 'CHUNK_SIZE_BYTES 2000'),
                                (True, 'COMPACTION_POLICY', 'max:1m:1d\\;min:10s:1h\\;avg:2h:10d\\;avg:3d:100d'),
                                (True, 'DUPLICATE_POLICY MAX'),
                                (True, 'RETENTION_POLICY 30')
                                ]

    def test(self):
        Env().skipOnCluster()
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


def test_encoding_uncompressed(env):
    env.skipOnCluster()
    env = Env(moduleArgs='ENCODING UNCOMPRESSED; COMPACTION_POLICY max:1s:1m')
    with env.getConnection() as r:
        r.execute_command('FLUSHALL')
        r.execute_command('TS.ADD', 't1', '1', 1.0)
        assert TSInfo(r.execute_command('TS.INFO', 't1_MAX_1000')).chunk_type == 'uncompressed'


def test_encoding_compressed(env):
    env.skipOnCluster()
    env = Env(moduleArgs='ENCODING compressed; COMPACTION_POLICY max:1s:1m')
    with env.getConnection() as r:
        r.execute_command('FLUSHALL')
        r.execute_command('TS.ADD', 't1', '1', 1.0)
        assert TSInfo(r.execute_command('TS.INFO', 't1_MAX_1000')).chunk_type == 'compressed'

def test_uncompressed(env):
    env.skipOnCluster()
    env = Env(moduleArgs='CHUNK_TYPE UNCOMPRESSED; COMPACTION_POLICY max:1s:1m')
    with env.getConnection() as r:
        r.execute_command('FLUSHALL')
        r.execute_command('TS.ADD', 't1', '1', 1.0)
        assert TSInfo(r.execute_command('TS.INFO', 't1_MAX_1000')).chunk_type == 'uncompressed'


def test_compressed(env):
    env.skipOnCluster()
    env = Env(moduleArgs='CHUNK_TYPE compressed; COMPACTION_POLICY max:1s:1m')
    with env.getConnection() as r:
        r.execute_command('FLUSHALL')
        r.execute_command('TS.ADD', 't1', '1', 1.0)
        assert TSInfo(r.execute_command('TS.INFO', 't1_MAX_1000')).chunk_type == 'compressed'

def test_compressed_debug(env):
    env.skipOnCluster()

    env = Env(moduleArgs='CHUNK_TYPE compressed COMPACTION_POLICY max:1s:1m')
    with env.getConnection() as r:
        r.execute_command('FLUSHALL')
        r.execute_command('TS.ADD', 't1', '1', 1.0)
        r.execute_command('TS.ADD', 't1', '3000', 1.0)
        r.execute_command('TS.ADD', 't1', '5000', 1.0)

        assert TSInfo(r.execute_command('TS.INFO', 't1_MAX_1000', 'DEBUG')).chunks == [['startTimestamp', 0, 'endTimestamp', 3000, 'samples', 2, 'size', 4096, 'bytesPerSample', '2048']]

class testGlobalConfigTests():

    def __init__(self):
        Env().skipOnCluster()
        self.env = Env(moduleArgs='COMPACTION_POLICY max:1m:1d\\;min:10s:1h\\;avg:2h:10d\\;avg:3d:100d')

    def test_autocreate(self):
        with self.env.getConnection() as r:
            assert r.execute_command('TS.ADD tester 1980 0 LABELS name',
                                     'brown color pink') == 1980
            keys = r.execute_command('keys *')
            keys = sorted(keys)
            assert keys == ['tester', 'tester_AVG_259200000', 'tester_AVG_7200000', 'tester_MAX_1',
                            'tester_MIN_10000']
            r.execute_command('TS.ADD tester 1981 1')

            r.execute_command('set exist_MAX_1 foo')
            r.execute_command('TS.ADD exist 1980 0')
            keys = r.execute_command('keys *')
            keys = sorted(keys)
            assert keys == ['exist', 'exist_AVG_259200000', 'exist_AVG_7200000', 'exist_MAX_1', 'exist_MIN_10000',
                            'tester', 'tester_AVG_259200000', 'tester_AVG_7200000', 'tester_MAX_1',
                            'tester_MIN_10000']
            r.execute_command('TS.ADD exist 1981 0')

    def test_big_compressed_chunk_reverserange(self):
        with self.env.getConnection() as r:
            r.execute_command('del tester')
            start_ts = 1599941160000
            last_ts = 0
            samples = []
            for i in range(4099):
                last_ts = start_ts + i * 60000
                samples.append([last_ts, '1'])
                r.execute_command('TS.ADD', 'tester', last_ts, 1)
            rev_samples = list(samples)
            rev_samples.reverse()
            assert r.execute_command('TS.GET', 'tester') == [last_ts, '1']
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


def test_negative_configuration(env):
    env.skipOnCluster()
    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_SIZE_BYTES 100; DUPLICATE_POLICY abc')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_SIZE_BYTES 100; DUPLICATE_POLICY')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_SIZE_BYTES 100; CHUNK_TYPE')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_SIZE_BYTES 100; ENCODING')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='ENCODING; CHUNK_SIZE_BYTES 100')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='ENCODING abc; CHUNK_SIZE_BYTES 100')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_TYPE; CHUNK_SIZE_BYTES 100')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_TYPE compressed; COMPACTION_POLICY')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_TYPE compressed; COMPACTION_POLICY NOT_A_REAL_POLICY')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_TYPE compressed; RETENTION_POLICY')

    with pytest.raises(Exception) as excinfo:
        env = Env(moduleArgs='CHUNK_TYPE compressed; CHUNK_SIZE_BYTES')
