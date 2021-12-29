import os

from RLTest import Env
from create_test_rdb_file import load_into_redis
from test_helper_classes import _get_ts_info
from includes import *


def normalize_info(data):
    info = {}
    for i in range(0, len(data), 2):
        info[data[i]] = data[i + 1]
    info.pop(b'memoryUsage')
    info.pop(b'chunkSize')
    info.pop(b'chunkType')
    return info

def testRDB():
    env = Env()
    env.skipOnCluster()
    with Env().getConnection() as r:
        assert r.execute_command('TS.CREATE', 'ts1', 'RETENTION', '1000', 'CHUNK_SIZE', '1024',
                            'ENCODING', 'UNCOMPRESSED', 'DUPLICATE_POLICY', 'min',
                            'LABELS', 'name',
                            'brown', 'color', 'pink')
        r.execute_command('TS.ADD', 'ts1', 100, 99)
        r.execute_command('TS.ADD', 'ts1', 110, 500.5)
        dump = r.execute_command("dump", "ts1")
        assert r.execute_command("restore", "ts2", "0", dump)
        info1 = r.execute_command('TS.INFO', 'ts1', 'DEBUG')
        info2 = r.execute_command('TS.INFO', 'ts2', 'DEBUG')
        assert info1 == info2

def testRDBCompatibility():
    env = Env()
    env.skipOnCluster()
    skip_on_rlec()
    env.skipOnAOF()
    RDBS = os.listdir('rdbs')

    # Current RDB version check
    TSRANGE_RESULTS = {}
    TSINFO_RESULTS = {}
    KEYS = None

    current_r = env.getConnection()
    load_into_redis(current_r)
    KEYS = current_r.keys()
    KEYS.sort()
    for key in KEYS:
        TSRANGE_RESULTS[key] = current_r.execute_command('ts.range', key, "-", "+")
        TSINFO_RESULTS[key] = normalize_info(current_r.execute_command('ts.info', key))

    # Compatibility check
    for fileName in RDBS:
        filePath = os.path.abspath(os.path.join("rdbs", fileName))
        dbFileName = env.cmd('config', 'get', 'dbfilename')[1].decode('ascii')
        dbDir = env.cmd('config', 'get', 'dir')[1].decode('ascii')
        rdbFilePath = os.path.join(dbDir, dbFileName)
        env.stop()
        try:
            os.remove(rdbFilePath)
        except FileNotFoundError:
            pass
        os.symlink(filePath, rdbFilePath)
        env.start()

        r = env.getConnection()
        OLD_KEYS = r.keys()
        newDbFileName = r.execute_command('config', 'get', 'dbfilename')[1].decode('ascii')
        env.assertEqual(newDbFileName, dbFileName)
        OLD_KEYS.sort()

        if(fileName == "1.4.9_with_avg_ctx.rdb"):
            keys = r.keys()
            keys.sort()
            assert keys == [b'ts1', b'ts2']
            assert r.execute_command('ts.info', 'ts1') == [b'totalSamples', 2, b'memoryUsage', 4240, b'firstTimestamp', 100, b'lastTimestamp', 120, b'retentionTime', 0, b'chunkCount', 1, b'chunkSize', 4096, b'chunkType', b'compressed', b'duplicatePolicy', None, b'labels', [], b'sourceKey', None, b'rules', [[b'ts2', 1000, b'AVG']]]
            assert r.execute_command('ts.info', 'ts2') == [b'totalSamples', 0, b'memoryUsage', 4184, b'firstTimestamp', 0, b'lastTimestamp', 0, b'retentionTime', 0, b'chunkCount', 1, b'chunkSize', 4096, b'chunkType', b'compressed', b'duplicatePolicy', None, b'labels', [], b'sourceKey', b'ts1', b'rules', []]
            assert r.execute_command('ts.range', 'ts1', '-', '+') == [[100, b'3'], [120, b'5']]
            assert r.execute_command('ts.range', 'ts2', '-', '+') == []
            assert r.execute_command('ts.add', 'ts1', 1500, 100)
            assert r.execute_command('ts.range', 'ts2', '-', '+') == [[0, b'4']]

            continue

        env.assertEqual(OLD_KEYS, KEYS)
        for key in OLD_KEYS:
            assert r.execute_command('ts.range', key, "-", "+") == TSRANGE_RESULTS[key]
            assert normalize_info(r.execute_command('ts.info', key)) == TSINFO_RESULTS[key]
