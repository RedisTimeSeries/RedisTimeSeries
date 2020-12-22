import os

from RLTest import Env
from create_test_rdb_file import load_into_redis


def normalize_info(data):
    info = {}
    for i in range(0, len(data), 2):
        info[data[i]] = data[i + 1]
    info.pop(b'memoryUsage')
    info.pop(b'chunkSize')
    info.pop(b'chunkType')
    return info


def testRDBCompatibility():
    env = Env()
    env.skipOnCluster()
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
        env.assertEqual(OLD_KEYS, KEYS)
        for key in OLD_KEYS:
            assert r.execute_command('ts.range', key, "-", "+") == TSRANGE_RESULTS[key]
            assert normalize_info(r.execute_command('ts.info', key)) == TSINFO_RESULTS[key]
