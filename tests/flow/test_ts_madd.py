import os
import time
import aof_parser

from RLTest import Env, StandardEnv
from includes import *


def test_madd():
    sample_len = 1024
    Env().skipOnCluster()
    skip_on_rlec()
    with Env().getConnection() as r:
        r.execute_command("ts.create", 'test_key1')
        r.execute_command("ts.create", 'test_key2')
        r.execute_command("ts.create", 'test_key3')

        for i in range(sample_len):
            assert [i + 1000, i + 3000, i + 6000] == r.execute_command("ts.madd", 'test_key1', i + 1000, i,
                                                                       'test_key2', i + 3000, i, 'test_key3',
                                                                       i + 6000, i, )

        res = r.execute_command('ts.range', 'test_key1', 1000, 1000 + sample_len)
        i = 0
        for sample in res:
            assert sample == [1000 + i, str(i).encode('ascii')]
            i += 1

        res = r.execute_command('ts.range', 'test_key2', 3000, 3000 + sample_len)
        i = 0
        for sample in res:
            assert sample == [3000 + i, str(i).encode('ascii')]
            i += 1

        res = r.execute_command('ts.range', 'test_key3', 6000, 6000 + sample_len)
        i = 0
        for sample in res:
            assert sample == [6000 + i, str(i).encode('ascii')]
            i += 1


def test_ooo_madd():
    sample_len = 100
    start_ts = 1600204334000

    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 'test_key1')
        last_sample = None
        samples = []
        for i in range(0, sample_len, 3):
            assert [start_ts + (i * 1000 + 2000), start_ts + (i * 1000 + 1000),
                    start_ts + (i * 1000)] == r.execute_command("ts.madd", 'test_key1', start_ts + (i * 1000 + 2000), i,
                                                                'test_key1', start_ts + i * 1000 + 1000, i, 'test_key1',
                                                                start_ts + i * 1000, i)
            samples.append([start_ts + (i * 1000), str(i).encode('ascii')])
            samples.append([start_ts + (i * 1000 + 1000), str(i).encode('ascii')])
            samples.append([start_ts + (i * 1000 + 2000), str(i).encode('ascii')])
            last_sample = [start_ts + (i * 1000 + 2000), str(i).encode('ascii')]

        assert r.execute_command('ts.get', 'test_key1') == last_sample
        assert r.execute_command('ts.range', 'test_key1', '-', '+') == samples


def test_partial_madd():
    Env().skipOnCluster()
    skip_on_rlec()
    with Env().getConnection() as r:
        r.execute_command("ts.create", 'test_key1')
        r.execute_command("ts.create", 'test_key2')
        r.execute_command("ts.create", 'test_key3')

        now = int(time.time() * 1000)
        res = r.execute_command("ts.madd", 'test_key1', "*", 10, 'test_key2', 2000, 20, 'test_key3', 3000, 30)
        assert now <= res[0]
        assert 2000 == res[1]
        assert 3000 == res[2]

        res = r.execute_command("ts.madd", 'test_key1', now + 1000, 10, 'test_key2', 1000, 20, 'test_key3', 3001, 30)
        assert (now + 1000, 1000, 3001) == (res[0], res[1], res[2])
        assert len(r.execute_command('ts.range', 'test_key1', "-", "+")) == 2
        assert len(r.execute_command('ts.range', 'test_key2', "-", "+")) == 2
        assert len(r.execute_command('ts.range', 'test_key3', "-", "+")) == 2


def test_extensive_ts_madd():
    Env().skipOnCluster()
    skip_on_rlec()
    with Env(decodeResponses=True).getConnection() as r:
        r.execute_command("ts.create", 'test_key1')
        r.execute_command("ts.create", 'test_key2')
        pos = 1
        lines = []
        float_lines = []
        with open("lemire_canada.txt","r") as file:
            lines = file.readlines()
        for line in lines:
            float_v = float(line.strip())
            res = r.execute_command("ts.madd", 'test_key1', pos, float_v, 'test_key2', pos, float_v)
            assert res == [pos,pos]
            pos=pos+1
            float_lines.append(float_v)
        returned_floats = r.execute_command('ts.range', 'test_key1', "-", "+")
        assert len(returned_floats) == len(float_lines)
        for pos,datapoint in enumerate(returned_floats,start=1):
            assert pos == datapoint[0]
            assert float_lines[pos-1] == float(datapoint[1])

def test_madd_some_failed_replicas():
    if not Env().useSlaves:
        Env().skip()
    # getSlaveConnection is not supported in cluster mode
    Env().skipOnCluster()
    env =  Env(decodeResponses=False)
    res = env.cmd('CONFIG', 'GET', 'appendfsync')
    env.cmd('CONFIG', 'SET', 'appendfsync', 'always')

    with env.getClusterConnectionIfNeeded() as r:
        is_less_than_ver7 = is_redis_version_smaller_than(r, "7.0.0")

        r.execute_command("ts.create", "test_key1", 'RETENTION', 100, 'ENCODING', 'UNCOMPRESSED', 'CHUNK_SIZE', 128, "DUPLICATE_POLICY", "block", 'LABELS', 'name', 'Or', 'color', 'pink')
        r.execute_command("set", "test_key3", "danni")
        r.execute_command("ts.madd", "test_key1", 123, 11, "test_key1", 124, 12)
        r.execute_command("ts.madd", "test_key1", 122, 11, "test_key1", 123, 11, "test_key1", 124, 12, "test_key1", 125, 12)
        r.execute_command("ts.add", "test_key4", 110, 500.5, 'RETENTION' , 100, 'ENCODING', 'UNCOMPRESSED', 'CHUNK_SIZE', 128, 'ON_DUPLICATE', 'last', 'LABELS', 'name', 'Or', 'color', 'pink')
        r.execute_command("ts.incrby", "test_key5", 110, 'TIMESTAMP', 50, 'RETENTION', 100, 'UNCOMPRESSED', 'CHUNK_SIZE', 128, 'LABELS', 'name', 'Or', 'color', 'pink')
        r.execute_command("ts.decrby", "test_key6", 110, 'TIMESTAMP', 50, 'RETENTION', 100, 'UNCOMPRESSED', 'CHUNK_SIZE', 128, 'LABELS', 'name', 'Or', 'color', 'pink')
        r.execute_command("ts.createrule", "test_key5", "test_key6", 'AGGREGATION', 'sum', 10, 5)
        r.execute_command("ts.del", "test_key5", 100, 200)
        r.execute_command("ts.alter", "test_key6", 'RETENTION', 100, 'CHUNK_SIZE', 128, 'DUPLICATE_POLICY', 'last', 'LABELS', 'name', 'Or', 'color', 'pink')

        # Check non existant key
        r.execute_command("ts.madd", "test_key3", 122, 11, "test_key1", 123, 11, "test_key1", 124, 12, "test_key1", 125, 12)
        r.execute_command("ts.madd", "test_key3", "aaaaa", 11, "test_key1", 123, 11, "test_key1", 124, 12, "test_key1", 125, 12)
        r.execute_command("ts.madd", "test_key3", -222, 11, "test_key1", 123, 11, "test_key1", 124, 12, "test_key1", 125, 12)
        r.execute_command("ts.madd", "test_key3", -222, "bbbbbbb", "test_key1", 123, 11, "test_key1", 124, 12, "test_key1", 125, 12)

        r.execute_command("wait", 1, 0)
        env.assertEqual(r.execute_command("ts.range", "test_key1", "-", "+"), [[122, b'11'], [123, b'11'], [124, b'12'], [125, b'12']])
        assert r.execute_command("ts.range", "test_key4", "-", "+") == [[110, b'500.5']]

    with env.getSlaveConnection() as r:
        env.assertEqual(r.execute_command("ts.range", "test_key1", "-", "+"),  [[122, b'11'], [123, b'11'], [124, b'12'], [125, b'12']])

    if env.useAof and isinstance(env.envRunner, StandardEnv) and not DISABLE_AOF_PARSER:
        dbDirPath = env.envRunner.dbDirPath
        if not is_less_than_ver7:
            dbDirPath = dbDirPath + "/appendonlydir/"
        path = os.path.join(dbDirPath, env.envRunner._getFileName('master', '.aof'))
        if not is_less_than_ver7:
            path = path + ".1.incr.aof"
        cmds = aof_parser.parse_file(path)
        cmds_filtered = filter(lambda c: c[0].lower().startswith('ts.'), cmds)
        try:
            env.assertEqual(list(cmds_filtered),
                            [['ts.create', "test_key1", 'RETENTION', '100', 'ENCODING', 'UNCOMPRESSED', 'CHUNK_SIZE', '128', "DUPLICATE_POLICY", "block", 'LABELS', 'name', 'Or', 'color', 'pink'],
                            ['TS.MADD', 'test_key1', '123', '11', 'test_key1', '124', '12'],
                            ['TS.MADD', 'test_key1', '122', '11', 'test_key1', '125', '12'],
                            ['TS.ADD', 'test_key4', '110', '500.5', 'RETENTION', '100', 'ENCODING', 'UNCOMPRESSED', 'CHUNK_SIZE', '128', 'ON_DUPLICATE', 'last', 'LABELS', 'name', 'Or', 'color', 'pink'],
                            ["ts.incrby", "test_key5", '110', 'TIMESTAMP', '50', 'RETENTION', '100', 'UNCOMPRESSED', 'CHUNK_SIZE', '128', 'LABELS', 'name', 'Or', 'color', 'pink'],
                            ["ts.decrby", "test_key6", '110', 'TIMESTAMP', '50', 'RETENTION', '100', 'UNCOMPRESSED', 'CHUNK_SIZE', '128', 'LABELS', 'name', 'Or', 'color', 'pink'],
                            ["ts.createrule", "test_key5", "test_key6", 'AGGREGATION', 'sum', '10', '5'],
                            ["ts.del", "test_key5", '100', '200'],
                            ["ts.alter", "test_key6", 'RETENTION', '100', 'CHUNK_SIZE', '128', 'DUPLICATE_POLICY', 'last', 'LABELS', 'name', 'Or', 'color', 'pink']
                            ])
        except Exception:
            print(cmds)
            print(cmds_filtered)
            raise

    # rollback the config change
    env.cmd('CONFIG', 'SET', 'appendfsync', res[1])
