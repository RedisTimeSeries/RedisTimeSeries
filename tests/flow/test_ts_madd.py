import time

from RLTest import Env
from includes import *


def test_madd(env):
    sample_len = 1024
    env.skipOnCluster()
    with env.getConnection() as r:
        r.execute_command("ts.create", 'test_key1')
        r.execute_command("ts.create", 'test_key2')
        r.execute_command("ts.create", 'test_key3')

        for i in range(sample_len):
            env.expect("ts.madd", 'test_key1', i + 1000, i, 'test_key2', i + 3000, i, 'test_key3', i + 6000, i, conn=r).\
                equal([i + 1000, i + 3000, i + 6000])

        res = r.execute_command('ts.range', 'test_key1', 1000, 1000 + sample_len)
        i = 0
        for sample in res:
            env.assertEqual(sample, [1000 + i, str(i)])
            i += 1

        res = r.execute_command('ts.range', 'test_key2', 3000, 3000 + sample_len)
        i = 0
        for sample in res:
            env.assertEqual(sample, [3000 + i, str(i)])
            i += 1

        res = r.execute_command('ts.range', 'test_key3', 6000, 6000 + sample_len)
        i = 0
        for sample in res:
            env.assertEqual(sample, [6000 + i, str(i)])
            i += 1


def test_ooo_madd(env):
    sample_len = 100
    start_ts = 1600204334000

    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 'test_key1')
        last_sample = None
        samples = []
        for i in range(0, sample_len, 3):
            exp_res = [start_ts + (i * 1000 + 2000), start_ts + (i * 1000 + 1000), start_ts + (i * 1000)]
            env.expect("ts.madd", 'test_key1', start_ts + (i * 1000 + 2000), i, 'test_key1', 
                       start_ts + i * 1000 + 1000, i, 'test_key1', start_ts + i * 1000, i, conn=r).equal(exp_res)
            samples.append([start_ts + (i * 1000), str(i)])
            samples.append([start_ts + (i * 1000 + 1000), str(i)])
            samples.append([start_ts + (i * 1000 + 2000), str(i)])
            last_sample = [start_ts + (i * 1000 + 2000), str(i)]

        env.expect('ts.get', 'test_key1', conn=r).equal(last_sample)
        env.expect('ts.range', 'test_key1', '-', '+', conn=r).equal(samples)


def test_partial_madd(env):
    env.skipOnCluster()
    with env.getConnection() as r:
        r.execute_command("ts.create", 'test_key1')
        r.execute_command("ts.create", 'test_key2')
        r.execute_command("ts.create", 'test_key3')

        now = int(time.time() * 1000)
        res = r.execute_command("ts.madd", 'test_key1', "*", 10, 'test_key2', 2000, 20, 'test_key3', 3000, 30)
        env.assertTrue(now <= res[0])
        env.assertEqual(2000, res[1])
        env.assertEqual(3000, res[2])

        res = r.execute_command("ts.madd", 'test_key1', now + 1000, 10, 'test_key2', 1000, 20, 'test_key3', 3001, 30)
        env.assertEqual((now + 1000, 1000, 3001), (res[0], res[1], res[2]))
        env.expect('ts.range', 'test_key1', "-", "+", conn=r).apply(len).equal(2)
        env.expect('ts.range', 'test_key2', "-", "+", conn=r).apply(len).equal(2)
        env.expect('ts.range', 'test_key3', "-", "+", conn=r).apply(len).equal(2)


def test_extensive_ts_madd(env):
    env.skipOnCluster()
    with env.getConnection() as r:
        r.execute_command("ts.create", 'test_key1')
        r.execute_command("ts.create", 'test_key2')
        pos = 1
        lines = []
        float_lines = []
        with open("lemire_canada.txt","r") as file:
            lines = file.readlines()
        for line in lines:
            float_v = float(line.strip())
            env.expect("ts.madd", 'test_key1', pos, float_v, 'test_key2', pos, float_v, conn=r).equal([pos, pos])
            pos += 1
            float_lines.append(float_v)
        returned_floats = r.execute_command('ts.range', 'test_key1', "-", "+")
        env.assertEqual(len(returned_floats), len(float_lines))
        for pos,datapoint in enumerate(returned_floats,start=1):
            env.assertEqual(pos, datapoint[0])
            env.assertEqual(float_lines[pos-1], float(datapoint[1]))
