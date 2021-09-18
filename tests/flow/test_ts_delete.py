from RLTest import Env
import redis
from test_helper_classes import _get_ts_info
from includes import *


def test_ts_del_uncompressed(env):
    # total samples = 101
    sample_len = 101
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 'test_key', 'uncompressed')

        for i in range(sample_len):
            env.expect("ts.add", 'test_key', i, '1', conn=r).equal(i)

        res = r.execute_command('ts.range', 'test_key', 0, 100)
        i = 0
        for sample in res:
            env.assertEqual([i, '1'], sample)
            i += 1
        r.execute_command('ts.del', 'test_key', 0, 100)
        res = r.execute_command('ts.range', 'test_key', 0, 100)
        env.assertEqual(0, len(res))


def test_ts_del_uncompressed_in_range(env):
    sample_len = 101
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 'test_key', 'uncompressed')

        for i in range(sample_len):
            env.expect("ts.add", 'test_key', i, '1', conn=r).equal(i)

        res = r.execute_command('ts.range', 'test_key', 0, 100)
        i = 0
        for sample in res:
            env.assertEqual([i, '1'], sample)
            i += 1
        # delete 11 samples
        env.expect('ts.del', 'test_key', 50, 60, conn=r).equal(11)
        res = r.execute_command('ts.range', 'test_key', 0, 100)
        env.assertEqual(90, len(res))


def test_ts_del_compressed(env):
    sample_len = 101
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 'test_key')

        for i in range(sample_len):
            env.expect("ts.add", 'test_key', i, '1', conn=r).equal(i)

        res = r.execute_command('ts.range', 'test_key', 0, 100)
        i = 0
        for sample in res:
            env.assertEqual([i, '1'], sample)
            i += 1
        env.expect('ts.del', 'test_key', 0, 100, conn=r).equal(sample_len)
        res = r.execute_command('ts.range', 'test_key', 0, 100)
        env.assertEqual(0, len(res))


def test_ts_del_multi_chunk():
    for CHUNK_TYPE in ["compressed","uncompressed"]:
        sample_len = 1
        env = Env()
        with env.getClusterConnectionIfNeeded() as r:
            r.execute_command("ts.create", 'test_key', CHUNK_TYPE)
            while _get_ts_info(r, 'test_key').chunk_count < 2:
                env.expect("ts.add", 'test_key', sample_len, '1', conn=r).equal(sample_len)
                sample_len = sample_len + 1
            sample_len = sample_len -1
            res = r.execute_command('ts.range', 'test_key', 0, sample_len - 1)
            i = 1
            for sample in res:
                env.assertEqual(sample, [i, '1'])
                i += 1
            env.expect('ts.del', 'test_key', 0, sample_len - 1, conn=r).equal(sample_len - 1)
            res = r.execute_command('ts.range', 'test_key', 0, sample_len)
            env.assertEqual(_get_ts_info(r, 'test_key').chunk_count,1)
            env.assertEqual(len(res), 1)
        env.flush()


def test_ts_del_compressed_out_range(env):
    sample_len = 101
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command("ts.create", 'test_key')

        for i in range(sample_len):
             env.expect("ts.add", 'test_key', i + 100, '1', conn=r).equal(i + 100)

        res = r.execute_command('ts.range', 'test_key', 0 + 100, sample_len + 100 - 1)
        i = 0
        for sample in res:
            env.assertEqual([i + 100, '1'], sample)
            i += 1
        env.expect('ts.del', 'test_key', 0, 500, conn=r).equal(sample_len)
        res = r.execute_command('ts.range', 'test_key', 0 + 100, sample_len + 100 - 1)
        env.assertEqual(0, len(res))


def test_bad_del(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect("ts.del", "test_key", 100, 200, conn=r).raiseError()

        r.execute_command("ts.add", 'test_key', 120, '1')
        r.execute_command("ts.add", 'test_key', 140, '5')
        env.expect("ts.del", "test_key", 100, conn=r)

        env.expect("ts.del", "test_key", 100, '200a', conn=r).raiseError()
        env.expect("ts.del", "test_key", 200, 100, conn=r).equal(0)
        env.expect("ts.del", "test_key", 100, 300, conn=r).equal(2)

        env.expect("set", "BAD_X", "NOT_TS", conn=r).equal(True)
        env.expect("ts.del", "BAD_X", 100, 200, conn=r).raiseError()
