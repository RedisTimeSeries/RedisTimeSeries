from test_helper_classes import TSInfo
from utils import Env
from includes import *


def test_rename_src(env):
    with env.getClusterConnectionIfNeeded() as r:

        env.expect('TS.CREATE', 'a1{1}', conn=r).noError()
        env.expect('TS.CREATE', 'b{1}', conn=r).noError()

        env.assertTrue(r.execute_command('RENAME', 'a1{1}', 'a2{1}'))
        aInfo = TSInfo(r.execute_command('TS.INFO', 'a2{1}'))
        env.assertEqual(aInfo.sourceKey, None)
        env.assertEqual(aInfo.rules, [])

        env.expect('TS.CREATERULE', 'a2{1}', 'b{1}', 'AGGREGATION', 'AVG', 5000, conn=r).noError()
        bInfo = TSInfo(r.execute_command('TS.INFO', 'b{1}'))
        env.assertEqual(bInfo.sourceKey, 'a2{1}')
        env.assertEqual(bInfo.rules, [])

        env.assertTrue(r.execute_command('RENAME', 'a2{1}', 'a3{1}'))
        bInfo = TSInfo(r.execute_command('TS.INFO', 'b{1}'))
        env.assertEqual(bInfo.sourceKey, 'a3{1}')
        env.assertEqual(bInfo.rules, [])


def test_rename_dst(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.CREATE', 'a{2}', conn=r).noError()
        env.expect('TS.CREATE', 'b{2}', conn=r).noError()
        env.expect('TS.CREATERULE', 'a{2}', 'b{2}', 'AGGREGATION', 'AVG', 5000, conn=r).noError()

        env.assertTrue(r.execute_command('RENAME', 'b{2}', 'b1{2}'))
        aInfo = TSInfo(r.execute_command('TS.INFO', 'a{2}'))
        env.assertEqual(aInfo.sourceKey, None)
        env.assertEqual(aInfo.rules[0][0], 'b1{2}')

        env.expect('TS.CREATE', 'c{2}', conn=r).noError()
        env.expect('TS.CREATERULE', 'a{2}', 'c{2}', 'AGGREGATION', 'COUNT', 2000, conn=r).noError()

        env.expect('TS.CREATE', 'd{2}', conn=r).noError()
        env.expect('TS.CREATERULE', 'a{2}', 'd{2}', 'AGGREGATION', 'SUM', 3000, conn=r).noError()

        env.assertTrue(r.execute_command('RENAME', 'c{2}', 'c1{2}'))
        aInfo = TSInfo(r.execute_command('TS.INFO', 'a{2}'))
        env.assertEqual(aInfo.sourceKey, None)
        env.assertEqual(aInfo.rules[0][0], 'b1{2}')
        env.assertEqual(aInfo.rules[1][0], 'c1{2}')
        env.assertEqual(aInfo.rules[2][0], 'd{2}')


def test_rename_indexed(env):
    with env.getClusterConnectionIfNeeded() as r:
        env.expect('TS.ADD', 'a{3}', 100, 200, 'LABELS', 'sensor_id', '2', 'area_id', '32', conn=r).noError()
        env.expect('TS.MGET', 'FILTER', 'area_id=32', conn=r).equal([['a{3}', [], [100, '200']]])

        env.expect('RENAME', 'a{3}', 'a1{3}', conn=r).true()

        env.expect('TS.MGET', 'FILTER', 'area_id=32', conn=r).equal([['a1{3}', [], [100, '200']]])


def test_rename_none_ts(env):
    with env.getClusterConnectionIfNeeded() as r:

        env.expect('TS.CREATE', 'a{4}', conn=r).noError()
        env.expect('SET', 'key1{4}', 'val1', conn=r).noError()
        env.expect('SET', 'key2{4}', 'val2', conn=r).noError()

        env.assertTrue(r.execute_command('RENAME', 'key1{4}', 'key3{4}'))
        env.assertTrue(r.execute_command('RENAME', 'key2{4}', 'key1{4}'))

        env.expect('SET', 'key1{4}', 'val3', conn=r).noError()
        env.expect('SET', 'key3{4}', 'val4', conn=r).noError()

        aInfo = TSInfo(r.execute_command('TS.INFO', 'a{4}'))
        env.assertEqual(aInfo.sourceKey, None)
        env.assertEqual(aInfo.rules, [])
