from test_helper_classes import TSInfo

def test_rename_src(env):

    env.assertOk(env.cmd('TS.CREATE', 'a1'))
    env.assertOk(env.cmd('TS.CREATE', 'b'))

    env.assertTrue( env.cmd('RENAME', 'a1', 'a2'))
    aInfo = TSInfo(env.cmd('TS.INFO', 'a2'))
    env.assertEqual(aInfo.sourceKey, None)
    env.assertEqual(aInfo.rules, [])
    
    env.assertOk(env.cmd('TS.CREATERULE', 'a2', 'b', 'AGGREGATION', 'AVG', 5000))
    bInfo = TSInfo(env.cmd('TS.INFO', 'b'))
    env.assertEqual(bInfo.sourceKey, 'a2')
    env.assertEqual(bInfo.rules, [])

    env.assertTrue(env.cmd('RENAME', 'a2', 'a3'))
    bInfo = TSInfo(env.cmd('TS.INFO', 'b'))
    env.assertEqual(bInfo.sourceKey, 'a3')
    env.assertEqual(bInfo.rules, [])


def test_rename_dst(env):

    env.assertOk(env.cmd('TS.CREATE', 'a'))
    env.assertOk(env.cmd('TS.CREATE', 'b'))
    env.assertOk(env.cmd('TS.CREATERULE', 'a', 'b', 'AGGREGATION', 'AVG', 5000))

    env.assertTrue(env.cmd('RENAME', 'b', 'b1'))
    aInfo = TSInfo(env.cmd('TS.INFO', 'a'))
    env.assertEqual(aInfo.sourceKey, None)
    env.assertEqual(aInfo.rules[0][0], 'b1')

    env.assertOk(env.cmd('TS.CREATE', 'c'))
    env.assertOk(env.cmd('TS.CREATERULE', 'a', 'c', 'AGGREGATION', 'COUNT', 2000))

    env.assertOk(env.cmd('TS.CREATE', 'd'))
    env.assertOk(env.cmd('TS.CREATERULE', 'a', 'd', 'AGGREGATION', 'SUM', 3000))

    env.assertTrue(env.cmd('RENAME', 'c', 'c1'))
    aInfo = TSInfo(env.cmd('TS.INFO', 'a'))
    env.assertEqual(aInfo.sourceKey, None)
    env.assertEqual(aInfo.rules[0][0], 'b1')
    env.assertEqual(aInfo.rules[1][0], 'c1')
    env.assertEqual(aInfo.rules[2][0], 'd')




