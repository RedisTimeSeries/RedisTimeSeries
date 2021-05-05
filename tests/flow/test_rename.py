from test_helper_classes import TSInfo

def test_rename_src(env):

    assert env.cmd('TS.CREATE', 'a1')
    assert env.cmd('TS.CREATE', 'b')

    env.assertTrue(env.cmd('RENAME', 'a1', 'a2'))
    aInfo = TSInfo(env.cmd('TS.INFO', b'a2'))
    env.assertEqual(aInfo.sourceKey, None)
    env.assertEqual(aInfo.rules, [])
    
    assert env.cmd('TS.CREATERULE', 'a2', 'b', 'AGGREGATION', 'AVG', 5000)
    bInfo = TSInfo(env.cmd('TS.INFO', 'b'))
    env.assertEqual(bInfo.sourceKey, b'a2')
    env.assertEqual(bInfo.rules, [])

    env.assertTrue(env.cmd('RENAME', 'a2', 'a3'))
    bInfo = TSInfo(env.cmd('TS.INFO', 'b'))
    env.assertEqual(bInfo.sourceKey, b'a3')
    env.assertEqual(bInfo.rules, [])


def test_rename_dst(env):

    assert env.cmd('TS.CREATE', 'a')
    assert env.cmd('TS.CREATE', 'b')
    assert env.cmd('TS.CREATERULE', 'a', 'b', 'AGGREGATION', 'AVG', 5000)

    env.assertTrue(env.cmd('RENAME', 'b', 'b1'))
    aInfo = TSInfo(env.cmd('TS.INFO', 'a'))
    env.assertEqual(aInfo.sourceKey, None)
    env.assertEqual(aInfo.rules[0][0], b'b1')

    assert env.cmd('TS.CREATE', 'c')
    assert env.cmd('TS.CREATERULE', 'a', 'c', 'AGGREGATION', 'COUNT', 2000)

    assert env.cmd('TS.CREATE', 'd')
    assert env.cmd('TS.CREATERULE', 'a', 'd', 'AGGREGATION', 'SUM', 3000)

    env.assertTrue(env.cmd('RENAME', 'c', 'c1'))
    aInfo = TSInfo(env.cmd('TS.INFO', 'a'))
    env.assertEqual(aInfo.sourceKey, None)
    env.assertEqual(aInfo.rules[0][0], b'b1')
    env.assertEqual(aInfo.rules[1][0], b'c1')
    env.assertEqual(aInfo.rules[2][0], b'd')




