from test_helper_classes import TSInfo

def test_rename_indexed(env):

    with env.getClusterConnectionIfNeeded() as r:
        
        assert r.execute_command('TS.ADD', 'a{3}', 100, 200, 'LABELS', 'sensor_id', '2', 'area_id', '32')
        env.assertEqual(r.execute_command('TS.MGET', 'FILTER', 'area_id=32'), [[b'a{3}', [], [100, b'200']]])



