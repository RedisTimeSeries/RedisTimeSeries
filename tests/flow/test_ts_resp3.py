from includes import *
import time
from utils import set_hertz, is_resp3_possible
from test_helper_classes import TSInfo, ALLOWED_ERROR, _insert_data


def test_resp3(env):
    if not is_resp3_possible(env):
        env.skip()
    t1 = 't1{1}'
    t2 = 't2{1}'
    t3 = 't3{1}'
    t4 = 't4{1}'
    t5 = 't5{1}'
    t6 = 't6{1}'
    samples_count = 1000
    env = Env(protocol=3)
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        r.execute_command('ts.create', t1, 'CHUNK_SIZE', 128, 'LABELS', 'name', 'mush')
        r.execute_command('ts.create', t2, 'CHUNK_SIZE', 128, 'LABELS', 'name', 'mush')
        r.execute_command('ts.createrule', t1, t2, 'AGGREGATION', 'COUNT', 10)
        _insert_data(r, t1, 1, samples_count, 5)
        res = r.execute_command('ts.get', t1)
        assert res == [1000, 5.0]
        res = r.execute_command('ts.info', t1, 'DEBUG')
        assert res == {
            b'totalSamples': 1000,
            b'memoryUsage': 514, b'firstTimestamp': 1, b'lastTimestamp': 1000,
            b'retentionTime': 0, b'chunkCount': 2, b'chunkSize': 128,
            b'chunkType': b'compressed', b'duplicatePolicy': None,
            b'labels': {b'name': b'mush'},
            b'sourceKey': None,
            b'rules': {b't2{1}': [10, b'COUNT', 0]},
            b'keySelfName': b't1{1}',
            b'Chunks':
                [
                    {
                        b'startTimestamp': 1,
                        b'endTimestamp': 510,
                        b'samples': 510,
                        b'size': 128,
                        b'bytesPerSample': 0.250980406999588
                    },
                    {
                        b'startTimestamp': 511,
                        b'endTimestamp': 1000,
                        b'samples': 490,
                        b'size': 128,
                        b'bytesPerSample': 0.2612244784832001
                    }
                ],
            b'ignoreMaxTimeDiff': 0, b'ignoreMaxValDiff': 0.0,
        }
        res = r1.execute_command('ts.mget', 'FILTER', 'name=mush')
        assert res == {b't1{1}': [{}, [1000, 5.0]],
                       b't2{1}': [{}, [990, 10.0]]}

        res = r1.execute_command('ts.mget', 'WITHLABELS', 'FILTER', 'name=mush')
        assert res == {b't1{1}': [{b'name': b'mush'}, [1000, 5.0]],
                    b't2{1}': [{b'name': b'mush'}, [990, 10.0]]}

        res = r1.execute_command('ts.queryindex', 'name=mush')
        assert res == {b't1{1}', b't2{1}'}

def test_resp3_mrange(env):
    if not is_resp3_possible(env):
        env.skip()
    t1 = 't1{1}'
    t2 = 't2{1}'
    t3 = 't3{1}'
    t4 = 't4{1}'
    t5 = 't5{1}'
    t6 = 't6{1}'
    samples_count = 1000
    env = Env(protocol=3)
    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        r.execute_command('ts.create', t1, 'CHUNK_SIZE', 128, 'LABELS', 'name', 'mush')
        r.execute_command('ts.create', t2, 'CHUNK_SIZE', 128, 'LABELS', 'name', 'mush')
        r.execute_command('ts.createrule', t1, t2, 'AGGREGATION', 'COUNT', 10)
        _insert_data(r, t1, 1, samples_count, 5)

        r.execute_command('ts.create', t3, 'CHUNK_SIZE', 128, 'LABELS', 'name', 'bush')
        r.execute_command('ts.create', t4, 'CHUNK_SIZE', 128, 'LABELS', 'name', 'bush')
        r.execute_command('ts.create', t5, 'CHUNK_SIZE', 128, 'LABELS', 'name', 'rush')
        r.execute_command('ts.create', t6, 'CHUNK_SIZE', 128, 'LABELS', 'name', 'rush')

        _insert_data(r, t3, 1, 10, 2)
        _insert_data(r, t4, 1, 10, 6)
        _insert_data(r, t5, 1, 10, 2)
        _insert_data(r, t6, 1, 10, 6)

        exp = {b'name=bush':
            [{b'name': b'bush'}, {b'reducers': [b'count']}, {b'sources': [b't3{1}', b't4{1}']},
                [[1, 2.0], [2, 2.0], [3, 2.0], [4, 2.0], [5, 2.0], [6, 2.0], [7, 2.0], [8, 2.0], [9, 2.0], [10, 2.0]]],
                b'name=rush':
                [{b'name': b'rush'}, {b'reducers': [b'count']}, {b'sources': [b't5{1}', b't6{1}']},
                [[1, 2.0], [2, 2.0], [3, 2.0], [4, 2.0], [5, 2.0], [6, 2.0], [7, 2.0], [8, 2.0], [9, 2.0], [10, 2.0]]]}
        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'name=(rush,bush)', 'GROUPBY', 'name', 'REDUCE', 'COUNT')
        env.assertEqual(actual_result, exp)

        exp = {b'name=bush':
            [{b'name': b'bush'}, {b'reducers': [b'count']}, {b'sources': [b't3{1}', b't4{1}']},
                [[1, 2.0], [2, 2.0], [3, 2.0], [4, 2.0], [5, 2.0], [6, 2.0], [7, 2.0], [8, 2.0], [9, 2.0], [10, 2.0]]],
                b'name=rush':
                [{b'name': b'rush'}, {b'reducers': [b'count']}, {b'sources': [b't5{1}', b't6{1}']},
                [[1, 2.0], [2, 2.0], [3, 2.0], [4, 2.0], [5, 2.0], [6, 2.0], [7, 2.0], [8, 2.0], [9, 2.0], [10, 2.0]]]}
        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'SELECTED_LABELS', 'name', 'FILTER', 'name=(rush,bush)', 'GROUPBY', 'name', 'REDUCE', 'COUNT')
        env.assertEqual(actual_result, exp)

        exp = {b'name=bush':
            [{b'fame': None}, {b'reducers': [b'count']}, {b'sources': [b't3{1}', b't4{1}']},
                [[1, 2.0], [2, 2.0], [3, 2.0], [4, 2.0], [5, 2.0], [6, 2.0], [7, 2.0], [8, 2.0], [9, 2.0], [10, 2.0]]],
                b'name=rush':
                [{b'fame': None}, {b'reducers': [b'count']}, {b'sources': [b't5{1}', b't6{1}']},
                [[1, 2.0], [2, 2.0], [3, 2.0], [4, 2.0], [5, 2.0], [6, 2.0], [7, 2.0], [8, 2.0], [9, 2.0], [10, 2.0]]]}
        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'SELECTED_LABELS', 'fame', 'FILTER', 'name=(rush,bush)', 'GROUPBY', 'name', 'REDUCE', 'COUNT')
        env.assertEqual(actual_result, exp)

        exp = {
            b't3{1}':
            [{b'name': b'bush'}, {b'aggregators': []},
            [[1, 2.0], [2, 2.0], [3, 2.0], [4, 2.0], [5, 2.0], [6, 2.0], [7, 2.0], [8, 2.0], [9, 2.0], [10, 2.0]]],
            b't4{1}':
            [{b'name': b'bush'}, {b'aggregators': []},
            [[1, 6.0], [2, 6.0], [3, 6.0], [4, 6.0], [5, 6.0], [6, 6.0], [7, 6.0], [8, 6.0], [9, 6.0], [10, 6.0]]],
            b't5{1}':
            [{b'name': b'rush'}, {b'aggregators': []},
            [[1, 2.0], [2, 2.0], [3, 2.0], [4, 2.0], [5, 2.0], [6, 2.0], [7, 2.0], [8, 2.0], [9, 2.0], [10, 2.0]]],
            b't6{1}':
            [{b'name': b'rush'}, {b'aggregators': []},
                [[1, 6.0], [2, 6.0], [3, 6.0], [4, 6.0], [5, 6.0], [6, 6.0], [7, 6.0], [8, 6.0], [9, 6.0], [10, 6.0]]]}
        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'WITHLABELS', 'FILTER', 'name=(rush,bush)')
        env.assertEqual(actual_result, exp)

        exp = {
            b't3{1}':
            [{b'name': b'bush'}, {b'aggregators': []},
            [[1, 2.0], [2, 2.0], [3, 2.0], [4, 2.0], [5, 2.0], [6, 2.0], [7, 2.0], [8, 2.0], [9, 2.0], [10, 2.0]]],
            b't4{1}':
            [{b'name': b'bush'}, {b'aggregators': []},
            [[1, 6.0], [2, 6.0], [3, 6.0], [4, 6.0], [5, 6.0], [6, 6.0], [7, 6.0], [8, 6.0], [9, 6.0], [10, 6.0]]],
            b't5{1}':
            [{b'name': b'rush'}, {b'aggregators': []},
            [[1, 2.0], [2, 2.0], [3, 2.0], [4, 2.0], [5, 2.0], [6, 2.0], [7, 2.0], [8, 2.0], [9, 2.0], [10, 2.0]]],
            b't6{1}':
            [{b'name': b'rush'}, {b'aggregators': []},
                [[1, 6.0], [2, 6.0], [3, 6.0], [4, 6.0], [5, 6.0], [6, 6.0], [7, 6.0], [8, 6.0], [9, 6.0], [10, 6.0]]]}
        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'SELECTED_LABELS', 'name', 'FILTER', 'name=(rush,bush)')
        env.assertEqual(actual_result, exp)

        exp = {
            b't3{1}': [{b'fame': None}, {b'aggregators': []},
            [[1, 2.0], [2, 2.0], [3, 2.0], [4, 2.0], [5, 2.0], [6, 2.0], [7, 2.0], [8, 2.0], [9, 2.0], [10, 2.0]]],
            b't4{1}':
            [{b'fame': None}, {b'aggregators': []},
            [[1, 6.0], [2, 6.0], [3, 6.0], [4, 6.0], [5, 6.0], [6, 6.0], [7, 6.0], [8, 6.0], [9, 6.0], [10, 6.0]]],
            b't5{1}':
            [{b'fame': None}, {b'aggregators': []},
            [[1, 2.0], [2, 2.0], [3, 2.0], [4, 2.0], [5, 2.0], [6, 2.0], [7, 2.0], [8, 2.0], [9, 2.0], [10, 2.0]]],
            b't6{1}':
            [{b'fame': None}, {b'aggregators': []},
                [[1, 6.0], [2, 6.0], [3, 6.0], [4, 6.0], [5, 6.0], [6, 6.0], [7, 6.0], [8, 6.0], [9, 6.0], [10, 6.0]]]}
        actual_result = r1.execute_command(
            'TS.mrange', '-', '+', 'SELECTED_LABELS', 'fame', 'FILTER', 'name=(rush,bush)')
        env.assertEqual(actual_result, exp)
