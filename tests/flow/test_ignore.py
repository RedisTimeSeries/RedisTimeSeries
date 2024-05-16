
from RLTest import Env
from test_helper_classes import _get_ts_info
from includes import *


def test_ignore_invalid_params():
    with Env().getClusterConnectionIfNeeded() as r:
        # maxtimediff and maxvaldiff must be given together
        with pytest.raises(redis.ResponseError):
            r.execute_command('ts.create', 'key1', 'ignore')

        with pytest.raises(redis.ResponseError):
            r.execute_command('ts.create', 'key1', 'ignore', 'labels', 'label1')
        with pytest.raises(redis.ResponseError):
            r.execute_command('ts.create', 'key1', 'ignore', '3')
        with pytest.raises(redis.ResponseError):
            r.execute_command('ts.create', 'key1', 'ignore', '3', 'labels', 'label')

        # invalid maxtimediff
        with pytest.raises(redis.ResponseError):
            r.execute_command('ts.create', 'key1', 'ignore', '3.2', '5')
        with pytest.raises(redis.ResponseError):
            r.execute_command('ts.add', 'key1', 'ignore', '3.2', '5')
        with pytest.raises(redis.ResponseError):
            r.execute_command('ts.create', 'key1', 'ignore', 'invalid', '5')
        with pytest.raises(redis.ResponseError):
            r.execute_command('ts.add', 'key1', 'ignore', 'invalid', '5')
        with pytest.raises(redis.ResponseError):
            r.execute_command('ts.create', 'key1', 'ignore', '-3', '5')
        with pytest.raises(redis.ResponseError):
            r.execute_command('ts.add', 'key1', 'ignore', '-3', '5')

        # invalid maxvaldiff
        with pytest.raises(redis.ResponseError):
            r.execute_command('ts.create', 'key1', 'ignore', '3', 'invalid')
        with pytest.raises(redis.ResponseError):
            r.execute_command('ts.add', 'key1', 'ignore', '3', 'invalid')
        with pytest.raises(redis.ResponseError):
            r.execute_command('ts.create', 'key1', 'ignore', '3', '-5')
        with pytest.raises(redis.ResponseError):
            r.execute_command('ts.add', 'key1', 'ignore', '3', '-5')


def test_ignore_create():
    with Env().getClusterConnectionIfNeeded() as r:
        # Verify [0, 0] can be added when insert filter is not configured
        r.execute_command('TS.ADD', 'key0', '0', '0')
        assert r.execute_command('TS.range', 'key0', 0, '+') == [[0, b'0']]

        # Create key with ts.create
        r.execute_command('TS.CREATE', 'key1', 'IGNORE', '5', '5', 'DUPLICATE_POLICY', 'LAST')
        r.execute_command('TS.ADD', 'key1', '1000', '1')
        r.execute_command('TS.ADD', 'key1', '1001', '2')
        r.execute_command('TS.ADD', 'key1', '1006', '3')
        r.execute_command('TS.ADD', 'key1', '1007', '8')
        r.execute_command('TS.ADD', 'key1', '1008', '10')
        r.execute_command('TS.ADD', 'key1', '1009', '15.0001')

        expected = [[1000, b'1'], [1006, b'3'], [1008, b'10'], [1009, b'15.0001']]
        actual = r.execute_command('TS.range', 'key1', 0, '+')
        assert actual == expected

        # Create key with ts.add
        r.execute_command('TS.ADD', 'key2', '1000', '1', 'IGNORE', '5', '5', 'DUPLICATE_POLICY', 'LAST')
        r.execute_command('TS.ADD', 'key2', '1001', '2')
        r.execute_command('TS.ADD', 'key2', '1006', '3')
        r.execute_command('TS.ADD', 'key2', '1007', '8')
        r.execute_command('TS.ADD', 'key2', '1008', '10')
        r.execute_command('TS.ADD', 'key2', '1009', '21001.0000002')

        expected = [[1000, b'1'], [1006, b'3'], [1008, b'10'], [1009, b'21001.0000002']]
        actual = r.execute_command('TS.range', 'key2', 0, '+')
        assert actual == expected

        assert r.execute_command('TS.ADD', 'key2', '1010', '21003') == 1009
        assert r.execute_command('TS.ADD', 'key2', '1020', '22000') == 1020
        assert r.execute_command('TS.ADD', 'key2', '1022', '21998') == 1020
        assert r.execute_command('TS.ADD', 'key2', '1023', '21994') == 1023

        expected = [[1000, b'1'], [1006, b'3'], [1008, b'10'], [1009, b'21001.0000002'], [1020, b'22000'], [1023, b'21994']]
        actual = r.execute_command('TS.range', 'key2', 0, '+')
        assert actual == expected

        # Create key with incrby/decrby
        r.execute_command('TS.INCRBY', 'key3', '1', 'TIMESTAMP', '100', 'IGNORE', '100000', '5')
        # Value diff is smaller than the limit, this sample will be ignored.
        r.execute_command('TS.INCRBY', 'key3', '3', 'TIMESTAMP', '100')
        assert r.execute_command('TS.range', 'key3', 0, '+') == [[100, b'1']]
        # Value diff is larger than the limit.
        r.execute_command('TS.INCRBY', 'key3', '7', 'TIMESTAMP', '100')
        assert r.execute_command('TS.range', 'key3', 0, '+') == [[100, b'8']]

        r.execute_command('TS.DECRBY', 'key4', '1', 'TIMESTAMP', '100', 'IGNORE', '100000', '5')
        # Value diff is smaller than the limit, this sample will be ignored.
        r.execute_command('TS.DECRBY', 'key4', '3', 'TIMESTAMP', '100')
        assert r.execute_command('TS.range', 'key4', 0, '+') == [[100, b'-1']]
        # Value diff is larger than the limit.
        r.execute_command('TS.DECRBY', 'key4', '7', 'TIMESTAMP', '100')
        assert r.execute_command('TS.range', 'key4', 0, '+') == [[100, b'-8']]


def test_ignore_duplicate_policy():
    with Env().getClusterConnectionIfNeeded() as r:
        # Filter should not work without DUPLICATE_POLICY LAST
        r.execute_command('TS.CREATE', 'key1', 'IGNORE', '5', '5')
        r.execute_command('TS.ADD', 'key1', '1000', '1')
        r.execute_command('TS.ADD', 'key1', '1001', '2')
        r.execute_command('TS.ADD', 'key1', '1006', '3')
        r.execute_command('TS.ADD', 'key1', '1007', '8')
        r.execute_command('TS.ADD', 'key1', '1008', '10')
        r.execute_command('TS.ADD', 'key1', '1009', '15.0001')

        expected = [[1000, b'1'], [1001, b'2'], [1006, b'3'], [1007, b'8'],
                    [1008, b'10'], [1009, b'15.0001']]
        actual = r.execute_command('TS.range', 'key1', 0, '+')
        assert actual == expected

        # ON_DUPLICATE LAST will override key config. Sample will be ignored.
        r.execute_command('TS.ADD', 'key1', '1010', '16.0', 'ON_DUPLICATE', 'LAST')
        actual = r.execute_command('TS.range', 'key1', 0, '+')
        assert actual == expected


def test_ignore_madd():
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', '{tag}key1', 'IGNORE', '5', '5', 'DUPLICATE_POLICY', 'LAST')
        r.execute_command('TS.CREATE', '{tag}key2')
        r.execute_command('TS.MADD', '{tag}key1', '1000', '1', '{tag}key2', '1000', '1')
        r.execute_command('TS.MADD', '{tag}key1', '1001', '2', '{tag}key2', '1001', '2')
        r.execute_command('TS.MADD', '{tag}key1', '1006', '3', '{tag}key2', '1006', '3')
        r.execute_command('TS.MADD', '{tag}key1', '1007', '8', '{tag}key2', '1007', '8')
        r.execute_command('TS.MADD', '{tag}key1', '1008', '10', '{tag}key2', '1008', '10')
        r.execute_command('TS.MADD', '{tag}key1', '1009', '15.0001', '{tag}key2', '1009', '15.0001')

        expected = [[1000, b'1'], [1006, b'3'], [1008, b'10'], [1009, b'15.0001']]
        actual = r.execute_command('TS.range', '{tag}key1', 0, '+')
        assert actual == expected

        expected = [[1000, b'1'], [1001, b'2'], [1006, b'3'], [1007, b'8'],
                    [1008, b'10'], [1009, b'15.0001']]
        actual = r.execute_command('TS.range', '{tag}key2', 0, '+')
        assert actual == expected

        # If sample is ignored, return value is max timestamp of the series
        assert r.execute_command('TS.MADD', '{tag}key1', '1010', '15.0001', '{tag}key2', '1010', '16.0') == [1009, 1010]
        assert r.execute_command('TS.ADD', '{tag}key1', '1012', '16') == 1009


def test_ignore_restore():
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'key1', 'IGNORE', '3', '5',
                          'DUPLICATE_POLICY', 'LAST')
        r.execute_command('TS.ADD', 'key1', '1000', '1')
        r.execute_command('TS.ADD', 'key1', '1001', '2')
        r.execute_command('TS.ADD', 'key1', '1004', '3')
        r.execute_command('TS.ADD', 'key1', '1005', '8')
        r.execute_command('TS.ADD', 'key1', '1006', '10')
        r.execute_command('TS.ADD', 'key1', '1007', '15.0001')

        expected = [[1000, b'1'], [1004, b'3'], [1006, b'10'], [1007, b'15.0001']]
        actual = r.execute_command('TS.range', 'key1', 0, '+')
        assert actual == expected

        r.execute_command('TS.ADD', 'key1', '1010', '16.0')
        actual = r.execute_command('TS.range', 'key1', 0, '+')
        assert actual == expected

        # Verify restore works as expected
        dump = r.execute_command('dump', 'key1')
        assert r.execute_command('del', 'key1') == 1
        assert r.execute_command('restore', 'key1', 0, dump) == b'OK'

        r.execute_command('TS.ADD', 'key1', '1010', '16.0')
        actual = r.execute_command('TS.range', 'key1', 0, '+')
        assert actual == expected

        info = _get_ts_info(r, 'key1')
        assert info.ignore_max_time_diff == 3
        assert info.ignore_max_val_diff == b'5'


def test_ignore_rdb_load():
    env = Env()
    env.skipOnCluster()
    env.skipOnAOF()
    env.skipOnSlave()
    skip_on_rlec()

    r = env.getClusterConnectionIfNeeded()
    assert r.execute_command('TS.CREATE', 'key1', 'RETENTION', '1000', 'CHUNK_SIZE', '1024',
                             'ENCODING', 'UNCOMPRESSED', 'DUPLICATE_POLICY', 'LAST', 'IGNORE', '5', '10',
                             'LABELS', 'name',
                             'brown', 'color', 'pink')
    r.execute_command('TS.ADD', 'key1', 100, 99)
    r.execute_command('TS.ADD', 'key1', 110, 500.5)
    r.execute_command('TS.ADD', 'key1', 113, 502)
    assert r.execute_command('TS.range', 'key1', 0, '+') == [[100, b'99'], [110, b'500.5']]
    info = _get_ts_info(r, 'key1')

    env.stop()
    env.start()
    info2 = _get_ts_info(r, 'key1')

    assert info == info2
    r.execute_command('TS.ADD', 'key1', 113, 502)
    assert r.execute_command('TS.range', 'key1', 0, '+') == [[100, b'99'], [110, b'500.5']]


def test_ignore_alter():
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'key1', 'IGNORE', '3', '5', 'DUPLICATE_POLICY', 'LAST')
        r.execute_command('TS.ADD', 'key1', '1000', '1')
        r.execute_command('TS.ADD', 'key1', '1001', '2')
        r.execute_command('TS.ADD', 'key1', '1004', '3')
        r.execute_command('TS.ADD', 'key1', '1005', '8')
        r.execute_command('TS.ADD', 'key1', '1006', '10')
        r.execute_command('TS.ADD', 'key1', '1007', '15.0001')
        r.execute_command('TS.ADD', 'key1', '1010', '19')

        expected = [[1000, b'1'], [1004, b'3'], [1006, b'10'], [1007, b'15.0001']]
        actual = r.execute_command('TS.range', 'key1', 0, '+')
        assert actual == expected

        assert r.execute_command('TS.ALTER', 'key1', 'IGNORE', '2', '4') == b'OK'
        info = _get_ts_info(r, 'key1')
        assert info.ignore_max_time_diff == 2
        assert info.ignore_max_val_diff == b'4'

        # Verify new ignore parameters
        r.execute_command('TS.ADD', 'key1', '1010', '19')
        expected = [[1000, b'1'], [1004, b'3'], [1006, b'10'], [1007, b'15.0001'], [1010, b'19']]
        actual = r.execute_command('TS.range', 'key1', 0, '+')
        assert actual == expected
