import pytest
import redis
import time 
from utils import Env, set_hertz

def test_mget_with_expire_cmd():
    set_hertz(Env())
    with Env().getClusterConnectionIfNeeded() as r:
        # Lower hz value to make it more likely that mget triggers key expiration
        assert r.execute_command("TS.ADD", "X" ,"*" ,"1" ,"LABELS", "type", "DELAYED")
        assert r.execute_command("TS.ADD", "Y" ,"*" ,"1" ,"LABELS", "type", "DELAYED")
        assert r.execute_command("TS.ADD", "Z" ,"*" ,"1" ,"LABELS", "type", "DELAYED")
        current_ts = time.time()
        assert r.execute_command("EXPIRE","X", 5)
        assert r.execute_command("EXPIRE","Y", 6)
        assert r.execute_command("EXPIRE","Z", 7)
        while time.time() < (current_ts+10):
            reply = r.execute_command('TS.MGET', 'FILTER', 'type=DELAYED')
            assert(len(reply)>=0 and len(reply)<=3)
        assert r.execute_command("PING")
        

def test_mget_cmd():
    num_of_keys = 3
    time_stamp = 1511885909
    keys = ['k1', 'k2', 'k3']
    labels = ['a', 'a', 'b']
    values = [100, 200, 300]
    kvlabels = []

    with Env().getClusterConnectionIfNeeded() as r:
        # test for empty series
        assert r.execute_command('TS.CREATE', "key4_empty", "LABELS", "NODATA", "TRUE")
        assert r.execute_command('TS.CREATE', "key5_empty", "LABELS", "NODATA", "TRUE")
        # expect to received time-series k1 and k2
        expected_result = [
            [b"key4_empty", [], []],
            [b"key5_empty", [], []]
        ]

        actual_result = r.execute_command('TS.MGET', 'FILTER', 'NODATA=TRUE')
        assert sorted(expected_result) == sorted(actual_result)

        # test for series with data
        for i in range(num_of_keys):
            assert r.execute_command('TS.CREATE', keys[i], 'LABELS', labels[i], '1')
            kvlabels.append([labels[i].encode('ascii'), '1'.encode('ascii')])

            assert r.execute_command('TS.ADD', keys[i], time_stamp - 1, values[i] - 1)
            assert r.execute_command('TS.ADD', keys[i], time_stamp, values[i])

        # expect to received time-series k1 and k2
        expected_result = [
            [keys[0].encode('ascii'), [], [time_stamp, str(values[0]).encode('ascii')]],
            [keys[1].encode('ascii'), [], [time_stamp, str(values[1]).encode('ascii')]]
        ]

        actual_result = r.execute_command('TS.MGET', 'FILTER', 'a=1')
        assert sorted(expected_result) == sorted(actual_result)

        # expect to received time-series k3 with labels
        expected_result_withlabels = [
            [keys[2].encode('ascii'), [kvlabels[2]], [time_stamp, str(values[2]).encode('ascii')]]
        ]

        actual_result = r.execute_command('TS.MGET', 'WITHLABELS', 'FILTER', 'a!=1', 'b=1')
        assert expected_result_withlabels == actual_result

        # expect to received time-series k1 and k2 with labels
        expected_result_withlabels = [
            [keys[0].encode('ascii'), [kvlabels[0]], [time_stamp, str(values[0]).encode('ascii')]],
            [keys[1].encode('ascii'), [kvlabels[1]], [time_stamp, str(values[1]).encode('ascii')]]
        ]

        actual_result = r.execute_command('TS.MGET', 'WITHLABELS', 'FILTER', 'a=1')
        assert sorted(expected_result_withlabels) == sorted(actual_result)

        # negative test
        assert not r.execute_command('TS.MGET', 'FILTER', 'a=100')
        assert not r.execute_command('TS.MGET', 'FILTER', 'k=1')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.MGET', 'filter')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.MGET', 'filter', 'k+1')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.MGET', 'filter', 'k!=5')
        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('TS.MGET', 'retlif', 'k!=5')
