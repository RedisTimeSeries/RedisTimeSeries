import math
import time

import sys

import pytest
import redis
from RLTest import Env
from test_helper_classes import SAMPLE_SIZE, _get_ts_info

def test_blob():

    with Env().getConnection() as r:

        r.execute_command('ts.create', 'blob1', 'BLOB')

        assert b'blob' == _get_ts_info(r, 'blob1').type
        r.execute_command('ts.add', 'blob2', 1, 'value1', 'BLOB')
        assert b'blob' == _get_ts_info(r, 'blob1').type
        res = r.execute_command('ts.range', 'blob2', '-', '+')

        assert len(res) == 1
        assert res[0][0] == 1
        assert res[0][1] == b'value1'
        r.execute_command('ts.add', 'blob2', 2, 'value2' )
        r.execute_command('ts.add', 'blob2', 3, 'value3' )
        res = r.execute_command('ts.range', 'blob2', '-', '+')

        assert len(res) == 3
        ### upsert ###
        r.execute_command('ts.add', 'blob2', 2, 'new_value2')
        res = r.execute_command('ts.range', 'blob2', '-', '+')

        assert len(res) == 3

        assert res[0][0] == 1
        assert res[0][1] == b'value1'
        assert res[1][0] == 2
        assert res[1][1] == b'new_value2'
        assert res[2][0] == 3
        assert res[2][1] == b'value3'

        ### aggregation count ###
        res = r.execute_command('ts.range', 'blob2', '-', '+', 'AGGREGATION', 'count', 10)

        assert res[0][1] == b'3'

        ### Forbidden commands ###
        forbidden = False;

        try:
            res = r.execute_command('ts.incrby', 'blob2', 1)
        except:
            forbidden = True;

        assert forbidden == True

        forbidden = False;

        try:
	        res = r.execute_command('ts.decrby', 'blob2', 1)
        except:
            forbidden = True;

        assert forbidden == True

	    ### downsampling ###
        res = r.execute_command('ts.create', 'blob3', 'BLOB')
        res = r.execute_command('ts.create', 'blob3_downsample_first', 'BLOB')
        res = r.execute_command('ts.create', 'blob3_downsample_last', 'BLOB')
        res = r.execute_command('ts.create', 'blob3_count', 'BLOB')

        res = r.execute_command('ts.createrule', 'blob3', 'blob3_downsample_last', 'AGGREGATION', 'last', 3)
        res = r.execute_command('ts.createrule', 'blob3', 'blob3_downsample_first', 'AGGREGATION', 'first', 3)

        forbidden = False;

        try:
            res = r.execute_command('ts.createrule', 'blob3', 'blob3_count', 'AGGREGATION', 'count', 3)
        except:
            # must catch 'TSDB: the destination key is of binary type and cannot hold an aggregation count'
            forbidden = True;

        assert forbidden == True

        # re-create it with scalar type
        r.execute_command('del', 'blob3_count')
        res = r.execute_command('ts.create', 'blob3_count')
        res = r.execute_command('ts.createrule', 'blob3', 'blob3_count', 'AGGREGATION', 'count', 3)
        # test save of empty aggregated count

        for i in range(1,10):
            r.execute_command('ts.add', 'blob3', i, 'value'+str(i) )

        res = r.execute_command('ts.range', 'blob3_downsample_last', '-', '+')

        assert res[0][0] == 0
        assert res[0][1] == b'value2'

        assert res[1][0] == 3
        assert res[1][1] == b'value5'

        assert res[2][0] == 6
        assert res[2][1] == b'value8'

        res = r.execute_command('ts.range', 'blob3_downsample_first', '-', '+')

        assert res[0][0] == 0
        assert res[0][1] == b'value1'

        assert res[1][0] == 3
        assert res[1][1] == b'value3'

        assert res[2][0] == 6
        assert res[2][1] == b'value6'

        res = r.execute_command('ts.range', 'blob3_count', '-', '+')
        assert len(res) == 3

        # test save of empty blob
        res = r.execute_command('ts.create', 'empty', 'BLOB')

        # When server is started with slaves, a
        # "Background save already in progress" can happen.

        while True:
            try:
                res = r.execute_command('SAVE')
            except Exception as err:
                print(str(err), " -> retrying")
                time.sleep(1);
            else:
                break;

        r.execute_command('DUMP', 'blob3')
        r.execute_command('del', 'blob3')

        res = r.execute_command('ts.range', 'blob3_downsample_last', '-', '+')
        ### Test after deletion of source (blob must be unchanged) ###

        assert res[0][1] == b'value2'
        assert res[1][1] == b'value5'
        assert res[2][1] == b'value8'
