import os
import redis
import pytest
import time
import math
import random
import statistics
from rmtest import ModuleTestCase

from create_test_rdb_file import load_into_redis

if os.environ['REDISTIMESERIES'] != '':
    REDISTIMESERIES = os.path.realpath(os.environ['REDISTIMESERIES'])
else:
    REDISTIMESERIES = os.path.dirname(os.path.abspath(__file__)) + '/redistimeseries.so'


class RedisTimeseriesRDBTests(ModuleTestCase(REDISTIMESERIES)):
    def test_rdbs(self):
        with self.redis() as current_r:
            load_into_redis(current_r)

            for rdb_file in os.listdir('rdbs'):
                with self.redis(dir=os.path.realpath('rdbs'), dbfilename=rdb_file) as r:
                    for key in current_r.keys():
                        assert r.execute_command('ts.range', key, "-", "+") == \
                               current_r.execute_command('ts.range', key, "-", "+"), "data in key '{}' is not correct (rdb {})".format(key, rdb_file)
                        loaded_info = r.execute_command('ts.info', key)
                        current_info = current_r.execute_command('ts.info', key)

                        assert self.normalize_info(loaded_info) == self.normalize_info(current_info), \
                            "info for key '{}' is not correct (rdb {})".format(key, rdb_file)

    def normalize_info(self, data):
        info = {}
        for i in range(0, len(data), 2):
            info[data[i]] = data[i + 1]
        info.pop('memoryUsage')
        info.pop('defaultChunkSizeBytes')
        return info
