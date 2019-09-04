import os
import redis
import pytest
import time
import __builtin__
import math
import random
import statistics 
from rmtest import ModuleTestCase
from random import choice
from string import digits, ascii_lowercase

class RedisTimeseriesTests(ModuleTestCase(os.path.dirname(os.path.abspath(__file__)) + '/../bin/redistimeseries.so')):
  def test_no_labels(self):
    print
    iterations = 1000000
    with self.redis() as r:
      r.execute_command('FLUSHALL')
      with r.pipeline(transaction=False) as p:
        start_time = time.time()
        for i in range(iterations):
          p.execute_command('TS.CREATE', i)
        print("--- \nfilling pipe after %s seconds ---" % (time.time() - start_time))
        start_time = time.time()
        p.execute()
        print("--- creating after %s seconds ---" % (time.time() - start_time))
        for timestamp in range(100, 120, 2):
          
          start_time = time.time()
          for i in range(iterations):
            p.execute_command('TS.ADD', i, timestamp, 1)
          print("--- \nfilling pipe after %s seconds ---" % (time.time() - start_time))
          start_time = time.time()
          p.execute()
          print("--- TS.ADD after %s seconds ---" % (time.time() - start_time))

          timestamp += 1
          start_time = time.time()
          for i in range(iterations/10):
            p.execute_command('TS.MADD', i * 10 + 0, timestamp, 1,
                                        i * 10 + 1, timestamp, 1,
                                        i * 10 + 2, timestamp, 1,
                                        i * 10 + 3, timestamp, 1,
                                        i * 10 + 4, timestamp, 1,
                                        i * 10 + 5, timestamp, 1,
                                        i * 10 + 6, timestamp, 1,
                                        i * 10 + 7, timestamp, 1,
                                        i * 10 + 8, timestamp, 1,
                                        i * 10 + 9, timestamp, 1)
          print("--- \nfilling pipe after %s seconds ---" % (time.time() - start_time))
          start_time = time.time()
          p.execute()
          print("--- TS.MADD after %s seconds ---" % (time.time() - start_time))
          
        '''
        start_time = time.time()
        for i in range(iterations):
          p.execute_command('TS.ADD', i, '*', 1)
        print("--- \nfilling pipe after %s seconds ---" % (time.time() - start_time))
        start_time = time.time()
        p.execute()
        print("--- adding with timestamp '*' after %s seconds ---" % (time.time() - start_time))
        
        timestamp = '*'
        start_time = time.time()
        for i in range(iterations/10):
          p.execute_command('TS.MADD', i * 10 + 0, timestamp, 1,
                                       i * 10 + 1, timestamp, 1,
                                       i * 10 + 2, timestamp, 1,
                                       i * 10 + 3, timestamp, 1,
                                       i * 10 + 4, timestamp, 1,
                                       i * 10 + 5, timestamp, 1,
                                       i * 10 + 6, timestamp, 1,
                                       i * 10 + 7, timestamp, 1,
                                       i * 10 + 8, timestamp, 1,
                                       i * 10 + 9, timestamp, 1)
        print("--- \nfilling pipe after %s seconds ---" % (time.time() - start_time))
        start_time = time.time()
        p.execute()
        print("--- mass adding with timestamp '*' after %s seconds ---" % (time.time() - start_time))
        '''