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

chars = digits + ascii_lowercase
L = ["".join([choice(chars) for i in range(10)]) for j in range(10)]

def create_labels(list_names, lists):
  labels = []
  rand = int(random.random() * len(lists[0]))
  labels.extend([list_names[0], lists[0][rand]])  
  rand = int(random.random() * len(lists[1]))
  labels.extend([list_names[1], lists[1][rand]])  
  rand = int(random.random() * len(lists[2]))
  #if rand % 9 < 4: 
  labels.extend([list_names[2], lists[2][rand]])
  rand = int(random.random() * len(lists[3]))
  labels.extend([list_names[3], lists[3][rand]])
  rand = int(random.random() * len(lists[4]))
  labels.extend([list_names[4], lists[4][rand]])
  rand = int(random.random() * len(lists[5]))
  #if rand % 13 < 4: 
  labels.extend([list_names[5], lists[5][rand]])
  rand = int(random.random() * len(lists[6]))
  labels.extend([list_names[6], lists[6][rand]])
  #print len(labels) / 2, labels
  return labels

class RedisTimeseriesTests(ModuleTestCase(os.path.dirname(os.path.abspath(__file__)) + '/../bin/redistimeseries.so')):
  def a_test_benchmark(self): #remove 'a_' to run
    start_ts = 10L
    series_count =  12
  
    samples_count = 1500
    name = 0
    start_time = time.time()

    with self.redis() as r:
      r.execute_command('FLUSHALL')
      for i in range(series_count):
        for j in range(series_count):
          for k in range(series_count):
            for l in range(series_count):
              for m in range(series_count):
#                for n in range(series_count):
                  r.execute_command('TS.CREATE', name, 'LABELS', 'a', i, 'b', j, 'c', k, 'd', l, 'e', m)#, 'f', n)
                  name += 1      
      print
      print("--- %s seconds ---" % (time.time() - start_time))
      start_time = time.time()

      result = r.execute_command('TS.QUERYINDEX', 'a=0,1,2,3,4,5')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'a=0,1,2,3,4,5', 'b!=(1,2,3,4)')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'a=8')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'c=5')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'b=(1,4)', 'c=(2,3)', 'd!=(4,5)', 'e=(1,2,3)')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'b=(1,4)', 'c=(2,3)', 'd!=(4,5)', 'e=(1,2,3)')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'a=1', 'b=3', 'f=5', 'd=2')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'b=3', 'c!=4', 'e!=5')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'b=3', 'c!=(1,2,6)', 'e!=5')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'b=3', 'c=(1,2,5)', 'e!=5')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
  
  def test_benchmark_random(self):
    # Create random labels
    random.seed(0)
    chars = digits + ascii_lowercase
    l5_10 = ["".join([choice(chars) for i in range(5)]) for j in range(10)]
    l5_20 = ["".join([choice(chars) for i in range(5)]) for j in range(20)]
    l5_100 = ["".join([choice(chars) for i in range(5)]) for j in range(100)]
    l10_5 = ["".join([choice(chars) for i in range(10)]) for j in range(5)]
    l10_20 = ["".join([choice(chars) for i in range(10)]) for j in range(20)]
    l10_100 = ["".join([choice(chars) for i in range(10)]) for j in range(100)]
    l20_20 = ["".join([choice(chars) for i in range(20)]) for j in range(20)]

    names_list =['5_10', '5_20', '5_100', '10_5', '10_20', '10_100', '20_20']
    lists_list =[l5_10, l5_20, l5_100, l10_5, l10_20, l10_100, l20_20]
    start_ts = 10L
    series_count =  1000000
    name = 0
    start_time = time.time()

    with self.redis() as r:
      r.execute_command('FLUSHALL')
      for i in range(series_count):
        labels_list = create_labels(names_list, lists_list)
        chars = digits + ascii_lowercase
        keyname = "".join([choice(chars) for j in range(15)])
        #print keyname
        r.execute_command('TS.CREATE', keyname, 'LABELS', *labels_list)

      print "\n\nCreation of "+str(series_count)+ " time series ended after"
      print("--- %s seconds ---" % (time.time() - start_time))
      label = []
      ###########
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', names_list[0]+'='+lists_list[0][0])
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      ###########
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', names_list[1]+'='+lists_list[1][0])
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      ###########
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', names_list[2]+'='+lists_list[2][0])
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      ###########
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', names_list[3]+'='+lists_list[3][2])
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      ###########
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', names_list[4]+'='+lists_list[4][2])
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      ###########
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', names_list[5]+'='+lists_list[5][2])
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      ###########
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', names_list[6]+'='+lists_list[6][2])
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      ###########
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', names_list[2]+'!=('+lists_list[2][2]+','+lists_list[2][7]+','+lists_list[2][20]+')',
                                                  names_list[1]+'=('+lists_list[1][2]+','+lists_list[1][7]+','+lists_list[1][18]+')',
                                                  names_list[4]+'!=('+lists_list[4][2]+','+lists_list[4][7]+','+lists_list[4][18]+')',
                                                  names_list[6]+'!=('+lists_list[6][2]+')')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      ###########
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', names_list[0]+'=('+lists_list[0][2]+','+lists_list[0][4]+','+lists_list[0][0]+')',
                                                  names_list[1]+'=('+lists_list[1][2]+','+lists_list[1][7]+','+lists_list[1][18]+')',
                                              #   names_list[4]+'!=('+lists_list[4][2]+','+lists_list[4][7]+','+lists_list[4][18]+')',
                                                  names_list[6]+'!=('+lists_list[6][2]+')')    
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      ###########
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', names_list[5]+'=('+lists_list[5][2]+','+lists_list[5][7]+','+lists_list[1][18]+')')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))      
      ###########
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', names_list[5]+'!=('+lists_list[5][2]+','+lists_list[5][7]+','+lists_list[1][18]+')',
                                                  names_list[0]+'=('+lists_list[0][2]+','+lists_list[0][4]+','+lists_list[0][0]+')')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      
      '''
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'b=(1,4)', 'c=(2,3)', 'd!=(4,5)', 'e=(1,2,3)')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'b=(1,4)', 'c=(2,3)', 'd!=(4,5)', 'e=(1,2,3)')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'a=1', 'b=3', 'f=5', 'd=2')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'b=3', 'c!=4', 'e!=5')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'b=3', 'c!=(1,2,6)', 'e!=5')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'b=3', 'c=(1,2,5)', 'e!=5')
      print len(result)
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      '''

