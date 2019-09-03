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
  labels.extend([list_names[2], lists[2][rand]])
  rand = int(random.random() * len(lists[3]))
  labels.extend([list_names[3], lists[3][rand]])
  rand = int(random.random() * len(lists[4]))
  labels.extend([list_names[4], lists[4][rand]])
  rand = int(random.random() * len(lists[5]))
  labels.extend([list_names[5], lists[5][rand]])
  rand = int(random.random() * len(lists[6]))
  labels.extend([list_names[6], lists[6][rand]])
  return labels

def simple_query_func(self, samples_count):
  with self.redis() as r:
    print "\nTest creation and query of "+str(samples_count)+" series"
    r.execute_command('FLUSHALL')
    with r.pipeline(transaction=False) as p:
      start_time = time.time()
      for name in range(samples_count):
        p.execute_command('TS.CREATE', name, 'LABELS', 'number', name)
      p.execute()
      print("--- %s seconds to create---" % (time.time() - start_time))
      start_time = time.time()
      for name in range(samples_count):
        result = p.execute_command('TS.QUERYINDEX', 'number=' + str(name))
      response = p.execute()
      print("--- %s seconds to query index---" % (time.time() - start_time))
      for name in range(samples_count):
        assert response[name] == [str(name)]

class RedisTimeseriesTests(ModuleTestCase(os.path.dirname(os.path.abspath(__file__)) + '/../bin/redistimeseries.so')):
  def test_multiple_simple(self):
    print
    simple_query_func(self, 100)
    simple_query_func(self, 1000)
    simple_query_func(self, 10000)
    simple_query_func(self, 100000)
    simple_query_func(self, 1000000)


  def test_benchmark(self): #remove 'a_' to run
    start_ts = 10L
    series_count = 8
    name = 0
    start_time = time.time()

    with self.redis() as r:
      r.execute_command('FLUSHALL')
      print("\n--- finished flashing after %s seconds ---" % (time.time() - start_time))
      start_time = time.time()
      with r.pipeline(transaction=False) as p:
        for i in range(series_count):
          for j in range(series_count):
            for k in range(series_count):
              for l in range(series_count):
                for m in range(series_count):
                  for n in range(series_count):
                    #p.execute_command('TS.CREATE', name, 'LABELS', 'a', 'a'+str(i), 'b', 'b'+str(j), 'c', 'c'+str(k), 'd', 'd'+str(l), 'e', 'e'+str(m), 'f', 'f'+str(n))
                    p.execute_command('TS.CREATE', name, 'LABELS', 'a', i, 'b', j, 'c', k, 'd', l, 'e', m, 'f', n)
                    name += 1   
        p.execute()
        print("--- finished creating after %s seconds ---" % (time.time() - start_time))
        random.seed(0)
        query_list = []
        
        for _ in range(100):
          query_list.append([str(random.randint(0,series_count-1)), str(random.randint(0,series_count-1)), str(random.randint(0,series_count-1)),
                             str(random.randint(0,series_count-1)), str(random.randint(0,series_count-1)), str(random.randint(0,series_count-1))])
        start_time = time.time()
        '''
        for i in range(series_count):
          for j in range(series_count):
            for k in range(series_count):
              for l in range(series_count):
                for m in range(series_count):
                  for n in range(series_count):
                    p.execute_command('TS.QUERYINDEX', 'a='+str(i), 'b='+str(j), 'c='+str(k), 'd='+str(l), 'e='+str(m), 'f='+str(n))
#                    p.execute_command('TS.QUERYINDEX', 'a=a'+str(i), 'b=b'+str(j), 'c=c'+str(k), 'd=d'+str(l), 'e=e'+str(m), 'f=f'+str(n))
        '''
        
        for i in range(100):
          p.execute_command('TS.QUERYINDEX', 'a='+query_list[i][0], 'b='+query_list[i][1], 'c='+query_list[i][2],
                                            'd='+query_list[i][3], 'e='+query_list[i][4], 'f='+query_list[i][5])
        p.execute()
        print("--- %s seconds ---" % (time.time() - start_time))

      
      print
      print("--- %s seconds ---" % (time.time() - start_time))
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'a=(0,1,2,3,4,5)')
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      print len(result)
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'a=(0,1,2,3,4,5)', 'b!=(1,2,3,4)')
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      print len(result)
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'a=8')
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      print len(result)
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'b=(1,4)', 'c=(2,3)', 'd!=(4,5)', 'e=(1,2,3)')
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      print len(result)
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'b=(1,4)', 'c=(2,3)', 'd!=(4,5)', 'e!=(1,2,3)')
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      print len(result)
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'a=(3,6,9)', 'b=(1,4)', 'c=(2,3)', 'd!=(4,5)', 'e!=(1,2,3)', 'f=(2,4,6,8)')
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      print len(result)
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'a=(3,6,9)', 'b=(1,4)', 'c=(2,3)', 'd!=(4,5)', 'e!=(1,2,3)', 'f!=(2,4,6,8)')
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      print len(result)
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'b=(1,4)', 'c=(2,3)', 'd!=(4,5)', 'e!=(1,2,3)')
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      print len(result)
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'a=1', 'b=3', 'c=5', 'd=2')
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      print len(result)
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'b=3', 'c!=4', 'e!=5')
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      print len(result)
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'b=3', 'c!=(1,2,6)', 'e!=5')
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      print len(result)
      start_time = time.time()
      result = r.execute_command('TS.QUERYINDEX', 'b=3', 'c=(1,2,5)', 'e!=5')
      print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
      print len(result)
  
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
    series_count =  100000
    name = 0
    start_time = time.time()

    with self.redis() as r:
      r.execute_command('FLUSHALL')
      with r.pipeline() as p:

        for i in range(series_count):
          labels_list = create_labels(names_list, lists_list)
          chars = digits + ascii_lowercase
          keyname = "".join([choice(chars) for j in range(15)])
          #print keyname
          p.execute_command('TS.CREATE', keyname, 'LABELS', *labels_list)
        p.execute()

        print "\n\nCreation of "+str(series_count)+ " time series ended after"
        print("--- %s seconds ---" % (time.time() - start_time))
        label = []
        ###########
        start_time = time.time()
        result = r.execute_command('TS.QUERYINDEX', names_list[0]+'='+lists_list[0][0])
        print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
        print len(result)
        ###########
        start_time = time.time()
        result = r.execute_command('TS.QUERYINDEX', names_list[1]+'='+lists_list[1][0])
        print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
        print len(result)
        ###########
        start_time = time.time()
        result = r.execute_command('TS.QUERYINDEX', names_list[2]+'='+lists_list[2][0])
        print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
        print len(result)
        ###########
        start_time = time.time()
        result = r.execute_command('TS.QUERYINDEX', names_list[3]+'='+lists_list[3][2])
        print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
        print len(result)
        ###########
        start_time = time.time()
        result = r.execute_command('TS.QUERYINDEX', names_list[4]+'='+lists_list[4][2])
        print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
        print len(result)
        ###########
        start_time = time.time()
        result = r.execute_command('TS.QUERYINDEX', names_list[5]+'='+lists_list[5][2])
        print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
        print len(result)
        ###########
        start_time = time.time()
        result = r.execute_command('TS.QUERYINDEX', names_list[6]+'='+lists_list[6][2])
        print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
        print len(result)
        ###########
        start_time = time.time()
        result = r.execute_command('TS.QUERYINDEX', names_list[2]+'!=('+lists_list[2][2]+','+lists_list[2][7]+','+lists_list[2][20]+')',
                                                    names_list[1]+'=('+lists_list[1][2]+','+lists_list[1][7]+','+lists_list[1][18]+')',
                                                    names_list[4]+'!=('+lists_list[4][2]+','+lists_list[4][7]+','+lists_list[4][18]+')',
                                                    names_list[6]+'!=('+lists_list[6][2]+')')
        print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
        print len(result)
        ###########
        start_time = time.time()
        result = r.execute_command('TS.QUERYINDEX', names_list[0]+'=('+lists_list[0][2]+','+lists_list[0][4]+','+lists_list[0][0]+')',
                                                    names_list[1]+'=('+lists_list[1][2]+','+lists_list[1][7]+','+lists_list[1][18]+')',
                                                #   names_list[4]+'!=('+lists_list[4][2]+','+lists_list[4][7]+','+lists_list[4][18]+')',
                                                    names_list[6]+'!=('+lists_list[6][2]+')')    
        print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
        print len(result)
        ###########
        start_time = time.time()
        result = r.execute_command('TS.QUERYINDEX', names_list[5]+'=('+lists_list[5][2]+','+lists_list[5][7]+','+lists_list[1][18]+')')
        print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))      
        print len(result)
        ###########
        start_time = time.time()
        result = r.execute_command('TS.QUERYINDEX', names_list[5]+'!=('+lists_list[5][2]+','+lists_list[5][7]+','+lists_list[1][18]+')',
                                                    names_list[0]+'=('+lists_list[0][2]+','+lists_list[0][4]+','+lists_list[0][0]+')')
        print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
        print len(result)
      

      
        start_time = time.time()
        result = r.execute_command('TS.QUERYINDEX', 'b=(1,4)', 'c=(2,3)', 'd!=(4,5)', 'e=(1,2,3)')
        print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
        print len(result)
        start_time = time.time()
        result = r.execute_command('TS.QUERYINDEX', 'b=(1,4)', 'c=(2,3)', 'd!=(4,5)', 'e=(1,2,3)')
        print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
        print len(result)
        start_time = time.time()
        result = r.execute_command('TS.QUERYINDEX', 'a=1', 'b=3', 'f=5', 'd=2')
        print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
        print len(result)
        start_time = time.time()
        result = r.execute_command('TS.QUERYINDEX', 'b=3', 'c!=4', 'e!=5')
        print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
        print len(result)
        start_time = time.time()
        result = r.execute_command('TS.QUERYINDEX', 'b=3', 'c!=(1,2,6)', 'e!=5')
        print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
        print len(result)
        start_time = time.time()
        result = r.execute_command('TS.QUERYINDEX', 'b=3', 'c=(1,2,5)', 'e!=5')
        print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
        print len(result)

  def test_no_labels(self):
    with self.redis() as r:
      r.execute_command('FLUSHALL')
      with r.pipeline(transaction=False) as p:
        for i in range(812000):
          p.execute_command('TS.CREATE', i)
        p.execute()
      print r.execute_command('info memory')

  def test_realistic(self): #remove 'a_' to run
    name = 0
    zone_l = [5, 'USA', 'EMEA', 'Asia', 'Africa', 'Oceania']
    center_l = [[8, 'New York', 'Washington', 'Orlando', 'Chicago', 'Seattle', 'San Francisco', 'San Diego', 'Austin'],
                [7, 'London', 'Berlin', 'Doha', 'Tel Aviv', 'Copenhagen', 'Dubai', 'Helsinki'],
                [7, 'Moscow', 'Hong Kong', 'Beijing', 'Mumbai', 'Tokyo', 'Singapure', 'Hanoi'],
                [4, 'Cape Town', 'Lagos', 'Timbuktu', 'Tunis'],
                [3, 'Melborne', 'Perth', 'Aukland']]
    cluster_range = 20
    machine_range = 50
    measurement_l = [7, 'RAM', 'CPU', 'Storage', 'Temp', 'Humidity', 'Power', 'Latency']
    provider_l = [4, 'QWS', 'Azure', 'GCP', 'IBM Cloud']

    start_time = time.time()

    with self.redis() as r:
      r.execute_command('FLUSHALL')
      print("\n--- finished flashing after %s seconds ---" % (time.time() - start_time))
      start_time = time.time()
      with r.pipeline(transaction=False) as p:
        for zone in range(1, zone_l[0] + 1):
          for center in range(1, center_l[zone - 1][0] + 1):
            for cluster in range(cluster_range):
              for machine in range(machine_range):
                for measurement in range(1, measurement_l[0] + 1):
                  for provider in range(1, provider_l[0] + 1):
                    #p.execute_command('TS.CREATE', name, 'LABELS', 'a', 'a'+str(i), 'b', 'b'+str(j), 'c', 'c'+str(k), 'd', 'd'+str(l), 'e', 'e'+str(m), 'f', 'f'+str(n))
                    p.execute_command('TS.CREATE', name, 'LABELS', 
                                      'zone', zone_l[zone],
                                      'center', center_l[zone - 1][center],
                                      'cluster',  cluster,
                                      'machine', machine,
                                      'measurement', measurement_l[measurement],
                                      'provider', provider_l[provider])
                    name += 1   
        p.execute()
        print("--- finished creating after %s seconds ---" % (time.time() - start_time))
        random.seed(0)
        
        start_time = time.time()
        
        for i in range(1000):
          zone = random.randint(1, zone_l[0])
          p.execute_command('TS.QUERYINDEX', 'zone='+zone_l[zone],
                                            'center='+center_l[zone - 1][random.randint(1, center_l[zone -1][0])],
                                            'cluster='+str(random.randint(0, cluster_range)),
                                            'machine='+str(random.randint(0, machine_range)), 
                                            'measurement='+measurement_l[random.randint(1, measurement_l[0])],
                                            'provider='+provider_l[random.randint(1, provider_l[0])],)
        result = p.execute()
        end_time = time.time()
        #print result
        print("--- Querying for %s seconds ---" % (end_time - start_time))
        print name
        print r.execute_command('info memory')