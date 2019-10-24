import requests
import redis
import time

redis = redis.Redis(host='localhost', port=6379, db=0)
redis_pipe = redis.pipeline()
redis.flushall()

print "start benchmark"

start_time = time.time()

num_sample = 1024 * 1024

redis_pipe.execute_command('ts.create test')

for i in range(num_sample):
  redis_pipe.execute_command('ts.add test', i, i)
redis_pipe.execute()

total_time = time.time() - start_time

redis.execute_command('ts.range test 0 -1')

print total_time