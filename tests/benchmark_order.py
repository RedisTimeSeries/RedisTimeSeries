import redis
import time

redis = redis.Redis(host='localhost', port=6379, db=0)
redis.execute_command('flushall')
redis_pipe = redis.pipeline()

print "start benchmark"


print "no out-of-order"

start_time = time.time()

name = "rts"
start = 1000000
num_sample = 1024 * 1024 + 1

redis.execute_command('ts.create', name, 'uncompressed')

for i in range(start, start + num_sample, 2):
  redis_pipe.execute_command('ts.add', name, i, i)
  if i % 999 == 0:
    redis_pipe.execute()
redis_pipe.execute()

print time.time() - start_time
