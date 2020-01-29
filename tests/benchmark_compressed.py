import redis
import time

redis = redis.Redis(host='localhost', port=6379, db=0)
redis_pipe = redis.pipeline()

print "start benchmark"

start_time = time.time()

#name = str(time.time())
name = "rts"
num_sample = 1024 * 1024
redis.execute_command('flushall')
redis_pipe.execute_command('ts.create', name, 'uncompressed')

for i in range(num_sample):
  redis_pipe.execute_command('ts.add', name, i, i)
  if i % 999 == 0:
    redis_pipe.execute()
    
print time.time() - start_time
start_time = time.time()

print "run range"
for _ in range(50):
  redis.execute_command('ts.range', name, 0, -1)
  print ".",

print time.time() - start_time
