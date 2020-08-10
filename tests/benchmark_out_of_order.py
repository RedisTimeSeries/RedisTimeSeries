import redis
import time

redis = redis.Redis(host='localhost', port=6379, db=0)
redis.execute_command('flushall')
redis_pipe = redis.pipeline()

print "start benchmark"

print "no out-of-order"

start_time = time.time()

name = "rts"
start = 1000001
num_sample = 1024 * 1024 + 1

redis.execute_command('ts.create', name, 'uncompressed')

for i in range(start, start + num_sample, 2):
  redis_pipe.execute_command('ts.add', name, i, i)
  if i % 999 == 0:
    #print 'execute'
    redis_pipe.execute()
redis_pipe.execute()

print time.time() - start_time
print redis.execute_command('ts.info', name)

redis.execute_command('flushall')
print "out-of-order"
start_time = time.time()

name_ooo = "rts_ooo"
redis.execute_command('ts.create', name_ooo, 'uncompressed')

for i in range(start, start + (num_sample / 2), 2):
  redis_pipe.execute_command('ts.add', name_ooo, i, i)
  if i % 999 == 0:
    responses = redis_pipe.execute()
    for response in responses:
	    pass
    
responses = redis_pipe.execute()
for response in responses:
  pass

for i in range(start + (num_sample / 2), start + num_sample, 2):
  if i % 9 != 0:
    redis_pipe.execute_command('ts.add', name_ooo, i, i)
  else:
    redis_pipe.execute_command('ts.add', name_ooo, i - 111, i)
  if i % 999 == 0:
    print 1
    responses = redis_pipe.execute()
    for response in responses:
	    pass
    
responses = redis_pipe.execute()
for response in responses:
  pass

print time.time() - start_time
print redis.execute_command('ts.info', name_ooo)