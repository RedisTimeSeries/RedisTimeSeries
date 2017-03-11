import redis
import time
import pprint
import multiprocessing

r = redis.Redis()
ts = int(time.time())
pool_size = 20 
tsrange = 140000 / pool_size
r.delete('test')
r.delete('test_agg_10')
r.delete('test_sum_10')
r.delete('test_avg_100')
r.execute_command('ts.create', 'test', 0, 360)
# r.execute_command('ts.rule', 'test', 'avg', 10, 'test_agg_10')
# r.execute_command('ts.rule', 'test', 'count', 10, 'test_sum_10')
# r.execute_command('ts.rule', 'test_agg_10', 'avg', 100, 'test_avg_100')
print "from %s to %s" % (ts, ts+tsrange)

for i in range(pool_size):
    r.delete('test_%d' % i)
    r.execute_command('ts.create', 'test_%d' % i, 0, 360)

def func(arg):
    r = redis.Redis()
    pipe = r.pipeline(tsrange)
    for i in range(tsrange):
    #    p.zadd('test', ts+i, i)
        if tsrange % 100 and False:
            pipe.execute()
            pip = r.pipeline
        pipe.execute_command("ts.add","test_%d" % arg, ts+i, i)
    pipe.execute()
    return  True

pool = multiprocessing.Pool(pool_size)
s = time.time()
pool.map(func, range(pool_size))
e = time.time()
insert_time = e - s

print "items %s:" % (tsrange * pool_size )
print "took %s to insert sec" % insert_time
# print "took %s to query sec" % query_time
