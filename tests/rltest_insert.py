# python -m RLTest --module=./bin/linux-x64-release/redistimeseries.so  -t ./tests/bm_rltest.py -v

from RLTest import Env
import time

def testBM(env):
  name = "rts"
  start = 1000001
  dbl = 1
  num_sample = 1024 * 16

  env.execute_command('flushall')
  env.execute_command('ts.create', name, 'uncompressed')
  print "not out-of-order"
  r = env.getConnection()
  start_time = time.time()
  with r.pipeline() as pl:
    for i in range(start, start + num_sample, 2):
      pl.execute_command('ts.add', name, i, i * dbl)
      if i % 999 == 0:
        pl.execute()
    pl.execute()
    print time.time() - start_time
    print env.execute_command('ts.info', name)