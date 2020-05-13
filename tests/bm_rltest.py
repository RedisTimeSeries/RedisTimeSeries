# python -m RLTest --module=./bin/linux-x64-release/redistimeseries.so  -t ./tests/bm_rltest.py -v

from RLTest import Env
import time

def testBM(env):
  name = "rts"
  start = 1000001
  dbl = 1
  num_sample = 1024 * 1024

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

    print "out-of-order"
    name_ooo = "rts_ooo"
    start_time = time.time()
    r.execute_command('ts.create', name_ooo, 'uncompressed')

    for i in range(start, start + (num_sample / 2), 2):
      pl.execute_command('ts.add', name_ooo, i, i * dbl)
      if i % 999 == 0:
        pl.execute()        
    pl.execute()

    for i in range(start + (num_sample / 2), start + num_sample, 2):
      if i % 9 != 0:
        pl.execute_command('ts.add', name_ooo, i, i * dbl)
      else:
        pl.execute_command('ts.add', name_ooo, i - 21, (i - 21) * dbl)
      if i % 999 == 0:
        pl.execute()
    pl.execute()

    print time.time() - start_time
    print env.execute_command('ts.info', name_ooo)

    res = env.execute_command('ts.range', name_ooo, 0, -1)
    for i in range(len(res) - 1):
      #print str(res[i][0]) + ' ' + res[i][1]
      assert str(res[i][0]) == str(res[i][1])
      assert res[i][0] < res[i+1][0]
      