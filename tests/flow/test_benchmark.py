import random

import pytest
import redis
from RLTest import Env
from test_helper_classes import _get_ts_info
from includes import *


def test_rand_oom(self):
    count = int(1000000 / 3)
    random.seed(20)
    start_ts = 1
    current_ts = int(start_ts)
    data = []
    for i in range(count):
        val = random.randint(0, 10000)
        #fltval = float(val) / 100.0
        #print (fltval)
        data.append([current_ts, 5])
        current_ts += 1 #random.randrange(20, 1000)
        data.append([current_ts, 100])
        current_ts += 1 #random.randrange(20, 1000)
        data.append([current_ts, -100])
        current_ts += 1 #random.randrange(20, 1000)
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'tester')
        for sample in data:
            r.execute_command('ts.add', 'tester', sample[0], sample[1])
        #print(r.execute_command('ts.range', 'tester', '-', '+'))
        
    input('stop')
