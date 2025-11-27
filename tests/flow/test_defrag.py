import time
from includes import *

from RLTest import Defaults

def enableDefrag(env):
    version = env.cmd('info', 'server')['redis_version']
    if '6.0' in version:
        # skip on version 6.0 and defrag API for modules was not supported on this version.
        env.skip()

    # make defrag as aggressive as possible
    env.cmd('CONFIG', 'SET', 'hz', '100')
    env.cmd('CONFIG', 'SET', 'active-defrag-ignore-bytes', '1')
    env.cmd('CONFIG', 'SET', 'active-defrag-threshold-lower', '0')
    env.cmd('CONFIG', 'SET', 'active-defrag-cycle-min', '99')

    try:
        env.cmd('CONFIG', 'SET', 'activedefrag', 'yes')
    except Exception:
        # If active defrag is not supported by the current Redis, simply skip the test.
        env.skip()

def testDefrag(env):
    if VALGRIND:
        env.skip()
    enableDefrag(env)


    # Disable defrag so we can actually create fragmentation
    env.cmd('CONFIG', 'SET', 'activedefrag', 'no')
    numKeys = 1000
    numData = 500
    skip = 10

    with env.getClusterConnectionIfNeeded() as r:
        # Create keys with some data
        for i in range(numKeys):
            r.execute_command('ts.create', f'ts{i}')
            for j in range(numData):
                r.execute_command('ts.add', f'ts{i}', j, j * 1.1)

        # Delete some keys
        for i in range(numKeys):
            if i % skip != 0:
                r.execute_command('del', f'ts{i}')
            else:
                for j in range(numData):
                    if j % skip != 0:
                        r.execute_command('ts.del', f'ts{i}', j, j)

    sleepTime = 5

    # wait for fragmentation for up to sleepTime seconds
    time.sleep(sleepTime)
    frag = env.cmd('info', 'memory')['allocator_frag_ratio']
    defragExpected = 1 + (frag - 1) * 0.90

    #enable active defrag
    env.cmd('CONFIG', 'SET', 'activedefrag', 'yes')

    # wait for fragmentation for go down for up to sleepTime seconds
    frag = env.cmd('info', 'memory')['allocator_frag_ratio']
    startTime = time.time()
    while frag > defragExpected:
        time.sleep(0.1)
        frag = env.cmd('info', 'memory')['allocator_frag_ratio']
        if time.time() - startTime > sleepTime:
            # We will wait for up to sleepTime seconds and then we consider it a failure
            env.assertTrue(False, message=f'Failed waiting for fragmentation to go down, current value {frag} which is expected to be below {defragExpected}.')
            return
