import inspect

from RLTest import Env as rltestEnv
from includes import *

def Env(*args, **kwargs):
    if 'testName' not in kwargs:
        kwargs['testName'] = '%s.%s' % (inspect.getmodule(inspect.currentframe().f_back).__name__, inspect.currentframe().f_back.f_code.co_name)
    env = rltestEnv(*args, **kwargs)
    if not RLEC_CLUSTER:
        for shard in range(0, env.shardsCount):
            modules = env.getConnection(shard).execute_command('MODULE', 'LIST')
            if not any(module for module in modules if (module[1] == b'timeseries' or module[1] == 'timeseries')):
                break
            env.getConnection(shard).execute_command('timeseries.REFRESHCLUSTER')
    return env

def set_hertz(env):
    if RLEC_CLUSTER:
        return
    for shard in range(0, env.shardsCount):
        # Lower hz value to make it more likely that mrange triggers key expiration
        assert env.getConnection(shard).execute_command('config set hz 1') == b'OK'
