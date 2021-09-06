import inspect

from RLTest import Env as rltestEnv, Defaults
from includes import *


def Env(*args, **kwargs):
    if 'testName' not in kwargs:
        kwargs['testName'] = '%s.%s' % (inspect.getmodule(inspect.currentframe().f_back).__name__, inspect.currentframe().f_back.f_code.co_name)
    if 'simple' in kwargs:
        kwargs.pop('simple')
        return rltestEnv(*args, **kwargs)
    env = rltestEnv(*args, **kwargs)
    env.custom = 'RedisTimeSeries' # for Default.env_factory testing
    if not RLEC_CLUSTER:
        for shard in range(0, env.shardsCount):
            conn = env.getConnection(shard)
            modules = conn.execute_command('MODULE', 'LIST')
            if not any(module for module in modules if (module[1] == 'rg' or module[1] == 'rg')):
                break
            conn.execute_command('RG.REFRESHCLUSTER')
    else:
        for shard in range(0, env.shardsCount):
            try:
                env.getConnection(shard).execute_command('RG.REFRESHCLUSTER')
            except:
                pass
    return env

Defaults.env_factory = Env

def set_hertz(env):
    if RLEC_CLUSTER:
        return
    for shard in range(0, env.shardsCount):
        # Lower hz value to make it more likely that mrange triggers key expiration
        env.expect('config set hz 1', conn=env.getConnection(shard)).ok()
