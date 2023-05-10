import os
import sys
from logging import exception
from RLTest import Env as rltestEnv, Defaults
from packaging import version
import inspect
import redis
import pytest
from functools import wraps

try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../deps/readies"))
    import paella
except exception:
    pass


DISABLE_AOF_PARSER=True  # TODO: remove when hiredis RESP3-related problem is resolved

OSNICK = paella.Platform().osnick

RLEC_CLUSTER = os.getenv('RLEC_CLUSTER') == '1'

SANITIZER = os.getenv('SANITIZER', '')
VALGRIND = os.getenv('VALGRIND', '0') == '1'
CODE_COVERAGE = os.getenv('CODE_COVERAGE', '0') == '1'


Defaults.terminate_retries = 3
Defaults.terminate_retries_secs = 1


def Env(*args, **kwargs):
    if 'testName' not in kwargs:
        kwargs['testName'] = '%s.%s' % (inspect.getmodule(inspect.currentframe().f_back).__name__, inspect.currentframe().f_back.f_code.co_name)
    env = rltestEnv(*args, terminateRetries=3, terminateRetrySecs=1, **kwargs)
    if not RLEC_CLUSTER:
        for shard in range(0, env.shardsCount):
            conn = env.getConnection(shard)
            modules = conn.execute_command('MODULE', 'LIST')
            if env.protocol == 2:
                if not any(module for module in modules if (module[1] == b'timeseries' or module[1] == 'timeseries')):
                    break
            else:
                if not any(module for module in modules if (module[b'name'] == b'timeseries' or module[b'name'] == 'timeseries')):
                    break
            conn.execute_command('timeseries.REFRESHCLUSTER')
    return env

Defaults.env_factory = Env


def is_rlec():
    if RLEC_CLUSTER:
        return True
    else:
        return False

def skip_on_rlec():
    if RLEC_CLUSTER:
        rltestEnv().skip()

def decode_if_needed(data):
    if isinstance(data, list):
        ret = []
        for item in data:
            ret.append(decode_if_needed(item))
        return ret
    elif isinstance(data, bytes):
        return data.decode()
    else:
        return data

def is_redis_version_smaller_than(con, _version, is_cluster=False):
    res = con.execute_command('INFO')
    ver = ""
    if is_cluster:
        try:
            ver = ((list(res.values()))[0])['redis_version']
        except:
            ver = res['redis_version']
        #print(((list(res.values()))[0]))
    else:
        ver = res['redis_version']
    return (version.parse(ver) < version.parse(_version))

def skip(always=False, on_cluster=False, on_macos=False, asan=False):
    def decorate(f):
        @wraps(f)
        def wrapper(x, *args, **kwargs):
            env = x if isinstance(x, rltestEnv) else x.env
            if always:
                env.skip()
            if on_cluster and env.isCluster():
                env.skip()
            if on_macos and OS == 'macos':
                env.skip()
            if SANITIZER == 'address':
                env.skip()
            return f(x, *args, **kwargs)
        return wrapper
    return decorate
