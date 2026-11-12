import os
import sys
from logging import exception
from RLTest import Env as _rltestEnv
from packaging import version
from redis import RedisCluster

# These legacy commands fan out inside the module and have no routing key.
# Send each query to one coordinator; redis-py's generic key parser rejects them.
RedisCluster.COMMAND_FLAGS.update({
    command: RedisCluster.DEFAULT_NODE
    for command in ('TS.MGET', 'TS.MRANGE', 'TS.MREVRANGE', 'TS.QUERYINDEX')
})

try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../deps/readies"))
    import paella
except exception:
    pass

OSNICK = paella.Platform().osnick

RLEC_CLUSTER = os.getenv('RLEC_CLUSTER') == '1'

SANITIZER = os.getenv('SANITIZER', '')
VALGRIND = os.getenv('VALGRIND', '0') == '1'
CODE_COVERAGE = os.getenv('CODE_COVERAGE', '0') == '1'

def is_rlec():
    if RLEC_CLUSTER:
        return True
    else:
        return False

def skip_on_rlec():
    if RLEC_CLUSTER:
        _rltestEnv().skip()


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
