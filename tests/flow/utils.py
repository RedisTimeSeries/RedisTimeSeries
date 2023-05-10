
from includes import *
from packaging import version
import time
import inspect


def Refresh_Cluster(env):
    for shard in range(0, env.shardsCount):
        con = env.getConnection(shard)
        try:
            modules = con.execute_command('MODULE', 'LIST')
        except Exception as e:
            continue
        if not any(module for module in modules if (module[1] == b'timeseries' or module[1] == 'timeseries')):
            break
        con.execute_command('timeseries.REFRESHCLUSTER')

def set_hertz(env):
    if RLEC_CLUSTER:
        return
    for shard in range(0, env.shardsCount):
        # Lower hz value to make it more likely that mrange triggers key expiration
        assert env.getConnection(shard).execute_command('config set hz 1') == b'OK'

class Colors(object):
    @staticmethod
    def Cyan(data):
        return '\033[36m' + data + '\033[0m'

    @staticmethod
    def Yellow(data):
        return '\033[33m' + data + '\033[0m'

    @staticmethod
    def Bold(data):
        return '\033[1m' + data + '\033[0m'

    @staticmethod
    def Bred(data):
        return '\033[31;1m' + data + '\033[0m'

    @staticmethod
    def Gray(data):
        return '\033[30;1m' + data + '\033[0m'

    @staticmethod
    def Lgray(data):
        return '\033[30;47m' + data + '\033[0m'

    @staticmethod
    def Blue(data):
        return '\033[34m' + data + '\033[0m'

    @staticmethod
    def Green(data):
        return '\033[32m' + data + '\033[0m'

def timeit(method):
    def timed(*args, **kw):
        test_name = '%s:%s' % (inspect.getfile(method), method.__name__)
        print(Colors.Cyan('\tRunning: %s' % test_name))
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f ms' % \
                  (method.__name__, (te - ts) * 1000))
        return result
    return timed

def is_resp3_possible(env):
    res = env.execute_command('INFO')
    if version.parse(res['redis_version']) < version.parse("7.0.0"):
        return False
    else:
        return True
