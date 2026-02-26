import os
import sys
from logging import exception
from RLTest import Env as rltestEnv, Defaults
from packaging import version
import inspect
import redis
import pytest
import signal
import time
import tempfile
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
# Valgrind can be enabled either directly (VALGRIND=1) or via the make test flag (VG=1).
VALGRIND = (os.getenv('VALGRIND', '0') == '1') or (os.getenv('VG', '0') == '1')
CODE_COVERAGE = os.getenv('CODE_COVERAGE', '0') == '1'
GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS', '').lower() == 'true'


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(v)
    except ValueError:
        return default


_terminate_retries = 3
_terminate_retries_secs = 1

# Valgrind slows shutdown significantly; give Redis more time to exit cleanly
# to avoid RLTest force-killing processes (which then shows up as leaks).
if VALGRIND:
    # In CI, a very large shutdown patience can make the job exceed the global
    # Actions timeout (it adds up across many Redis instances). Keep it smaller
    # in CI, but allow local runs to be more patient.
    if GITHUB_ACTIONS:
        _terminate_retries = 12
        _terminate_retries_secs = 1
    else:
        _terminate_retries = 20
        _terminate_retries_secs = 2
elif SANITIZER:
    # Sanitized builds are slower too, but not as much as valgrind.
    _terminate_retries = 10
    _terminate_retries_secs = 1

_terminate_retries = _env_int('RLTEST_TERMINATE_RETRIES', _terminate_retries)
_terminate_retries_secs = _env_int('RLTEST_TERMINATE_RETRY_SECS', _terminate_retries_secs)

Defaults.terminate_retries = _terminate_retries
Defaults.terminate_retries_secs = _terminate_retries_secs


class ShardConnectionTimeoutException(Exception):
    pass

class TimeLimit(object):
    """
    A context manager that fires a TimeExpired exception if it does not
    return within the specified amount of time.
    """

    def __init__(self, timeout):
        self.timeout = timeout

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handler)
        signal.setitimer(signal.ITIMER_REAL, self.timeout, 0)

    def __exit__(self, exc_type, exc_value, traceback):
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)

    def handler(self, signum, frame):
        raise ShardConnectionTimeoutException()

def shardsConnections(env: rltestEnv):
    for s in range(1, env.shardsCount + 1):
        yield env.getConnection(shardId=s)

def verifyClusterInitialized(env):
    for conn in shardsConnections(env):
        try:
            conn.execute_command('debug', 'MARK-INTERNAL-CLIENT')
        except Exception:
            pass # in case we run on older version of redis
        allConnected = False
        while not allConnected:
            res = conn.execute_command('timeseries.INFOCLUSTER')
            nodes = res[4]
            allConnected = True
            for n in nodes:
                status = n[17]
                if status != b'connected':
                    allConnected = False
            if not allConnected:
                time.sleep(0.1)

def Env(*args, **kwargs):
    if 'testName' not in kwargs:
        kwargs['testName'] = '%s.%s' % (inspect.getmodule(inspect.currentframe().f_back).__name__, inspect.currentframe().f_back.f_code.co_name)
    if 'redisConfigFileContent' in kwargs:
        kwargs['redisConfigFile'] = create_config_file(kwargs['redisConfigFileContent'])
        del kwargs['redisConfigFileContent']

    temp_no_log = Defaults.no_log
    no_capture_output = Defaults.no_capture_output

    if 'noLog' in kwargs:
        Defaults.no_log = kwargs['noLog']
        # Defaults.no_capture_output = True
        del kwargs['noLog']

    env = rltestEnv(*args,
                    terminateRetries=_terminate_retries,
                    terminateRetrySecs=_terminate_retries_secs,
                    **kwargs)
    Defaults.no_log = temp_no_log
    Defaults.no_capture_output = no_capture_output

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

def _version_from_string(version_string):
    if sys.version_info[1] > 12:
        return version.Version(version_string)
    else:
        return version.parse(version_string)

def get_redis_version(con, is_cluster=False):
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

    return _version_from_string(ver)

def is_redis_version_lower_than(con, _version, is_cluster=False):
    version = get_redis_version(con, is_cluster)
    return (version < _version_from_string(_version))

def is_redis_version_higher_than(con, _version, is_cluster=False):
    version = get_redis_version(con, is_cluster)
    return (version > _version_from_string(_version))

def skip(always=False, on_cluster=False, on_macos=False, asan=False, onVersionLowerThan=None, onVersionHigherThan=None):
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
            if asan and SANITIZER == 'address':
                env.skip()
            if onVersionLowerThan and is_redis_version_lower_than(env, onVersionLowerThan, env.isCluster()):
                env.skip()
            if onVersionHigherThan and is_redis_version_higher_than(env, onVersionHigherThan, env.isCluster()):
                env.skip()
            return f(x, *args, **kwargs)
        return wrapper
    return decorate

def get_server_log_path(env):
    path = env.getConnection().execute_command('CONFIG', 'GET', 'logfile')[1].decode()
    # path = env.envRunner._getFileName('master', '.log')
    if os.path.isabs(path):
        return path
    return os.path.abspath(f"{env.logDir}/{path}")

def is_line_in_server_log(env, line):
    path = get_server_log_path(env)

    if path.endswith('/dev/null'):
        raise Exception("Server log is redirected to /dev/null, can't check for the logs.")

    with open(path) as file:
        for file_line in file:
            if line in file_line:
                return True
    return False

# Creates a temporary file with the content provided.
# Returns the filepath of the created file.
def create_config_file(content) -> str:
    dir = f"{os.getcwd()}/logs/"
    os.makedirs(dir, exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix='temp-redis-config', delete=False, dir=dir) as f:
        f.write(content.encode())
        return f.name
