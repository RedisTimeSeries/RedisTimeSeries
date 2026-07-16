import os
import re
import shutil
import subprocess
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
VALGRIND = (os.getenv('VALGRIND', '0') == '1') or (os.getenv('VG', '0') == '1')
CODE_COVERAGE = os.getenv('CODE_COVERAGE', '0') == '1'

BIGREDIS_TESTS = os.getenv('BIGREDIS_TESTS', '0') == '1'

# Redis config template injected when BIGREDIS_TESTS=1. bigredis-path is
# NOT in here — RLTest writes a single config file shared by all shards in
# an OSS cluster Env, and a fixed bigredis-path would make 2..N shards
# collide on the same speedb data dir (only shard 1 survives, cluster
# never forms). Instead we monkey-patch StandardEnv.createCmdArgs below
# to append a per-port --bigredis-path cmdline override per shard.
# bigredis-max-ram-keys is intentionally aggressive (5) so most key
# operations hit flash and exercise the prefetch path.
_BIGREDIS_CONFIG_TEMPLATE = """
bigredis-enabled yes
bigredis-driver speedb
bigredis-max-ram-keys 5
bigredis-use-async no
"""

# Per-process base dir for all per-shard bigredis data dirs. Lazily
# initialised on first Env() with BIGREDIS_TESTS=1.
_bigredis_shards_base = None

def _install_bigredis_per_shard_patch():
    """Monkey-patch RLTest's StandardEnv to append --bigredis-path per shard.

    Each redis-server gets a unique data dir derived from its --port, so
    multiple shards in an OSS cluster Env never collide on the same speedb
    directory. Cmdline args override the redisConfigFile, so the fact that
    the config doesn't set bigredis-path is fine.

    Note: RLTest reuses ports across the 5 variants (general/slaves/AOF/AOF-
    slaves/cluster) within a single Python process. A port-keyed dir alone
    would let stale speedb SST files from variant N collide with variant N+1.
    To prevent that we shutil.rmtree the per-port subdir before every boot —
    each StandardEnv boot starts with an empty data directory.
    """
    global _bigredis_shards_base
    import shutil
    import RLTest.redis_std as _redis_std
    if getattr(_redis_std.StandardEnv, '_bigredis_per_shard_patched', False):
        return
    _bigredis_shards_base = tempfile.mkdtemp(prefix='bigredis-shards-')
    _orig_createCmdArgs = _redis_std.StandardEnv.createCmdArgs

    def _patched_createCmdArgs(self, role):
        args = _orig_createCmdArgs(self, role)
        try:
            port = self.getPort(role)
        except Exception:
            port = 0
        shard_dir = os.path.join(_bigredis_shards_base, f'p{port}')
        # Wipe any leftover state from a prior variant that happened to reuse
        # this port. Speedb can't open a non-empty dir as a fresh DB.
        shutil.rmtree(shard_dir, ignore_errors=True)
        os.makedirs(shard_dir, exist_ok=True)
        return args + ['--bigredis-path', shard_dir]

    _redis_std.StandardEnv.createCmdArgs = _patched_createCmdArgs
    _redis_std.StandardEnv._bigredis_per_shard_patched = True


if BIGREDIS_TESTS:
    _install_bigredis_per_shard_patch()

# CI matrix runner label (e.g. "macos-15-intel"), exported by the workflow. Empty locally.
RUNNER_LABEL = os.getenv('RUNNER_LABEL', '')

# Upper bound on how long a "prompt" wake (e.g. TS.READ woken by a key deletion)
# may take before we consider it a regression. Scaled up under Valgrind/sanitizer
# so the slower wake-up callback there can't produce a false failure.
WAKE_TIMEOUT_SECS = 30 if (VALGRIND or SANITIZER) else 5

# Use generous terminate patience for all configurations. RLTest polls and
# returns as soon as the process exits, so a high retry count costs nothing
# when shutdown is fast, but prevents force-kills under Valgrind/sanitizer
# where shutdown is much slower.
Defaults.terminate_retries = 20
Defaults.terminate_retries_secs = 1


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

def is_enterprise(env):
    """Heuristic: redis-private (enterprise) binaries advertise rlec_version
    in INFO Server. OSS redis doesn't. Used to gate behaviors that depend on
    the RLEC proxy being present (which RLTest's OSS-cluster env can't
    provide)."""
    try:
        info = env.getConnection(shardId=1).execute_command('INFO', 'server')
    except Exception:
        return False
    return 'rlec_version' in str(info)

def refreshcluster_registered(env):
    """True iff the module registered `timeseries.REFRESHCLUSTER`.

    LibMR only registers this OSS topology-refresh command when it took the
    OSS code path (clusterCtx.isOss). An enterprise build running as a real
    OSS cluster (cluster-enabled yes, no RLEC proxy) now takes that path too,
    so the command is present and fan-out can be wired via CLUSTER SLOTS. If
    it's absent we're on a binary that still expects the RLEC proxy to manage
    topology, and OSS-cluster fan-out can't be set up."""
    try:
        info = env.getConnection(shardId=1).execute_command(
            'COMMAND', 'INFO', 'timeseries.REFRESHCLUSTER')
    except Exception:
        return False
    # COMMAND INFO returns one entry per requested command; an unknown command
    # yields a nil/empty entry. RESP2 -> [[name, arity, ...]] or [None];
    # RESP3 -> {name: {...}} or {name: None} / {}.
    if isinstance(info, dict):
        entry = info.get(b'timeseries.REFRESHCLUSTER') or info.get('timeseries.REFRESHCLUSTER')
        return bool(entry)
    if isinstance(info, (list, tuple)) and len(info) >= 1:
        return bool(info[0])
    return False

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

    if BIGREDIS_TESTS:
        # Prepend the bigredis config to whatever the test supplied (if any).
        # Later options in the file would override earlier ones, so this lets
        # individual tests still set their own bigredis-* overrides if they
        # need to. Per-shard bigredis-path is appended at cmdline time via
        # the createCmdArgs monkey-patch above — not in this config.
        existing = kwargs.pop('redisConfigFileContent', '')
        kwargs['redisConfigFileContent'] = _BIGREDIS_CONFIG_TEMPLATE + existing

    if 'redisConfigFileContent' in kwargs:
        kwargs['redisConfigFile'] = create_config_file(kwargs['redisConfigFileContent'])
        del kwargs['redisConfigFileContent']

    temp_no_log = Defaults.no_log
    no_capture_output = Defaults.no_capture_output

    if 'noLog' in kwargs:
        Defaults.no_log = kwargs['noLog']
        # Defaults.no_capture_output = True
        del kwargs['noLog']

    skipRefreshCluster = kwargs.pop('skipRefreshCluster', False)

    env = rltestEnv(*args,
                    terminateRetries=Defaults.terminate_retries,
                    terminateRetrySecs=Defaults.terminate_retries_secs,
                    **kwargs)
    Defaults.no_log = temp_no_log
    Defaults.no_capture_output = no_capture_output

    # Skip OSS-cluster variant only when an enterprise-build redis can't wire
    # OSS-cluster fan-out. The TS module detects rlec_version via INFO Server;
    # historically that forced the RLEC-proxy code path even under an OSS
    # cluster RLTest env (no proxy), so `timeseries.REFRESHCLUSTER` was never
    # registered → topology never got set up → multi-key TS.MRANGE / TS.MGET /
    # TS.QUERYINDEX fan-out was broken (each shard only saw its own keys).
    #
    # LibMR now takes the OSS path when the binary actually runs as an OSS
    # cluster (cluster-enabled yes), registering REFRESHCLUSTER. So we gate the
    # skip on a capability probe: if REFRESHCLUSTER is present the fixed binary
    # can form topology and we let the tests run (the REFRESHCLUSTER loop below
    # wires it up). If it's absent we're on an old/unfixed binary → skip
    # cleanly. Real RLEC tests (RLEC_CLUSTER=1) still run end-to-end through
    # the proxy and never reach this branch.
    #
    # Note: env.skip() raises unittest.SkipTest, which RLTest's _runTest
    # treats as a test failure when raised from inside env_factory (the
    # `except Exception` at __main__.py:709 catches it before the skip-
    # aware handler at :727 ever runs). So instead of raising here, we
    # patch the env's first-call entry points to raise SkipTest. The skip
    # fires from inside the test body and gets caught cleanly.
    if env.isCluster() and not RLEC_CLUSTER and is_enterprise(env) and not refreshcluster_registered(env):
        import unittest as _unittest
        _skip_msg = ('OSS cluster + enterprise-build redis (no real RLEC proxy): '
                     'TS multi-key fan-out unsupported in this configuration')
        def _skip_call(*_a, **_kw):
            raise _unittest.SkipTest(_skip_msg)
        for _attr in ('getConnection', 'getClusterConnectionIfNeeded',
                      'cmd', 'expect', 'execute_command'):
            if hasattr(env, _attr):
                setattr(env, _attr, _skip_call)
        return env

    # Hard runtime guarantee that BIGREDIS_TESTS=1 actually wired through.
    # Without this, future refactors that silently break the config injection
    # would let tests run on OSS while the env var advertised Flex coverage.
    if BIGREDIS_TESTS and not RLEC_CLUSTER:
        conn = env.getConnection(0)
        reply = conn.execute_command('CONFIG', 'GET', 'bigredis-enabled')
        # CONFIG GET returns a 2-element list under RESP2, and a 1-entry dict
        # under RESP3 — handle both shapes.
        if isinstance(reply, dict):
            val = reply.get(b'bigredis-enabled') or reply.get('bigredis-enabled')
        elif isinstance(reply, (list, tuple)) and len(reply) >= 2:
            val = reply[1]
        else:
            val = None
        if val not in (b'yes', 'yes'):
            raise RuntimeError(
                "BIGREDIS_TESTS=1 set but redis-server reports "
                f"bigredis-enabled={val!r} — config injection broken, "
                "tests would silently run on OSS instead of Flex")

    if not RLEC_CLUSTER and not skipRefreshCluster:
        for shard in range(0, env.shardsCount):
            conn = env.getConnection(shard)
            modules = conn.execute_command('MODULE', 'LIST')
            if env.protocol == 2:
                if not any(module for module in modules if (module[1] == b'timeseries' or module[1] == 'timeseries')):
                    break
            else:
                if not any(module for module in modules if (module[b'name'] == b'timeseries' or module[b'name'] == 'timeseries')):
                    break
            try:
                conn.execute_command('timeseries.REFRESHCLUSTER')
            except Exception:
                # Not registered on Enterprise/RLEC — RLEC manages cluster
                # topology itself, see deps/LibMR/src/cluster.c:1485.
                pass
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


def _get_worker_thread_names_linux(pid, prefix):
    task_dir = f"/proc/{pid}/task"
    if not os.path.isdir(task_dir):
        return None

    names = []
    for tid in os.listdir(task_dir):
        comm_path = os.path.join(task_dir, tid, "comm")
        try:
            with open(comm_path) as f:
                name = f.read().strip()
        except OSError:
            continue
        if re.fullmatch(re.escape(prefix) + r"\d+", name):
            names.append(name)
    return names

def _get_worker_thread_names_darwin_sample(pid, prefix):
    """Use /usr/bin/sample (1s) to read pthread names; returns None if sampling fails."""
    sample_bin = shutil.which("sample")
    if not sample_bin:
        return None
    r = subprocess.run(
        [sample_bin, str(pid), "1"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if r.returncode != 0:
        return None
    text = r.stdout or ""
    # e.g. "    873 Thread_4791505: timeseries-5"
    pat = re.compile(r"Thread_\d+:\s*(" + re.escape(prefix) + r"\d+)")
    found = pat.findall(text)
    return list(dict.fromkeys(found))


def get_worker_thread_names(conn, prefix="timeseries-"):
    """Return the list of LibMR worker thread names for a Redis server process.

    *conn* is an open Redis connection to the instance to inspect.
    On Linux, reads /proc/<pid>/task/*/comm. On macOS, runs ``sample`` for one
    second and parses its stdout (requires ``/usr/bin/sample``).
    Only includes numbered pool threads (*prefix* + digits), not e.g.
    *prefix* + ``el``. Returns None if thread names cannot be read.
    """
    info = conn.info("server")
    pid = info["process_id"]

    if sys.platform == "linux":
        return _get_worker_thread_names_linux(pid, prefix)

    if sys.platform == "darwin":
        return _get_worker_thread_names_darwin_sample(pid, prefix)

    return None


# Creates a temporary file with the content provided.
# Returns the filepath of the created file.
def create_config_file(content) -> str:
    dir = f"{os.getcwd()}/logs/"
    os.makedirs(dir, exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix='temp-redis-config', delete=False, dir=dir) as f:
        f.write(content.encode())
        return f.name
