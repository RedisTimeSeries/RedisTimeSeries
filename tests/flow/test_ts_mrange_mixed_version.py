"""
Mixed-version cluster backward-compatibility tests for TS.MRANGE pre-aggregation.

Two separate builds happen once per process (in parallel threads) and are cached:
  1. Redis 8.8.0 redis-server binary  — used for "old" nodes.
  2. Old module .so from origin/master — loaded by the 8.8 redis-server.

The "new" coord node uses the module already built by the CI at _ROOT and the
unstable redis-server binary that's already in PATH.

Scenarios
---------
a. Both coord and shard run OLD module on Redis 8.8.
     Coordinator sends TS.INTERNAL_MRANGE (no shard pre-agg); coordinator aggregates.
     TS.MRANGE with AGGREGATION must return correct results.

b. Coordinator runs NEW module on unstable redis-server; shard runs OLD module on Redis 8.8.
     Coordinator dispatches TS.INTERNAL_MRANGE_AGG; old shard does not have that command.
     TS.MRANGE must return an error.

RLTest convention: all test functions accept env as first positional argument.
Tests skip via env.skip() when not running in cluster mode.
"""
import atexit
import multiprocessing
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time

import redis

_ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

_OLD_MODULE_ENV = os.environ.get("OLD_MODULE_PATH", "")
_OLD_REDIS_ENV = os.environ.get("OLD_REDIS_SERVER", "")

# Session-level cache — populated once, reused across tests.
_old_module_path = None
_old_module_worktree = None
_old_redis_bin = None
_old_redis_dir = None
_setup_lock = threading.Lock()
_setup_error = None   # RuntimeError set if the parallel build failed


# ---------------------------------------------------------------------------
# Module / binary discovery helpers
# ---------------------------------------------------------------------------

def _find_so(base):
    for plat in ("linux-x86_64-release", "linux-arm64v8-release", "macos-arm64v8-release"):
        p = os.path.join(base, "bin", plat, "redistimeseries.so")
        if os.path.exists(p):
            return p
    return None


NEW_MODULE = _find_so(_ROOT)


def _remove_worktree(path):
    try:
        subprocess.run(
            ["git", "worktree", "remove", "--force", path],
            cwd=_ROOT, capture_output=True,
        )
    except Exception:
        pass
    shutil.rmtree(path, ignore_errors=True)


# ---------------------------------------------------------------------------
# Build Redis 8.8
# ---------------------------------------------------------------------------

_REDIS_OLD_TAG = "8.8.0"


def _build_old_redis():
    """
    Build Redis 8.8.0 from source.  Returns path to the redis-server binary.
    Caches the result in _old_redis_bin; raises RuntimeError on failure.
    """
    global _old_redis_bin, _old_redis_dir

    if _old_redis_bin:
        return _old_redis_bin

    if _OLD_REDIS_ENV:
        if not os.path.isfile(_OLD_REDIS_ENV):
            raise RuntimeError(f"OLD_REDIS_SERVER={_OLD_REDIS_ENV!r} is not a file")
        _old_redis_bin = _OLD_REDIS_ENV
        return _old_redis_bin

    tmpdir = tempfile.mkdtemp(prefix="redis_88_")
    try:
        subprocess.run(
            ["git", "clone", "--depth=1", "--branch", _REDIS_OLD_TAG,
             "https://github.com/redis/redis.git", tmpdir],
            check=True, capture_output=True, timeout=300,
        )
        jobs = max(1, multiprocessing.cpu_count() - 1)
        subprocess.run(
            ["make", f"-j{jobs}"],
            cwd=tmpdir, check=True, capture_output=True, timeout=600,
        )
    except Exception as e:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise RuntimeError(f"Could not build Redis {_REDIS_OLD_TAG}: {e}")

    binary = os.path.join(tmpdir, "src", "redis-server")
    if not os.path.isfile(binary):
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise RuntimeError(f"redis-server not found after building Redis {_REDIS_OLD_TAG}")

    _old_redis_bin = binary
    _old_redis_dir = tmpdir
    atexit.register(shutil.rmtree, tmpdir, True)
    return _old_redis_bin


# ---------------------------------------------------------------------------
# Build origin/master module
# ---------------------------------------------------------------------------

def _build_old_module():
    """
    Build the TimeSeries module from origin/master.  Returns path to .so.
    Origin/master's own deps/RedisModulesSDK headers are for the 8.x era and
    are compatible with the Redis 8.8 binary — no header patching needed.
    Caches the result in _old_module_path; raises RuntimeError on failure.
    """
    global _old_module_path, _old_module_worktree

    if _old_module_path:
        return _old_module_path

    if _OLD_MODULE_ENV:
        if not os.path.isfile(_OLD_MODULE_ENV):
            raise RuntimeError(f"OLD_MODULE_PATH={_OLD_MODULE_ENV!r} is not a file")
        _old_module_path = _OLD_MODULE_ENV
        return _old_module_path

    worktree = tempfile.mkdtemp(prefix="ts_old_master_")
    try:
        subprocess.run(
            ["git", "fetch", "--depth=1", "origin", "master"],
            cwd=_ROOT, check=True, capture_output=True, timeout=120,
        )
        subprocess.run(
            ["git", "worktree", "add", "--detach", worktree, "origin/master"],
            cwd=_ROOT, check=True, capture_output=True,
        )
        subprocess.run(
            ["git", "submodule", "update", "--init", "--recursive"],
            cwd=worktree, check=True, capture_output=True, timeout=120,
        )
        jobs = max(1, multiprocessing.cpu_count() - 1)
        subprocess.run(
            ["make", "build", f"-j{jobs}"],
            cwd=worktree, check=True, capture_output=True, timeout=900,
        )
    except Exception as e:
        _remove_worktree(worktree)
        raise RuntimeError(f"Could not build old module from origin/master: {e}")

    so = _find_so(worktree)
    if not so:
        _remove_worktree(worktree)
        raise RuntimeError("origin/master .so not found after build")

    _old_module_path = so
    _old_module_worktree = worktree
    atexit.register(_remove_worktree, worktree)
    return _old_module_path


# ---------------------------------------------------------------------------
# Parallel setup: build Redis 8.8 + old module concurrently
# ---------------------------------------------------------------------------

def _ensure_old_setup():
    """
    Ensure Redis 8.8 binary and old module .so are ready.
    Runs both builds in parallel threads; caches results for subsequent calls.
    Raises RuntimeError (stored in _setup_error) if either build fails.
    """
    global _setup_error

    with _setup_lock:
        # Already succeeded
        if _old_redis_bin and _old_module_path:
            return
        # Already failed
        if _setup_error:
            raise _setup_error

        errors = []

        def _build_redis_thread():
            try:
                _build_old_redis()
            except RuntimeError as e:
                errors.append(e)

        def _build_module_thread():
            try:
                _build_old_module()
            except RuntimeError as e:
                errors.append(e)

        t1 = threading.Thread(target=_build_redis_thread, daemon=True)
        t2 = threading.Thread(target=_build_module_thread, daemon=True)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        if errors:
            _setup_error = errors[0]
            raise _setup_error


# ---------------------------------------------------------------------------
# Cluster helpers
# ---------------------------------------------------------------------------

def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _start_node(port, module_path, tmpdir, redis_bin="redis-server"):
    proc = subprocess.Popen(
        [
            redis_bin,
            "--port", str(port),
            "--cluster-enabled", "yes",
            "--cluster-config-file", os.path.join(tmpdir, f"nodes-{port}.conf"),
            "--cluster-node-timeout", "5000",
            "--loadmodule", module_path,
            "--save", "",
            "--stop-writes-on-bgsave-error", "no",
            "--loglevel", "warning",
            "--logfile", os.path.join(tmpdir, f"{port}.log"),
            "--dir", tmpdir,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    r = redis.Redis(host="127.0.0.1", port=port, socket_connect_timeout=1)
    for _ in range(100):
        try:
            r.ping()
            return proc, r
        except Exception:
            time.sleep(0.1)
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except Exception:
        proc.kill()
    log_path = os.path.join(tmpdir, f"{port}.log")
    try:
        with open(log_path) as f:
            log_tail = f.read()[-800:]
    except Exception:
        log_tail = "(no log)"
    raise RuntimeError(
        f"Redis did not start on port {port} "
        f"(bin={os.path.basename(redis_bin)}, mod={os.path.basename(module_path)}). "
        f"Log tail:\n{log_tail}"
    )


def _form_cluster(r_coord, coord_port, r_shard, shard_port):
    """Form a 2-node cluster: coord owns slots 0-8191; shard owns 8192-16383."""
    r_coord.execute_command("CLUSTER", "MEET", "127.0.0.1", str(shard_port))
    time.sleep(0.5)
    r_coord.execute_command("CLUSTER", "ADDSLOTSRANGE", 0, 8191)
    r_shard.execute_command("CLUSTER", "ADDSLOTSRANGE", 8192, 16383)
    deadline = time.time() + 15.0
    while time.time() < deadline:
        try:
            ok_coord = "cluster_state:ok" in r_coord.execute_command("CLUSTER", "INFO").decode()
            ok_shard = "cluster_state:ok" in r_shard.execute_command("CLUSTER", "INFO").decode()
            if ok_coord and ok_shard:
                for conn in (r_coord, r_shard):
                    try:
                        conn.execute_command("timeseries.REFRESHCLUSTER")
                    except Exception:
                        pass
                return
        except Exception:
            pass
        time.sleep(0.25)
    raise RuntimeError("Cluster did not reach ok state within 15s")


def _key_in_slots(r, prefix, lo, hi):
    """Return first 'prefix_N' key whose hash slot is in [lo, hi)."""
    for i in range(10000):
        key = f"{prefix}_{i}"
        if lo <= int(r.execute_command("CLUSTER", "KEYSLOT", key)) < hi:
            return key
    raise RuntimeError(f"No key for prefix '{prefix}' in slot range [{lo}, {hi})")


def _populate(r_coord, r_shard, label):
    """Create TS series on BOTH shards so cross-shard LibMR is exercised."""
    coord_keys = [_key_in_slots(r_coord, f"ts_{label}_c{i}", 0, 8192) for i in range(2)]
    shard_keys = [_key_in_slots(r_shard, f"ts_{label}_s{i}", 8192, 16384) for i in range(2)]
    for r, keys in ((r_coord, coord_keys), (r_shard, shard_keys)):
        for key in keys:
            r.execute_command("TS.CREATE", key, "LABELS", "group", label)
            for t in range(10, 200, 10):
                r.execute_command("TS.ADD", key, t, t // 10)
    return coord_keys + shard_keys


def _stop_procs(procs):
    for p in procs:
        p.terminate()
        try:
            p.wait(timeout=5)
        except Exception:
            p.kill()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
# RLTest calls test functions as test(env). env.skip() signals a skip.

def test_old_coordinator_old_shard_aggregation_correct(env):
    """
    Scenario a: Redis 8.8 + old module on both nodes.
    Coordinator sends TS.INTERNAL_MRANGE (no shard pre-agg) and aggregates at coordinator.
    Result must be correct — verifies the old code path is not broken by our changes.
    """
    if not env.is_cluster():
        env.skip()

    try:
        _ensure_old_setup()
    except RuntimeError:
        env.skip()

    old_so = _old_module_path
    old_bin = _old_redis_bin
    tmpdir = tempfile.mkdtemp(prefix="ts_mixed_a_")
    procs = []
    try:
        coord_port, shard_port = _free_port(), _free_port()
        p1, r_coord = _start_node(coord_port, old_so, tmpdir, redis_bin=old_bin)
        p2, r_shard = _start_node(shard_port, old_so, tmpdir, redis_bin=old_bin)
        procs = [p1, p2]
        _form_cluster(r_coord, coord_port, r_shard, shard_port)
        _populate(r_coord, r_shard, "grp_a")

        result = r_coord.execute_command(
            "TS.MRANGE", 10, 190, "AGGREGATION", "SUM", 50, "FILTER", "group=grp_a"
        )

        print(f"\nScenario a response:\n{result}")
        assert result is not None
        assert len(result) == 4, f"Expected 4 series (2 per shard), got {len(result)}"
        for _key, _labels, samples in result:
            assert len(samples) > 0, "Expected aggregated samples, got none"
            for ts, _val in samples:
                assert ts % 50 == 0, f"Bucket timestamp {ts} not aligned to bucket size 50"
    finally:
        _stop_procs(procs)
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_new_coordinator_old_shard_returns_error(env):
    """
    Scenario b: new coordinator (unstable redis-server + NEW module) + old shard (Redis 8.8 + OLD module).
    Coordinator dispatches TS.INTERNAL_MRANGE_AGG; old shard does not have that command.
    TS.MRANGE must return an error — not a partial or silent result.
    """
    if not env.is_cluster():
        env.skip()

    if not NEW_MODULE:
        env.skip()

    try:
        _ensure_old_setup()
    except RuntimeError:
        env.skip()

    old_so = _old_module_path
    old_bin = _old_redis_bin
    tmpdir = tempfile.mkdtemp(prefix="ts_mixed_b_")
    procs = []
    try:
        coord_port, shard_port = _free_port(), _free_port()
        # coord: CI's unstable redis-server + our new module
        p1, r_coord = _start_node(coord_port, NEW_MODULE, tmpdir)
        # shard: Redis 8.8 redis-server + origin/master module (no TS.INTERNAL_MRANGE_AGG)
        p2, r_shard = _start_node(shard_port, old_so, tmpdir, redis_bin=old_bin)
        procs = [p1, p2]
        _form_cluster(r_coord, coord_port, r_shard, shard_port)
        _populate(r_coord, r_shard, "grp_b")

        error_raised = False
        try:
            result = r_coord.execute_command(
                "TS.MRANGE", 10, 190, "AGGREGATION", "SUM", 50, "FILTER", "group=grp_b"
            )
            print(f"\nScenario b response (expected error, got): {result}")
        except redis.exceptions.ResponseError as e:
            error_raised = True
            print(f"\nScenario b error response (expected):\n{e}")
        assert error_raised, "Expected ResponseError but TS.MRANGE succeeded"
    finally:
        _stop_procs(procs)
        shutil.rmtree(tmpdir, ignore_errors=True)
