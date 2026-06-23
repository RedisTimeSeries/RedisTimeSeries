"""
Mixed-version cluster backward-compatibility tests for TS.MRANGE pre-aggregation.

The "old" module (without TS.INTERNAL_MRANGE_AGG) is acquired automatically:
  1. If OLD_MODULE_PATH env var is set, that path is used directly.
  2. Otherwise origin/master is built in a temporary git worktree (once per session).

Scenarios
---------
a. Old coordinator + old shard:
     coordinator sends TS.INTERNAL_MRANGE (no shard pre-agg), aggregates at coordinator.
     TS.MRANGE result must be correct.

b. New coordinator + old shard:
     coordinator sends TS.INTERNAL_MRANGE_AGG; old shard does not have that command.
     TS.MRANGE must return an error.

RLTest calls every test function as test(env) — env is RLTest's Env object.
Tests skip via env.skip() when not running in cluster mode.
"""
import atexit
import multiprocessing
import os
import shutil
import socket
import subprocess
import tempfile
import time

import redis

_ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
_OLD_MODULE_ENV = os.environ.get("OLD_MODULE_PATH", "")

# Session-level cache so the master build runs at most once.
_old_module_path = None
_old_module_worktree = None


def _find_so(base):
    for plat in ("linux-x86_64-release", "linux-arm64v8-release", "macos-arm64v8-release"):
        p = os.path.join(base, "bin", plat, "redistimeseries.so")
        if os.path.exists(p):
            return p
    return None


NEW_MODULE = _find_so(_ROOT)


def _remove_worktree(worktree):
    try:
        subprocess.run(
            ["git", "worktree", "remove", "--force", worktree],
            cwd=_ROOT, capture_output=True,
        )
    except Exception:
        pass
    shutil.rmtree(worktree, ignore_errors=True)


def _ensure_old_module():
    """
    Return the path to the old module .so, building from origin/master if needed.
    Raises RuntimeError if the module cannot be acquired (caller converts to env.skip()).
    """
    global _old_module_path, _old_module_worktree

    if _old_module_path:
        return _old_module_path

    if _OLD_MODULE_ENV:
        if not os.path.exists(_OLD_MODULE_ENV):
            raise RuntimeError(f"OLD_MODULE_PATH={_OLD_MODULE_ENV!r} does not exist")
        _old_module_path = _OLD_MODULE_ENV
        return _old_module_path

    # Auto-build from origin/master in a temporary worktree.
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
        raise RuntimeError("Built old module .so not found in worktree")

    _old_module_path = so
    _old_module_worktree = worktree
    atexit.register(_remove_worktree, worktree)
    return _old_module_path


# ---- cluster helpers ----

def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _start_node(port, module_path, tmpdir):
    proc = subprocess.Popen(
        [
            "redis-server",
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
    raise RuntimeError(f"Redis did not start on port {port}")


def _form_cluster(r_coord, coord_port, r_shard, shard_port):
    """
    Form a 2-node cluster.
    coord owns slots 0-8191; shard owns 8192-16383.
    """
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
    """Return the first 'prefix_N' key whose hash slot is in [lo, hi)."""
    for i in range(10000):
        key = f"{prefix}_{i}"
        slot = int(r.execute_command("CLUSTER", "KEYSLOT", key))
        if lo <= slot < hi:
            return key
    raise RuntimeError(f"No key for prefix '{prefix}' found in slot range [{lo}, {hi})")


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


# ---- tests ----
# RLTest calls test functions as test(env). env.skip() signals a skip to the runner.

def test_old_coordinator_old_shard_aggregation_correct(env):
    """
    Scenario a: old coordinator + old shards (no TS.INTERNAL_MRANGE_AGG on either side).
    Coordinator sends TS.INTERNAL_MRANGE, aggregates raw shard data itself.
    Result must be correct — verifies the old code path is not broken by our changes.
    """
    if not env.is_cluster():
        env.skip()

    try:
        old = _ensure_old_module()
    except RuntimeError:
        env.skip()

    tmpdir = tempfile.mkdtemp(prefix="ts_mixed_a_")
    procs = []
    try:
        coord_port, shard_port = _free_port(), _free_port()
        p1, r_coord = _start_node(coord_port, old, tmpdir)
        p2, r_shard = _start_node(shard_port, old, tmpdir)
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
                assert ts % 50 == 0, f"Bucket timestamp {ts} is not aligned to bucket size 50"
    finally:
        _stop_procs(procs)
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_new_coordinator_old_shard_returns_error(env):
    """
    Scenario b: new coordinator sends TS.INTERNAL_MRANGE_AGG; old shard does not have it.
    TS.MRANGE must return an error — not a partial or silent result.
    """
    if not env.is_cluster():
        env.skip()

    if not NEW_MODULE:
        env.skip()

    try:
        old = _ensure_old_module()
    except RuntimeError:
        env.skip()

    tmpdir = tempfile.mkdtemp(prefix="ts_mixed_b_")
    procs = []
    try:
        coord_port, shard_port = _free_port(), _free_port()
        p1, r_coord = _start_node(coord_port, NEW_MODULE, tmpdir)
        p2, r_shard = _start_node(shard_port, old, tmpdir)
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
