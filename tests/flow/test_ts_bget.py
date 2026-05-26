import time
from threading import Thread

from includes import *


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _start_bget(env, key, cursor, count, timeout_ms):
    """Spawn a worker thread that issues TS.BGET on its own connection.

    Returns (thread, slot) where `slot` is mutated by the worker:
        slot["result"]  -> reply payload (or None if it errored / hasn't returned)
        slot["error"]   -> exception text (or None)
        slot["elapsed"] -> wall-clock seconds the BGET call took
    """
    slot = {"result": None, "error": None, "elapsed": None}

    def worker():
        conn = env.getConnection()
        t0 = time.time()
        try:
            slot["result"] = conn.execute_command(
                "TS.BGET", key, cursor, count, timeout_ms
            )
        except Exception as e:
            slot["error"] = str(e)
        finally:
            slot["elapsed"] = time.time() - t0

    t = Thread(target=worker, daemon=True)
    t.start()
    return t, slot


def _seed(r, key, samples):
    assert r.execute_command("TS.CREATE", key)
    for ts, val in samples:
        assert r.execute_command("TS.ADD", key, ts, val) == ts


# ---------------------------------------------------------------------------
# 1. Immediate reply when enough qualifying samples already exist.
# ---------------------------------------------------------------------------

def test_bget_returns_immediately_when_count_already_satisfied():
    env = Env()
    if env.is_cluster():
        env.skip()  # BGET is a single-key primitive; cluster routing is orthogonal.

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0")])

    # cursor=101 -> qualifying = {200, 300}; count=2 is already met,
    # so BGET should NOT block and return synchronously.
    t0 = time.time()
    res = r.execute_command("TS.BGET", "ts", 101, 2, 1000)
    elapsed = time.time() - t0

    env.assertEqual(res, [[200, b"2"], [300, b"3"]])
    # Sanity: nothing close to the 1s timeout.
    env.assertTrue(elapsed < 0.5, message="BGET should be ~instant, got %.3fs" % elapsed)


# ---------------------------------------------------------------------------
# 2. Blocks until SignalKeyAsReady has pushed qualifying-count to >= count.
# ---------------------------------------------------------------------------

def test_bget_blocks_then_unblocks_when_count_is_reached():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0")])

    # cursor=101 -> initial qualifying = {200, 300} = 2 < 5, so BGET blocks.
    # NOTE: to reach count=5 with cursor=101 we need THREE more samples,
    # not two — the sample at ts=100 doesn't qualify because 100 < 101
    # (cursor is an inclusive lower bound: only ts >= cursor qualifies).
    # Each TS.ADD fires SignalKeyAsReady -> reply_cb runs; only the third
    # add commits because that's when qualifying becomes 5.
    t, slot = _start_bget(env, "ts", 101, 5, 10_000)

    # Give the worker enough time to issue BlockClientOnKeys on the server.
    time.sleep(0.3)
    env.assertTrue(t.is_alive(), message="BGET should be blocked (2 < 5 qualifying)")

    # Add #1: qualifying = {200, 300, 400} = 3 < 5 -> stay blocked.
    assert r.execute_command("TS.ADD", "ts", 400, "4.0") == 400
    time.sleep(0.2)
    env.assertTrue(t.is_alive(),
                   message="BGET should still be blocked after 1st add (3 < 5)")

    # Add #2: qualifying = {200, 300, 400, 500} = 4 < 5 -> stay blocked.
    assert r.execute_command("TS.ADD", "ts", 500, "5.0") == 500
    time.sleep(0.2)
    env.assertTrue(t.is_alive(),
                   message="BGET should still be blocked after 2nd add (4 < 5)")

    # Add #3: qualifying = {200, 300, 400, 500, 600} = 5 -> reply_cb commits.
    assert r.execute_command("TS.ADD", "ts", 600, "6.0") == 600

    t.join(timeout=2.0)
    env.assertFalse(t.is_alive(),
                    message="BGET should have replied once qualifying >= count")

    env.assertEqual(slot["error"], None)
    env.assertEqual(
        slot["result"],
        [[200, b"2"], [300, b"3"], [400, b"4"], [500, b"5"], [600, b"6"]],
    )
    # Sanity: well below the 10s timeout — we unblocked on the signal, not on timeout.
    env.assertTrue(slot["elapsed"] < 5.0,
                   message="BGET took %.2fs, should have unblocked promptly" % slot["elapsed"])


# ---------------------------------------------------------------------------
# 3. Oldest-first paging: when MORE than `count` samples qualify, return the
#    oldest `count` (so callers can advance the cursor to the last returned ts).
# ---------------------------------------------------------------------------

def test_bget_returns_oldest_count_when_more_qualify():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [
        (100, "1.0"), (200, "2.0"), (300, "3.0"),
        (400, "4.0"), (500, "5.0"),
    ])

    # cursor=101 -> 4 qualifying (200, 300, 400, 500); count=2 -> take the OLDEST 2.
    res = r.execute_command("TS.BGET", "ts", 101, 2, 1000)
    env.assertEqual(res, [[200, b"2"], [300, b"3"]])

    # The caller pages forward by setting cursor to last_returned_ts + 1
    # (cursor is inclusive: ts >= cursor qualifies, so passing 300 would
    # re-emit the sample we already saw).
    res = r.execute_command("TS.BGET", "ts", 301, 2, 1000)
    env.assertEqual(res, [[400, b"4"], [500, b"5"]])


# ---------------------------------------------------------------------------
# 4. Timeout flush: client blocks, no producer feeds it, timeout fires and we
#    flush whatever is available (which may be fewer than count).
# ---------------------------------------------------------------------------

def test_bget_timeout_flushes_available_samples():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [
        (100, "1.0"), (200, "2.0"), (300, "3.0"),
        (400, "4.0"), (500, "5.0"),
    ])

    # cursor=101 -> 4 qualifying; count=10 cannot be reached. Nobody else adds.
    # After timeout_ms expires the timeout_cb flushes the 4 available samples.
    t0 = time.time()
    res = r.execute_command("TS.BGET", "ts", 101, 10, 1000)
    elapsed = time.time() - t0

    env.assertEqual(
        res,
        [[200, b"2"], [300, b"3"], [400, b"4"], [500, b"5"]],
    )
    # The whole timeout should have elapsed (we expected to be blocked).
    env.assertTrue(elapsed >= 0.9,
                   message="Expected to wait ~1s on timeout, only waited %.3fs" % elapsed)
    env.assertTrue(elapsed < 5.0,
                   message="Timeout overshoot too large: %.3fs" % elapsed)


# ---------------------------------------------------------------------------
# 5. '-' sentinel: cursor=0, matches every existing sample.
# ---------------------------------------------------------------------------

def test_bget_dash_returns_all_existing_samples():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0")])

    # '-' resolves statically to cursor=0; with ts >= 0 every sample qualifies,
    # so count=3 is already met and BGET returns synchronously.
    res = r.execute_command("TS.BGET", "ts", "-", 3, 5000)
    env.assertEqual(res, [[100, b"1"], [200, b"2"], [300, b"3"]])


# ---------------------------------------------------------------------------
# 6. '-' on a missing key: cursor=0, blocks until the first qualifying ADD.
# ---------------------------------------------------------------------------

def test_bget_dash_on_empty_series_blocks_until_first_add():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    r.execute_command("FLUSHALL")  # key "ts" must not exist for this scenario.

    t, slot = _start_bget(env, "ts", "-", 1, 10_000)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(),
                   message="BGET '-' on missing key should block until ADD")

    assert r.execute_command("TS.ADD", "ts", 100, "1.0") == 100

    t.join(timeout=2.0)
    env.assertFalse(t.is_alive(),
                    message="BGET should have replied once the first sample arrived")
    env.assertEqual(slot["error"], None)
    env.assertEqual(slot["result"], [[100, b"1"]])


# ---------------------------------------------------------------------------
# 7. '+' sentinel on an empty / missing key: cursor=0, behaves like '-'.
# ---------------------------------------------------------------------------

def test_bget_plus_on_empty_series_blocks_until_first_add():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    r.execute_command("FLUSHALL")

    t, slot = _start_bget(env, "ts", "+", 1, 10_000)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(),
                   message="BGET '+' on missing key should block until ADD")

    assert r.execute_command("TS.ADD", "ts", 100, "1.0") == 100

    t.join(timeout=2.0)
    env.assertFalse(t.is_alive())
    env.assertEqual(slot["error"], None)
    env.assertEqual(slot["result"], [[100, b"1"]])


# ---------------------------------------------------------------------------
# 8. '+' sentinel: snapshot lastTs at command time, only NEW samples qualify.
# ---------------------------------------------------------------------------

def test_bget_plus_only_returns_samples_added_after_command():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0")])

    # '+' resolves now to lastTs + 1 = 301. Existing 100/200/300 must NOT
    # qualify; only a future ADD with ts >= 301 should wake us.
    t, slot = _start_bget(env, "ts", "+", 1, 10_000)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(),
                   message="BGET '+' should ignore pre-existing samples")

    # Producer adds a strictly newer sample -> wake-up.
    assert r.execute_command("TS.ADD", "ts", 400, "4.0") == 400

    t.join(timeout=2.0)
    env.assertFalse(t.is_alive())
    env.assertEqual(slot["error"], None)
    env.assertEqual(slot["result"], [[400, b"4"]])


# ---------------------------------------------------------------------------
# 9. '+' sentinel without producer: existing samples must NEVER be returned;
#    on timeout we flush an empty array.
# ---------------------------------------------------------------------------

def test_bget_plus_with_no_new_samples_times_out_empty():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0")])

    t0 = time.time()
    res = r.execute_command("TS.BGET", "ts", "+", 1, 1000)
    elapsed = time.time() - t0

    env.assertEqual(res, [])
    env.assertTrue(elapsed >= 0.9,
                   message="Expected to wait the full timeout, only %.3fs" % elapsed)
    env.assertTrue(elapsed < 5.0,
                   message="Timeout overshoot too large: %.3fs" % elapsed)
