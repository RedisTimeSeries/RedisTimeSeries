import time
from threading import Thread

from includes import *


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bget_argv(key, cursor, timeout_ms, min_count=None, max_count=None):
    """Build a TS.BGET argument list for the
    `key timestamp timeout [MIN_COUNT n] [MAX_COUNT n]` syntax."""
    argv = [key, cursor, timeout_ms]
    if min_count is not None:
        argv += ["MIN_COUNT", min_count]
    if max_count is not None:
        argv += ["MAX_COUNT", max_count]
    return argv


def _start_bget(env, key, cursor, timeout_ms, min_count=None, max_count=None):
    """Spawn a worker thread that issues TS.BGET on its own connection.

    Returns (thread, slot) where `slot` is mutated by the worker:
        slot["result"]  -> reply payload (or None if it errored / hasn't returned)
        slot["error"]   -> exception text (or None)
        slot["elapsed"] -> wall-clock seconds the BGET call took
    """
    slot = {"result": None, "error": None, "elapsed": None}
    argv = _bget_argv(key, cursor, timeout_ms, min_count, max_count)

    def worker():
        conn = env.getConnection()
        # Cap the socket read so a stuck server-side BGET cannot keep the
        # thread (daemon or not) alive past a short, predictable bound.
        # Slightly above timeout_ms to let the server's own timeout fire
        # first under normal conditions.
        try:
            conn.connection_pool.connection_kwargs["socket_timeout"] = \
                max(1.0, (timeout_ms / 1000.0) + 1.0)
        except Exception:
            pass
        t0 = time.time()
        try:
            slot["result"] = conn.execute_command("TS.BGET", *argv)
        except Exception as e:
            slot["error"] = str(e)
        finally:
            slot["elapsed"] = time.time() - t0
            try:
                conn.close()
            except Exception:
                pass

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

def test_bget_returns_immediately_when_min_count_already_satisfied():
    env = Env()
    if env.is_cluster():
        env.skip()  # BGET is a single-key primitive; cluster routing is orthogonal.

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0")])

    # cursor=101 -> qualifying = {200, 300}; MIN_COUNT 2 is already met, so BGET
    # should NOT block and return synchronously (MAX_COUNT 2 caps the reply).
    t0 = time.time()
    res = r.execute_command("TS.BGET", "ts", 101, 1000, "MIN_COUNT", 2, "MAX_COUNT", 2)
    elapsed = time.time() - t0

    env.assertEqual(res, [[200, b"2"], [300, b"3"]])
    # Sanity: nothing close to the 1s timeout.
    env.assertTrue(elapsed < 0.5, message="BGET should be ~instant, got %.3fs" % elapsed)


# ---------------------------------------------------------------------------
# 2. Blocks until SignalKeyAsReady has pushed qualifying-count to >= min_count.
# ---------------------------------------------------------------------------

def test_bget_blocks_then_unblocks_when_min_count_is_reached():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0")])

    # cursor=101 -> initial qualifying = {200, 300} = 2 < 5, so BGET blocks.
    # NOTE: to reach min_count=5 with cursor=101 we need THREE more samples,
    # not two — the sample at ts=100 doesn't qualify because 100 < 101
    # (cursor is an inclusive lower bound: only ts >= cursor qualifies).
    # Each TS.ADD fires SignalKeyAsReady -> reply_cb runs; only the third
    # add commits because that's when qualifying becomes 5.
    t, slot = _start_bget(env, "ts", 101, 2_000, min_count=5, max_count=5)

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
                    message="BGET should have replied once qualifying >= min_count")

    env.assertEqual(slot["error"], None)
    env.assertEqual(
        slot["result"],
        [[200, b"2"], [300, b"3"], [400, b"4"], [500, b"5"], [600, b"6"]],
    )
    # Sanity: well below the 10s timeout — we unblocked on the signal, not on timeout.
    env.assertTrue(slot["elapsed"] < 5.0,
                   message="BGET took %.2fs, should have unblocked promptly" % slot["elapsed"])


# ---------------------------------------------------------------------------
# 3. Oldest-first paging: when MORE than `max_count` samples qualify, return the
#    oldest `max_count` (so callers can advance the cursor to the last returned ts).
# ---------------------------------------------------------------------------

def test_bget_returns_oldest_max_count_when_more_qualify():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [
        (100, "1.0"), (200, "2.0"), (300, "3.0"),
        (400, "4.0"), (500, "5.0"),
    ])

    # cursor=101 -> 4 qualifying (200, 300, 400, 500); MAX_COUNT 2 -> take the OLDEST 2.
    res = r.execute_command("TS.BGET", "ts", 101, 1000, "MAX_COUNT", 2)
    env.assertEqual(res, [[200, b"2"], [300, b"3"]])

    # The caller pages forward by setting cursor to last_returned_ts + 1
    # (cursor is inclusive: ts >= cursor qualifies, so passing 300 would
    # re-emit the sample we already saw).
    res = r.execute_command("TS.BGET", "ts", 301, 1000, "MAX_COUNT", 2)
    env.assertEqual(res, [[400, b"4"], [500, b"5"]])


# ---------------------------------------------------------------------------
# 3b. MIN_COUNT vs MAX_COUNT divergence: wait for min_count, then return the
#     oldest max_count (which may be fewer than what qualifies).
# ---------------------------------------------------------------------------

def test_bget_min_below_max_returns_oldest_max_count():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [
        (100, "1.0"), (200, "2.0"), (300, "3.0"),
        (400, "4.0"), (500, "5.0"), (600, "6.0"),
    ])

    # cursor=0 -> 6 qualify; MIN_COUNT 2 already met, MAX_COUNT 4 caps to oldest 4.
    res = r.execute_command("TS.BGET", "ts", 0, 1000, "MIN_COUNT", 2, "MAX_COUNT", 4)
    env.assertEqual(res, [[100, b"1"], [200, b"2"], [300, b"3"], [400, b"4"]])


# ---------------------------------------------------------------------------
# 3d. Blocking path with MIN_COUNT < MAX_COUNT: the wake-up gate uses min_count,
#     but the committed reply is still capped to the oldest max_count. A single
#     TS.MADD lands all samples before the wake-up callback runs, so qualifying
#     jumps past max_count in one shot — exercising gate vs cap in the async
#     (reply-callback) path, not just the synchronous fast path (test 3b).
# ---------------------------------------------------------------------------

def test_bget_blocking_min_gate_max_cap():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0")])

    # cursor = 101 (literal), so the existing sample at 100 doesn't qualify and
    # BGET blocks (0 < MIN_COUNT 2). MAX_COUNT 3 caps whatever we eventually return.
    t, slot = _start_bget(env, "ts", 101, 2_000, min_count=2, max_count=3)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(), message="BGET should block until min_count qualifies")

    # One MADD adds 5 new qualifying samples atomically; the wake-up sees 5 >= 2
    # (gate satisfied) and must return the OLDEST 3 (cap), not all 5.
    assert r.execute_command(
        "TS.MADD",
        "ts", 200, "2.0",
        "ts", 300, "3.0",
        "ts", 400, "4.0",
        "ts", 500, "5.0",
        "ts", 600, "6.0",
    ) == [200, 300, 400, 500, 600]

    t.join(timeout=2.0)
    env.assertFalse(t.is_alive(), message="BGET should have unblocked once min_count qualified")
    env.assertEqual(slot["error"], None)
    env.assertEqual(slot["result"], [[200, b"2"], [300, b"3"], [400, b"4"]])


# ---------------------------------------------------------------------------
# 3e. Keyword matching is case-insensitive (house-style RMUtil_ArgIndex).
# ---------------------------------------------------------------------------

def test_bget_count_keywords_are_case_insensitive():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [
        (100, "1.0"), (200, "2.0"), (300, "3.0"), (400, "4.0"),
    ])

    # cursor=0 -> 4 qualify; lower/mixed-case keywords must behave like uppercase.
    res = r.execute_command("TS.BGET", "ts", 0, 1000, "min_count", 1, "Max_Count", 2)
    env.assertEqual(res, [[100, b"1"], [200, b"2"]])


# ---------------------------------------------------------------------------
# 3c. Defaults: no MIN_COUNT/MAX_COUNT -> min_count=1, max_count=unlimited.
# ---------------------------------------------------------------------------

def test_bget_defaults_min_one_max_unlimited():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0")])

    # min_count defaults to 1 (already met), max_count defaults to unlimited
    # so every qualifying sample is returned.
    res = r.execute_command("TS.BGET", "ts", 0, 1000)
    env.assertEqual(res, [[100, b"1"], [200, b"2"], [300, b"3"]])


# ---------------------------------------------------------------------------
# 4. Timeout flush: client blocks, no producer feeds it, timeout fires and we
#    flush whatever is available (which may be fewer than min_count).
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

    # cursor=101 -> 4 qualifying; MIN_COUNT 10 cannot be reached. Nobody else adds.
    # After timeout_ms expires the timeout_cb flushes the 4 available samples.
    t0 = time.time()
    res = r.execute_command("TS.BGET", "ts", 101, 1000, "MIN_COUNT", 10)
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
    # so MIN_COUNT 3 is already met and BGET returns synchronously.
    res = r.execute_command("TS.BGET", "ts", "-", 5000, "MIN_COUNT", 3, "MAX_COUNT", 3)
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

    t, slot = _start_bget(env, "ts", "-", 2_000)
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

    t, slot = _start_bget(env, "ts", "+", 2_000)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(),
                   message="BGET '+' on missing key should block until ADD")

    assert r.execute_command("TS.ADD", "ts", 100, "1.0") == 100

    t.join(timeout=2.0)
    env.assertFalse(t.is_alive())
    env.assertEqual(slot["error"], None)
    env.assertEqual(slot["result"], [[100, b"1"]])


# ---------------------------------------------------------------------------
# 8. '+' sentinel: resolves to the latest sample's timestamp (aligned with
#    TS.RANGE). Because the cursor is inclusive, the latest EXISTING sample
#    qualifies, so the first call returns it immediately without blocking.
# ---------------------------------------------------------------------------

def test_bget_plus_returns_latest_existing_sample():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0")])

    # '+' resolves to lastTs = 300. With inclusive ts >= 300, only the latest
    # sample qualifies; min_count defaults to 1 so it returns synchronously.
    t0 = time.time()
    res = r.execute_command("TS.BGET", "ts", "+", 1000)
    elapsed = time.time() - t0

    env.assertEqual(res, [[300, b"3"]])
    env.assertTrue(elapsed < 0.5, message="BGET '+' should be ~instant, got %.3fs" % elapsed)


# ---------------------------------------------------------------------------
# 9. Paging continuation: after consuming via '+', a follow-up call from
#    (last retrieved ts + 1) has nothing newer and times out with an empty reply.
# ---------------------------------------------------------------------------

def test_bget_paging_past_latest_times_out_empty():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0")])

    # Caller already consumed up to ts=300 and pages forward to 301; no sample
    # has ts >= 301, so the call blocks and flushes empty on timeout.
    t0 = time.time()
    res = r.execute_command("TS.BGET", "ts", 301, 1000)
    elapsed = time.time() - t0

    env.assertEqual(res, [])
    env.assertTrue(elapsed >= 0.9,
                   message="Expected to wait the full timeout, only %.3fs" % elapsed)
    env.assertTrue(elapsed < 5.0,
                   message="Timeout overshoot too large: %.3fs" % elapsed)


# ---------------------------------------------------------------------------
# 9b. '$' sentinel: resolves to lastTs + 1, so the latest EXISTING sample is
#     excluded and only samples reported after the command qualify. The first
#     call blocks until a strictly-newer sample arrives.
# ---------------------------------------------------------------------------

def test_bget_dollar_only_returns_samples_added_after_command():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0")])

    # '$' resolves to lastTs + 1 = 301. Existing 100/200/300 must NOT qualify;
    # only a future ADD with ts >= 301 should wake us.
    t, slot = _start_bget(env, "ts", "$", 2_000)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(),
                   message="BGET '$' should ignore pre-existing samples and block")

    # Producer adds a strictly newer sample -> wake-up.
    assert r.execute_command("TS.ADD", "ts", 400, "4.0") == 400

    t.join(timeout=2.0)
    env.assertFalse(t.is_alive())
    env.assertEqual(slot["error"], None)
    env.assertEqual(slot["result"], [[400, b"4"]])


# ---------------------------------------------------------------------------
# 9c. '$' on an empty / missing series: resolves to cursor=0 (like '-'/'+'),
#     so it blocks until the first sample is added.
# ---------------------------------------------------------------------------

def test_bget_dollar_on_empty_series_blocks_until_first_add():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    r.execute_command("FLUSHALL")

    t, slot = _start_bget(env, "ts", "$", 2_000)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(),
                   message="BGET '$' on missing key should block until ADD")

    assert r.execute_command("TS.ADD", "ts", 100, "1.0") == 100

    t.join(timeout=2.0)
    env.assertFalse(t.is_alive())
    env.assertEqual(slot["error"], None)
    env.assertEqual(slot["result"], [[100, b"1"]])


# ---------------------------------------------------------------------------
# 10. Multi-client broadcast: every parked client receives the new sample
#     (TS.BGET semantics — not BLPOP-style first-waiter-wins dequeue).
# ---------------------------------------------------------------------------

def test_bget_broadcasts_to_all_blocked_clients():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0")])

    # cursor = 101 (literal): the existing sample at 100 doesn't qualify, so all
    # clients block until a newer sample arrives.
    workers = [_start_bget(env, "ts", 101, 2_000) for _ in range(4)]
    time.sleep(0.3)
    for t, _ in workers:
        env.assertTrue(t.is_alive(), message="all clients should be parked")

    # One producer ADD must wake every parked client with the same sample.
    assert r.execute_command("TS.ADD", "ts", 200, "2.0") == 200

    for t, slot in workers:
        t.join(timeout=2.0)
        env.assertFalse(t.is_alive(),
                        message="every parked client should have unblocked")
        env.assertEqual(slot["error"], None)
        env.assertEqual(slot["result"], [[200, b"2"]])


# ---------------------------------------------------------------------------
# 11. Compaction wake-up: BGET parked on a compaction destination unblocks
#     when a bucket commits into it.
# ---------------------------------------------------------------------------

def test_bget_on_compaction_destination_wakes_on_bucket_commit():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    assert r.execute_command("TS.CREATE", "src")
    assert r.execute_command("TS.CREATE", "dst")
    assert r.execute_command(
        "TS.CREATERULE", "src", "dst", "AGGREGATION", "avg", 10)

    t, slot = _start_bget(env, "dst", "-", 2_000)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(),
                   message="BGET on empty dst should be parked")

    # Two samples in bucket [0,9], then a sample in [10,19] closes the first
    # bucket and writes the aggregated point into dst -> SignalKeyAsReady.
    assert r.execute_command("TS.ADD", "src", 1, "1.0") == 1
    assert r.execute_command("TS.ADD", "src", 2, "2.0") == 2
    assert r.execute_command("TS.ADD", "src", 15, "3.0") == 15

    t.join(timeout=2.0)
    env.assertFalse(t.is_alive(),
                    message="BGET on dst should wake on bucket commit")
    env.assertEqual(slot["error"], None)
    env.assertTrue(len(slot["result"]) >= 1)


# ---------------------------------------------------------------------------
# 12. WRONGTYPE on the literal-cursor path.
# ---------------------------------------------------------------------------

def test_bget_wrongtype_literal_cursor():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    r.set("not_a_ts", "hello")
    with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
        r.execute_command("TS.BGET", "not_a_ts", 0, 0)


# ---------------------------------------------------------------------------
# 13. Refusal inside MULTI / EVAL when timeout > 0.
# ---------------------------------------------------------------------------

def _is_deny_blocking_error(err):
    s = str(err)
    return "deny" in s.lower() or "MULTI" in s or "EVAL" in s or "BGET" in s


def test_bget_inside_multi_refuses_with_clear_error():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    r.execute_command("FLUSHALL")  # key must not exist so strict try_reply
                                   # bails and we reach the deny-blocking
                                   # branch instead of replying synchronously.

    r.execute_command("MULTI")
    r.execute_command("TS.BGET", "ts", 0, 1000)
    try:
        res = r.execute_command("EXEC")
        env.assertEqual(len(res), 1)
        env.assertTrue(_is_deny_blocking_error(res[0]),
                       message="unexpected EXEC reply: %r" % (res[0],))
    except redis.ResponseError as e:
        env.assertTrue(_is_deny_blocking_error(e),
                       message="unexpected error: %r" % (e,))


def test_bget_inside_eval_refuses_with_clear_error():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    r.execute_command("FLUSHALL")  # same reason as the MULTI test above.

    with pytest.raises(redis.ResponseError) as excinfo:
        r.execute_command(
            "EVAL", "return redis.call('TS.BGET','ts',0,1000)", 1, "ts")
    env.assertTrue(_is_deny_blocking_error(excinfo.value),
                   message="unexpected error: %r" % (excinfo.value,))


# ---------------------------------------------------------------------------
# 14. Parse / arity validation — every malformed call must surface an error.
# ---------------------------------------------------------------------------

def test_bget_parse_errors():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0")])

    bad_argv = [
        ("ts",),                                       # wrong arity (too few)
        ("ts", 0),                                     # wrong arity (too few)
        ("ts", 0, 0, "MIN_COUNT", 1, "MAX_COUNT", 1, "x"),  # arity (too many)
        ("ts", 0, 1000, "MIN_COUNT", 0),               # min_count == 0
        ("ts", 0, 1000, "MIN_COUNT", -1),              # min_count < 0
        ("ts", 0, 1000, "MAX_COUNT", 0),               # max_count == 0
        ("ts", 0, -1),                                 # timeout < 0
        ("ts", 0, 1000, "MIN_COUNT", "abc"),           # non-integer min_count
        ("ts", 0, 1000, "MIN_COUNT", "1.5"),           # non-integer min_count
        ("ts", 0, "abc"),                              # non-integer timeout
        ("ts", "abc", 1000),                           # non-integer / non-sentinel ts
        ("ts", -1, 1000),                              # negative literal timestamp
        ("ts", 0, 1000, "MIN_COUNT", 5, "MAX_COUNT", 2),   # min_count > max_count
        ("ts", 0, 1000, "MIN_COUNT"),                  # dangling keyword (no value)
    ]
    for argv in bad_argv:
        with pytest.raises(redis.ResponseError):
            r.execute_command("TS.BGET", *argv)


# ---------------------------------------------------------------------------
# 15. timeout_ms == 0: return whatever is available immediately, never block.
# ---------------------------------------------------------------------------

def test_bget_timeout_zero_returns_partial_without_blocking():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0")])

    # MIN_COUNT 10, only 2 qualifying samples; timeout=0 must flush immediately.
    t0 = time.time()
    res = r.execute_command("TS.BGET", "ts", 0, 0, "MIN_COUNT", 10)
    elapsed = time.time() - t0

    env.assertEqual(res, [[100, b"1"], [200, b"2"]])
    env.assertTrue(elapsed < 0.3,
                   message="timeout=0 must not block, took %.3fs" % elapsed)


# ---------------------------------------------------------------------------
# 16. Key deletion unblocks a parked BGET client well before its timeout.
#     Regression: TS.BGET previously used BlockClientOnKeys (BLPOP-style)
#     which ignored delete signals, so deleting the key left the client
#     parked until timeout_ms — leaking the connection and hanging CI.
# ---------------------------------------------------------------------------

def test_bget_del_wakes_parked_client():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0")])

    t, slot = _start_bget(env, "ts", 0, 2_000, min_count=5)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(), message="BGET should be parked waiting for more samples")

    t0 = time.time()
    assert r.execute_command("DEL", "ts") == 1

    t.join(timeout=1.5)
    elapsed = time.time() - t0
    env.assertFalse(t.is_alive(),
                    message="DEL must wake the parked BGET client (took %.3fs)" % elapsed)
    env.assertEqual(slot["error"], None)
    env.assertEqual(slot["result"], [])
    env.assertTrue(elapsed < 1.0,
                   message="DEL wake must be immediate, took %.3fs" % elapsed)


def test_bget_unlink_wakes_parked_client():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0")])

    t, slot = _start_bget(env, "ts", 0, 2_000, min_count=5)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(), message="BGET should be parked")

    t0 = time.time()
    assert r.execute_command("UNLINK", "ts") == 1

    t.join(timeout=1.5)
    elapsed = time.time() - t0
    env.assertFalse(t.is_alive(),
                    message="UNLINK must wake the parked BGET client (took %.3fs)" % elapsed)
    env.assertEqual(slot["error"], None)
    env.assertEqual(slot["result"], [])
    env.assertTrue(elapsed < 1.0,
                   message="UNLINK wake must be immediate, took %.3fs" % elapsed)


def test_bget_flushall_wakes_parked_client():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0")])

    t, slot = _start_bget(env, "ts", 0, 2_000, min_count=5)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(), message="BGET should be parked")

    t0 = time.time()
    r.execute_command("FLUSHALL")

    t.join(timeout=1.5)
    elapsed = time.time() - t0
    env.assertFalse(t.is_alive(),
                    message="FLUSHALL must wake the parked BGET client (took %.3fs)" % elapsed)
    env.assertEqual(slot["error"], None)
    env.assertEqual(slot["result"], [])
    env.assertTrue(elapsed < 1.0,
                   message="FLUSHALL wake must be immediate, took %.3fs" % elapsed)


def test_bget_flushdb_wakes_parked_client():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0")])

    t, slot = _start_bget(env, "ts", 0, 2_000, min_count=5)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(), message="BGET should be parked")

    t0 = time.time()
    r.execute_command("FLUSHDB")

    t.join(timeout=1.5)
    elapsed = time.time() - t0
    env.assertFalse(t.is_alive(),
                    message="FLUSHDB must wake the parked BGET client (took %.3fs)" % elapsed)
    env.assertEqual(slot["error"], None)
    env.assertEqual(slot["result"], [])
    env.assertTrue(elapsed < 1.0,
                   message="FLUSHDB wake must be immediate, took %.3fs" % elapsed)


def test_bget_multiple_parked_clients_all_wake_on_del():
    """Every client parked on the same key must wake on a single DEL,
    not just one — TS.BGET is broadcast, not BLPOP-style dequeue."""
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0")])

    workers = [_start_bget(env, "ts", 0, 2_000, min_count=5) for _ in range(4)]
    time.sleep(0.3)
    for t, _ in workers:
        env.assertTrue(t.is_alive(), message="each BGET should be parked")

    assert r.execute_command("DEL", "ts") == 1

    for t, slot in workers:
        t.join(timeout=1.5)
        env.assertFalse(t.is_alive(),
                        message="every parked BGET must wake on DEL")
        env.assertEqual(slot["error"], None)
        env.assertEqual(slot["result"], [])
