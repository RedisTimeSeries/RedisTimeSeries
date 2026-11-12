import time
from threading import Thread

from includes import *
from utils import slot_table


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_argv(key, cursor, block_ms=None, min_count=None, max_count=None):
    """Build a TS.READ argument list for the
    `key timestamp [BLOCK ms min_count] [MAX_COUNT n]` syntax."""
    argv = [key, cursor]
    if block_ms is not None:
        argv += ["BLOCK", block_ms, min_count if min_count is not None else 1]
    if max_count is not None:
        argv += ["MAX_COUNT", max_count]
    return argv


def _start_read(env, key, cursor, block_ms=None, min_count=None, max_count=None):
    """Spawn a worker thread that issues TS.READ on its own connection.

    Returns (thread, slot) where `slot` is mutated by the worker:
        slot["result"]  -> reply payload (or None if it errored / hasn't returned)
        slot["error"]   -> exception text (or None)
        slot["elapsed"] -> wall-clock seconds the READ call took
    """
    slot = {"result": None, "error": None, "elapsed": None}
    argv = _read_argv(key, cursor, block_ms, min_count, max_count)

    def worker():
        conn = env.getConnection()
        # Cap the socket read so a stuck server-side READ cannot keep the
        # thread (daemon or not) alive past a short, predictable bound.
        # Slightly above block_ms to let the server's own timeout fire
        # first under normal conditions.
        try:
            if block_ms is not None:
                timeout = WAKE_TIMEOUT_SECS + 1.0 if block_ms == 0 else max(1.0, (block_ms / 1000.0) + 1.0)
                conn.connection_pool.connection_kwargs["socket_timeout"] = timeout
        except Exception:
            pass
        t0 = time.time()
        try:
            slot["result"] = conn.execute_command("TS.READ", *argv)
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


def _wait_for_parked_readers(conn, expected):
    """Wait for server-side blocking; a live worker may not have sent TS.READ yet."""
    deadline = time.monotonic() + WAKE_TIMEOUT_SECS
    while time.monotonic() < deadline:
        clients = conn.client_list()
        parked = [c for c in clients if c["cmd"].lower() == "ts.read" and "b" in c["flags"]]
        if len(parked) == expected:
            return
        time.sleep(0.01)
    raise AssertionError("Expected %d parked TS.READ clients, got %d: %r" %
                         (expected, len(parked), clients))


def _seed(r, key, samples):
    assert r.execute_command("TS.CREATE", key)
    for ts, val in samples:
        assert r.execute_command("TS.ADD", key, ts, val) == ts


# ---------------------------------------------------------------------------
# 1. Immediate reply when enough qualifying samples already exist.
# ---------------------------------------------------------------------------

def test_read_returns_immediately_when_min_count_already_satisfied():
    env = Env()
    if env.is_cluster():
        env.skip()  # READ is a single-key primitive; cluster routing is orthogonal.

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0")])

    # cursor=101 -> qualifying = {200, 300}; BLOCK min_count 2 is already met, so
    # READ should NOT block and return synchronously (MAX_COUNT 2 caps the reply).
    t0 = time.time()
    res = r.execute_command("TS.READ", "ts", 101, "BLOCK", 1000, 2, "MAX_COUNT", 2)
    elapsed = time.time() - t0

    env.assertEqual(res, [[200, b"2"], [300, b"3"]])
    # Sanity: nothing close to the 1s timeout.
    env.assertTrue(elapsed < 0.5, message="READ should be ~instant, got %.3fs" % elapsed)


# ---------------------------------------------------------------------------
# 2. Blocks until SignalKeyAsReady has pushed qualifying-count to >= min_count.
# ---------------------------------------------------------------------------

def test_read_blocks_then_unblocks_when_min_count_is_reached():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0")])

    # cursor=101 -> initial qualifying = {200, 300} = 2 < 5, so READ blocks.
    # NOTE: to reach min_count=5 with cursor=101 we need THREE more samples,
    # not two — the sample at ts=100 doesn't qualify because 100 < 101
    # (cursor is an inclusive lower bound: only ts >= cursor qualifies).
    # Each TS.ADD fires SignalKeyAsReady -> reply_cb runs; only the third
    # add commits because that's when qualifying becomes 5.
    t, slot = _start_read(env, "ts", 101, 2_000, min_count=5, max_count=5)

    # Give the worker enough time to issue BlockClientOnKeys on the server.
    time.sleep(0.3)
    env.assertTrue(t.is_alive(), message="READ should be blocked (2 < 5 qualifying)")

    # Add #1: qualifying = {200, 300, 400} = 3 < 5 -> stay blocked.
    assert r.execute_command("TS.ADD", "ts", 400, "4.0") == 400
    time.sleep(0.2)
    env.assertTrue(t.is_alive(),
                   message="READ should still be blocked after 1st add (3 < 5)")

    # Add #2: qualifying = {200, 300, 400, 500} = 4 < 5 -> stay blocked.
    assert r.execute_command("TS.ADD", "ts", 500, "5.0") == 500
    time.sleep(0.2)
    env.assertTrue(t.is_alive(),
                   message="READ should still be blocked after 2nd add (4 < 5)")

    # Add #3: qualifying = {200, 300, 400, 500, 600} = 5 -> reply_cb commits.
    assert r.execute_command("TS.ADD", "ts", 600, "6.0") == 600

    t.join(timeout=2.0)
    env.assertFalse(t.is_alive(),
                    message="READ should have replied once qualifying >= min_count")

    env.assertEqual(slot["error"], None)
    env.assertEqual(
        slot["result"],
        [[200, b"2"], [300, b"3"], [400, b"4"], [500, b"5"], [600, b"6"]],
    )
    # Sanity: well below the 10s timeout — we unblocked on the signal, not on timeout.
    env.assertTrue(slot["elapsed"] < 5.0,
                   message="READ took %.2fs, should have unblocked promptly" % slot["elapsed"])


# ---------------------------------------------------------------------------
# 3. Oldest-first paging: when MORE than `max_count` samples qualify, return the
#    oldest `max_count` (so callers can advance the cursor to the last returned ts).
# ---------------------------------------------------------------------------

def test_read_returns_oldest_max_count_when_more_qualify():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [
        (100, "1.0"), (200, "2.0"), (300, "3.0"),
        (400, "4.0"), (500, "5.0"),
    ])

    # cursor=101 -> 4 qualifying (200, 300, 400, 500); MAX_COUNT 2 -> take the OLDEST 2.
    res = r.execute_command("TS.READ", "ts", 101, "BLOCK", 1000, 1, "MAX_COUNT", 2)
    env.assertEqual(res, [[200, b"2"], [300, b"3"]])

    # The caller pages forward by setting cursor to last_returned_ts + 1
    # (cursor is inclusive: ts >= cursor qualifies, so passing 300 would
    # re-emit the sample we already saw).
    res = r.execute_command("TS.READ", "ts", 301, "BLOCK", 1000, 1, "MAX_COUNT", 2)
    env.assertEqual(res, [[400, b"4"], [500, b"5"]])


# ---------------------------------------------------------------------------
# 3b. BLOCK min_count vs MAX_COUNT divergence: wait for min_count, then return
#     the oldest max_count (which may be fewer than what qualifies).
# ---------------------------------------------------------------------------

def test_read_min_below_max_returns_oldest_max_count():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [
        (100, "1.0"), (200, "2.0"), (300, "3.0"),
        (400, "4.0"), (500, "5.0"), (600, "6.0"),
    ])

    # cursor=0 -> 6 qualify; BLOCK min_count 2 already met, MAX_COUNT 4 caps to oldest 4.
    res = r.execute_command("TS.READ", "ts", 0, "BLOCK", 1000, 2, "MAX_COUNT", 4)
    env.assertEqual(res, [[100, b"1"], [200, b"2"], [300, b"3"], [400, b"4"]])


# ---------------------------------------------------------------------------
# 3d. Blocking path with min_count < MAX_COUNT: the wake-up gate uses min_count,
#     but the committed reply is still capped to the oldest max_count. A single
#     TS.MADD lands all samples before the wake-up callback runs, so qualifying
#     jumps past max_count in one shot — exercising gate vs cap in the async
#     (reply-callback) path, not just the synchronous fast path (test 3b).
# ---------------------------------------------------------------------------

def test_read_blocking_min_gate_max_cap():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0")])

    # cursor = 101 (literal), so the existing sample at 100 doesn't qualify and
    # READ blocks (0 < min_count 2). MAX_COUNT 3 caps whatever we eventually return.
    t, slot = _start_read(env, "ts", 101, 2_000, min_count=2, max_count=3)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(), message="READ should block until min_count qualifies")

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
    env.assertFalse(t.is_alive(), message="READ should have unblocked once min_count qualified")
    env.assertEqual(slot["error"], None)
    env.assertEqual(slot["result"], [[200, b"2"], [300, b"3"], [400, b"4"]])


# ---------------------------------------------------------------------------
# 3e. Keyword matching is case-insensitive (house-style RMUtil_ArgIndex).
# ---------------------------------------------------------------------------

def test_read_count_keywords_are_case_insensitive():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [
        (100, "1.0"), (200, "2.0"), (300, "3.0"), (400, "4.0"),
    ])

    # cursor=0 -> 4 qualify; lower/mixed-case keywords must behave like uppercase.
    res = r.execute_command("TS.READ", "ts", 0, "block", 1000, 1, "Max_Count", 2)
    env.assertEqual(res, [[100, b"1"], [200, b"2"]])


# ---------------------------------------------------------------------------
# 3c. Defaults: no BLOCK/MAX_COUNT -> non-blocking, max_count=unlimited.
# ---------------------------------------------------------------------------

def test_read_defaults_non_blocking_max_unlimited():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0")])

    # No BLOCK: returns immediately. max_count defaults to unlimited so every
    # qualifying sample is returned.
    res = r.execute_command("TS.READ", "ts", 0)
    env.assertEqual(res, [[100, b"1"], [200, b"2"], [300, b"3"]])


# ---------------------------------------------------------------------------
# 4. Timeout flush: client blocks, no producer feeds it, timeout fires and we
#    flush whatever is available (which may be fewer than min_count).
# ---------------------------------------------------------------------------

def test_read_timeout_flushes_available_samples():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [
        (100, "1.0"), (200, "2.0"), (300, "3.0"),
        (400, "4.0"), (500, "5.0"),
    ])

    # cursor=101 -> 4 qualifying; BLOCK min_count 10 cannot be reached. Nobody else adds.
    # After milliseconds expires the timeout_cb flushes the 4 available samples.
    t0 = time.time()
    res = r.execute_command("TS.READ", "ts", 101, "BLOCK", 1000, 10)
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
# 4b. BLOCK 0 = block forever: client parks with no timeout and only wakes
#     when a qualifying sample arrives via SignalKeyAsReady.
# ---------------------------------------------------------------------------

def test_read_block_zero_blocks_forever_until_sample_arrives():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    r.execute_command("FLUSHALL")

    # BLOCK 0 min_count=1: no timeout, must stay parked until first ADD.
    t, slot = _start_read(env, "ts", "-", block_ms=0, min_count=1)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(),
                   message="BLOCK 0 should park the client indefinitely (no timeout)")

    # Still blocked after another pause — no producer yet.
    time.sleep(0.5)
    env.assertTrue(t.is_alive(),
                   message="BLOCK 0 must still be parked with no data")

    assert r.execute_command("TS.ADD", "ts", 100, "1.0") == 100

    t.join(timeout=WAKE_TIMEOUT_SECS)
    env.assertFalse(t.is_alive(),
                    message="BLOCK 0 must wake promptly once a qualifying sample arrives")
    env.assertEqual(slot["error"], None)
    env.assertEqual(slot["result"], [[100, b"1"]])


# ---------------------------------------------------------------------------
# 5. '-' sentinel: cursor=0, matches every existing sample.
# ---------------------------------------------------------------------------

def test_read_dash_returns_all_existing_samples():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0")])

    # '-' resolves statically to cursor=0; with ts >= 0 every sample qualifies,
    # so BLOCK min_count 3 is already met and READ returns synchronously.
    res = r.execute_command("TS.READ", "ts", "-", "BLOCK", 5000, 3, "MAX_COUNT", 3)
    env.assertEqual(res, [[100, b"1"], [200, b"2"], [300, b"3"]])


# ---------------------------------------------------------------------------
# 6. '-' on a missing key: cursor=0, blocks until the first qualifying ADD.
# ---------------------------------------------------------------------------

def test_read_dash_on_empty_series_blocks_until_first_add():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    r.execute_command("FLUSHALL")  # key "ts" must not exist for this scenario.

    t, slot = _start_read(env, "ts", "-", 2_000)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(),
                   message="READ '-' on missing key should block until ADD")

    assert r.execute_command("TS.ADD", "ts", 100, "1.0") == 100

    t.join(timeout=2.0)
    env.assertFalse(t.is_alive(),
                    message="READ should have replied once the first sample arrived")
    env.assertEqual(slot["error"], None)
    env.assertEqual(slot["result"], [[100, b"1"]])


# ---------------------------------------------------------------------------
# 7. '+' sentinel on an empty / missing key: cursor=0, behaves like '-'.
# ---------------------------------------------------------------------------

def test_read_plus_on_empty_series_blocks_until_first_add():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    r.execute_command("FLUSHALL")

    t, slot = _start_read(env, "ts", "+", 2_000)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(),
                   message="READ '+' on missing key should block until ADD")

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

def test_read_plus_returns_latest_existing_sample():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0")])

    # '+' resolves to lastTs = 300. With inclusive ts >= 300, only the latest
    # sample qualifies; BLOCK min_count defaults to 1 so it returns synchronously.
    t0 = time.time()
    res = r.execute_command("TS.READ", "ts", "+", "BLOCK", 1000, 1)
    elapsed = time.time() - t0

    env.assertEqual(res, [[300, b"3"]])
    env.assertTrue(elapsed < 0.5, message="READ '+' should be ~instant, got %.3fs" % elapsed)


# ---------------------------------------------------------------------------
# 9. Paging continuation: after consuming via '+', a follow-up call from
#    (last retrieved ts + 1) has nothing newer and times out with an empty reply.
# ---------------------------------------------------------------------------

def test_read_paging_past_latest_times_out_empty():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0")])

    # Caller already consumed up to ts=300 and pages forward to 301; no sample
    # has ts >= 301, so the call blocks and flushes empty on timeout.
    t0 = time.time()
    res = r.execute_command("TS.READ", "ts", 301, "BLOCK", 1000, 1)
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

def test_read_dollar_only_returns_samples_added_after_command():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0")])

    # '$' resolves to lastTs + 1 = 301. Existing 100/200/300 must NOT qualify;
    # only a future ADD with ts >= 301 should wake us.
    t, slot = _start_read(env, "ts", "$", 2_000)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(),
                   message="READ '$' should ignore pre-existing samples and block")

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

def test_read_dollar_on_empty_series_blocks_until_first_add():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    r.execute_command("FLUSHALL")

    t, slot = _start_read(env, "ts", "$", 2_000)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(),
                   message="READ '$' on missing key should block until ADD")

    assert r.execute_command("TS.ADD", "ts", 100, "1.0") == 100

    t.join(timeout=2.0)
    env.assertFalse(t.is_alive())
    env.assertEqual(slot["error"], None)
    env.assertEqual(slot["result"], [[100, b"1"]])


# ---------------------------------------------------------------------------
# 10. Multi-client broadcast: every parked client receives the new sample
#     (TS.READ semantics — not BLPOP-style first-waiter-wins dequeue).
# ---------------------------------------------------------------------------

def test_read_broadcasts_to_all_blocked_clients():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0")])

    # cursor = 101 (literal): the existing sample at 100 doesn't qualify, so all
    # clients block until a newer sample arrives.
    workers = [_start_read(env, "ts", 101, 2_000) for _ in range(4)]
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
# 11. Compaction wake-up: READ parked on a compaction destination unblocks
#     when a bucket commits into it.
# ---------------------------------------------------------------------------

def test_read_on_compaction_destination_wakes_on_bucket_commit():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    assert r.execute_command("TS.CREATE", "src")
    assert r.execute_command("TS.CREATE", "dst")
    assert r.execute_command(
        "TS.CREATERULE", "src", "dst", "AGGREGATION", "avg", 10)

    t, slot = _start_read(env, "dst", "-", 2_000)
    time.sleep(0.3)
    env.assertTrue(t.is_alive(),
                   message="READ on empty dst should be parked")

    # Two samples in bucket [0,9], then a sample in [10,19] closes the first
    # bucket and writes the aggregated point into dst -> SignalKeyAsReady.
    assert r.execute_command("TS.ADD", "src", 1, "1.0") == 1
    assert r.execute_command("TS.ADD", "src", 2, "2.0") == 2
    assert r.execute_command("TS.ADD", "src", 15, "3.0") == 15

    t.join(timeout=2.0)
    env.assertFalse(t.is_alive(),
                    message="READ on dst should wake on bucket commit")
    env.assertEqual(slot["error"], None)
    env.assertTrue(len(slot["result"]) >= 1)


# ---------------------------------------------------------------------------
# 12. WRONGTYPE on the literal-cursor path.
# ---------------------------------------------------------------------------

def test_read_wrongtype_literal_cursor():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    r.set("not_a_ts", "hello")
    with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
        r.execute_command("TS.READ", "not_a_ts", 0)


# ---------------------------------------------------------------------------
# 13. Refusal inside MULTI / EVAL when BLOCK ms > 0.
# ---------------------------------------------------------------------------

def _is_deny_blocking_error(err):
    s = str(err)
    return "deny" in s.lower() or "MULTI" in s or "EVAL" in s or "with BLOCK" in s


def test_read_inside_multi_refuses_with_clear_error():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    r.execute_command("FLUSHALL")  # key must not exist so strict try_reply
                                   # bails and we reach the deny-blocking
                                   # branch instead of replying synchronously.

    r.execute_command("MULTI")
    r.execute_command("TS.READ", "ts", 0, "BLOCK", 1000, 1)
    try:
        res = r.execute_command("EXEC")
        env.assertEqual(len(res), 1)
        env.assertTrue(_is_deny_blocking_error(res[0]),
                       message="unexpected EXEC reply: %r" % (res[0],))
    except redis.ResponseError as e:
        env.assertTrue(_is_deny_blocking_error(e),
                       message="unexpected error: %r" % (e,))


def test_read_inside_eval_refuses_with_clear_error():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    r.execute_command("FLUSHALL")  # same reason as the MULTI test above.

    with pytest.raises(redis.ResponseError) as excinfo:
        r.execute_command(
            "EVAL", "return redis.call('TS.READ','ts',0,'BLOCK',1000,1)", 1, "ts")
    env.assertTrue(_is_deny_blocking_error(excinfo.value),
                   message="unexpected error: %r" % (excinfo.value,))


def test_read_block_zero_refused_in_multi():
    """BLOCK 0 (infinite) must also be refused inside MULTI — it blocks forever."""
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    r.execute_command("FLUSHALL")

    r.execute_command("MULTI")
    r.execute_command("TS.READ", "ts", 0, "BLOCK", 0, 1)
    try:
        res = r.execute_command("EXEC")
        env.assertEqual(len(res), 1)
        env.assertTrue(_is_deny_blocking_error(res[0]),
                       message="unexpected EXEC reply: %r" % (res[0],))
    except redis.ResponseError as e:
        env.assertTrue(_is_deny_blocking_error(e),
                       message="unexpected error: %r" % (e,))


# ---------------------------------------------------------------------------
# 14. Parse / arity validation — every malformed call must surface an error.
# ---------------------------------------------------------------------------

def test_read_parse_errors():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0")])

    bad_argv = [
        ("ts",),                                              # wrong arity (too few, missing ts)
        ("ts", 0, "BLOCK", -1, 1),                           # timeout < 0
        ("ts", 0, "BLOCK", 1000, 0),                         # min_count == 0
        ("ts", 0, "BLOCK", 1000, -1),                        # min_count < 0
        ("ts", 0, "BLOCK", 1000, "abc"),                     # non-integer min_count
        ("ts", 0, "BLOCK", 1000, "1.5"),                     # non-integer min_count
        ("ts", 0, "BLOCK", "abc", 1),                        # non-integer timeout
        ("ts", 0, "BLOCK", 1000),                            # BLOCK without min_count
        ("ts", 0, "MAX_COUNT", 0),                           # max_count == 0
        ("ts", 0, "MAX_COUNT", -1),                          # max_count < 0
        ("ts", 0, "MAX_COUNT"),                              # dangling MAX_COUNT keyword
        ("ts", "abc", "BLOCK", 1000, 1),                     # non-integer / non-sentinel ts
        ("ts", -1, "BLOCK", 1000, 1),                        # negative literal timestamp
        ("ts", 0, "BLOCK", 1000, 5, "MAX_COUNT", 2),         # min_count > max_count
        ("ts", 0, "BLOCK", 1000, 1, "MAX_COUNT", 1, "x"),    # too many args
    ]
    for argv in bad_argv:
        with pytest.raises(redis.ResponseError):
            r.execute_command("TS.READ", *argv)


# ---------------------------------------------------------------------------
# 15. Without BLOCK: return whatever is available immediately, never block.
# ---------------------------------------------------------------------------

def test_read_without_block_returns_partial_immediately():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0")])

    # Without BLOCK, READ must flush immediately even when only 2 samples exist.
    t0 = time.time()
    res = r.execute_command("TS.READ", "ts", 0)
    elapsed = time.time() - t0

    env.assertEqual(res, [[100, b"1"], [200, b"2"]])
    env.assertTrue(elapsed < 0.3,
                   message="non-blocking READ must not block, took %.3fs" % elapsed)


def test_read_without_block_on_missing_key_returns_empty_immediately():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    r.execute_command("FLUSHALL")

    t0 = time.time()
    res = r.execute_command("TS.READ", "ts", 0)
    elapsed = time.time() - t0

    env.assertEqual(res, [])
    env.assertTrue(elapsed < 0.3,
                   message="non-blocking READ on missing key must not block, took %.3fs" % elapsed)


def test_read_without_block_on_empty_series_returns_empty_immediately():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    assert r.execute_command("TS.CREATE", "ts")

    t0 = time.time()
    res = r.execute_command("TS.READ", "ts", 0)
    elapsed = time.time() - t0

    env.assertEqual(res, [])
    env.assertTrue(elapsed < 0.3,
                   message="non-blocking READ on empty series must not block, took %.3fs" % elapsed)


def test_read_without_block_respects_max_count():
    env = Env()
    if env.is_cluster():
        env.skip()

    r = env.getConnection()
    _seed(r, "ts", [(100, "1.0"), (200, "2.0"), (300, "3.0"), (400, "4.0")])

    res = r.execute_command("TS.READ", "ts", 0, "MAX_COUNT", 2)
    env.assertEqual(res, [[100, b"1"], [200, b"2"]])


def _raw_read_connection(conn):
    # Read the server's reply without redis-py following MOVED or retrying.
    return conn.connection_pool.connection_class(**{
        **conn.connection_pool.connection_kwargs,
        "socket_timeout": WAKE_TIMEOUT_SECS,
    })


def test_read_cluster_migration_redirects_blocked_client():
    env = Env(shardsCount=2, decodeResponses=True)
    if env.env != "oss-cluster":
        env.skip()

    source, target = env.getConnection(1), env.getConnection(2)
    source_id = source.execute_command("CLUSTER", "MYID")
    target_id = target.execute_command("CLUSTER", "MYID")
    target_port = target.connection_pool.connection_kwargs["port"]

    # Cover both a migrated series and a read waiting for a not-yet-created key.
    for slot, exists in enumerate((True, False)):
        key = "moving:{%s}" % slot_table[slot]
        stationary = "stationary:{%s}" % slot_table[slot + 2]
        if exists:
            source.execute_command("TS.ADD", key, 1, 1)
        source.execute_command("TS.ADD", stationary, 1, 1)
        reader = _raw_read_connection(source)
        unaffected = _raw_read_connection(source)
        try:
            for conn, name in ((reader, key), (unaffected, stationary)):
                conn.send_command("TS.READ", name, 1000, "BLOCK",
                                  WAKE_TIMEOUT_SECS * 2 * 1000, 1)
            _wait_for_parked_readers(source, 2)

            target.execute_command("CLUSTER", "SETSLOT", slot, "IMPORTING", source_id)
            source.execute_command("CLUSTER", "SETSLOT", slot, "MIGRATING", target_id)
            result = source.execute_command("MIGRATE", "127.0.0.1", target_port, key, 0, 5000)
            env.assertEqual(result, "OK" if exists else "NOKEY")
            # MIGRATE deleting the source key must not produce an empty reply.
            _wait_for_parked_readers(source, 2)
            for conn in (target, source):
                conn.execute_command("CLUSTER", "SETSLOT", slot, "NODE", target_id)

            try:
                reply = reader.read_response()
                env.assertTrue(False, message="Expected MOVED, got %r" % (reply,))
            except redis.exceptions.ResponseError as error:
                env.assertEqual(str(error), "MOVED %d 127.0.0.1:%d" % (slot, target_port))
            reader.send_command("PING")
            env.assertEqual(reader.read_response(), "PONG")

            _wait_for_parked_readers(source, 1)
            source.execute_command("TS.ADD", stationary, 1000, 42)
            env.assertEqual(unaffected.read_response(), [[1000, "42"]])
            target.execute_command("TS.ADD", key, 1000, 99)
            env.assertEqual(target.execute_command("TS.READ", key, 1000), [[1000, "99"]])
        finally:
            reader.disconnect()
            unaffected.disconnect()
            # Restore ownership and data so the shared cluster is reusable.
            target.execute_command("ASKING")
            target.execute_command("DEL", key)
            for conn in (source, target):
                conn.execute_command("CLUSTER", "SETSLOT", slot, "NODE", source_id)
            source.execute_command("DEL", stationary)
        _wait_for_parked_readers(source, 0)
        env.assertEqual(source.info("clients")["blocked_clients"], 0)
        env.assertEqual(target.info("clients")["blocked_clients"], 0)


def test_read_key_removal_waits_for_recreation():
    env = Env(decodeResponses=True)
    source = env.getConnection(1)
    key = "ts:{%s}" % slot_table[0]
    for block_ms in (0, WAKE_TIMEOUT_SECS * 2 * 1000):
        for removal in (("DEL", key), ("UNLINK", key), ("FLUSHDB",),
                        ("FLUSHALL",), ("PEXPIREAT", key, 1)):
            source.execute_command("DEL", key)
            source.execute_command("TS.ADD", key, 100, 1)
            reader = _raw_read_connection(source)
            try:
                reader.send_command("TS.READ", key, "$", "BLOCK", block_ms, 2)
                _wait_for_parked_readers(source, 1)
                source.execute_command(*removal)
                env.assertEqual(source.exists(key), 0)
                _wait_for_parked_readers(source, 1)

                # Recreation must preserve both the resolved '$' cursor and min_count.
                source.execute_command("TS.ADD", key, 1, 1)
                source.execute_command("TS.ADD", key, 100, 1)
                _wait_for_parked_readers(source, 1)
                source.execute_command("TS.ADD", key, 101, 2)
                _wait_for_parked_readers(source, 1)
                source.execute_command("TS.ADD", key, 102, 3)
                env.assertEqual(reader.read_response(), [[101, "2"], [102, "3"]])
                reader.send_command("PING")
                env.assertEqual(reader.read_response(), "PONG")
            finally:
                reader.disconnect()
            _wait_for_parked_readers(source, 0)


def test_read_deleted_key_timeout_and_disconnect_cleanup():
    env = Env(decodeResponses=True)
    source = env.getConnection(1)
    key = "ts:{%s}" % slot_table[0]
    for finish in ("timeout", "disconnect", "unblock"):
        source.execute_command("TS.ADD", key, 1, 1)
        reader = _raw_read_connection(source)
        try:
            reader.send_command("CLIENT", "ID")
            client_id = reader.read_response()
            block_ms = WAKE_TIMEOUT_SECS * 200 if finish == "timeout" else 0
            reader.send_command("TS.READ", key, 1000, "BLOCK", block_ms, 1)
            started = time.monotonic()
            _wait_for_parked_readers(source, 1)
            source.execute_command("DEL", key)
            _wait_for_parked_readers(source, 1)
            if finish == "timeout":
                env.assertEqual(reader.read_response(), [])
                env.assertTrue(time.monotonic() - started >= block_ms / 1000 * 0.9)
            elif finish == "disconnect":
                reader.disconnect()
            else:
                env.assertEqual(source.execute_command("CLIENT", "UNBLOCK", client_id, "ERROR"), 1)
                try:
                    reply = reader.read_response()
                    env.assertTrue(False, message="Expected UNBLOCKED, got %r" % (reply,))
                except redis.exceptions.ResponseError as error:
                    env.assertTrue(str(error).startswith("UNBLOCKED "))
            if finish != "disconnect":
                reader.send_command("PING")
                env.assertEqual(reader.read_response(), "PONG")
        finally:
            reader.disconnect()
        _wait_for_parked_readers(source, 0)
        env.assertEqual(source.info("clients")["blocked_clients"], 0)


def test_read_multiple_parked_clients_wake_after_recreation():
    env = Env(decodeResponses=True)
    source = env.getConnection(1)
    key = "ts:{%s}" % slot_table[0]
    source.execute_command("TS.ADD", key, 1, 1)
    readers = [_raw_read_connection(source) for _ in range(4)]
    try:
        for reader in readers:
            reader.send_command("TS.READ", key, 1000, "BLOCK", 0, 1)
        _wait_for_parked_readers(source, len(readers))
        source.execute_command("DEL", key)
        _wait_for_parked_readers(source, len(readers))
        source.execute_command("TS.ADD", key, 1000, 42)
        for reader in readers:
            env.assertEqual(reader.read_response(), [[1000, "42"]])
    finally:
        for reader in readers:
            reader.disconnect()
    _wait_for_parked_readers(source, 0)
