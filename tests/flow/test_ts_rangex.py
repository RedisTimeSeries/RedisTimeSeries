import math
import pytest
import redis
from includes import *

# Tests for TS.RANGEX / TS.REVRANGEX.
#
# Strategy ("follow the DB and commands"): the source of truth is the existing
# single-key TS.RANGE / TS.REVRANGE. For every scenario we query each key with
# TS.RANGE (with its own aggregator when relevant) and merge those result-sets by
# timestamp in Python (outer join, NaN for gaps, key order preserved). TS.RANGEX
# must produce exactly that pivot.
#
# All keys share a hash tag so they live in the same slot (TS.RANGEX requires it).
# Cluster is skipped until the keynum key-spec lands.


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _norm(v):
    """Normalize a RESP2 (bytes) / RESP3 (float|int) scalar for comparison."""
    if isinstance(v, bytes):
        v = v.decode()
    if isinstance(v, str):
        try:
            f = float(v)
        except ValueError:
            return v
        return 'NAN' if math.isnan(f) else f
    if isinstance(v, float):
        return 'NAN' if math.isnan(v) else v
    return v


def _pivot_ref(r, keys, lo, hi, rev=False, aggs=None, bucket=None, extra=None):
    """Expected TS.RANGEX result, derived from per-key TS.RANGE/TS.REVRANGE."""
    cmd = 'TS.REVRANGE' if rev else 'TS.RANGE'
    extra = list(extra or [])
    maps = []
    for i, k in enumerate(keys):
        args = [cmd, k, lo, hi] + extra
        if aggs:
            a = aggs[0] if len(aggs) == 1 else aggs[i]
            args += ['AGGREGATION', a, bucket]
        rows = r.execute_command(*args)
        maps.append({row[0]: row[1] for row in rows})
    all_ts = set()
    for m in maps:
        all_ts |= set(m.keys())
    ordered = sorted(all_ts, reverse=rev)
    # b'NaN' is the sentinel TS.RANGEX uses for a key with no sample at a ts.
    return [[ts, [m.get(ts, b'NaN') for m in maps]] for ts in ordered]


def _assert_pivot(actual, expected):
    assert len(actual) == len(expected), \
        f"row count {len(actual)} != {len(expected)}\nactual={actual}\nexpected={expected}"
    for ai, ei in zip(actual, expected):
        assert ai[0] == ei[0], f"timestamp {ai[0]} != {ei[0]}"
        av, ev = ai[1], ei[1]
        assert len(av) == len(ev), f"row width at ts {ai[0]}: {len(av)} != {len(ev)}"
        for j, (a, e) in enumerate(zip(av, ev)):
            assert _norm(a) == _norm(e), \
                f"ts {ai[0]} col {j}: {a!r} != {e!r}"


def _setup_ohlcv(r, tag):
    """5 OHLCV series; close intentionally missing from the [10,20) bucket."""
    keys = [f'{tag}:open', f'{tag}:high', f'{tag}:low', f'{tag}:close', f'{tag}:volume']
    for k in keys:
        r.execute_command('TS.CREATE', k)
    for ts, v in [(0, 10), (3, 14), (6, 8), (9, 11)]:  # all 5 keys
        r.execute_command('TS.MADD', keys[0], ts, v, keys[1], ts, v,
                           keys[2], ts, v, keys[3], ts, v, keys[4], ts, v)
    for ts, v in [(10, 12), (13, 9), (17, 15)]:        # close omitted
        r.execute_command('TS.MADD', keys[0], ts, v, keys[1], ts, v,
                           keys[2], ts, v, keys[4], ts, v)
    return keys


def _setup_distinct(r, tag):
    """3 series with different, partially-overlapping timestamps (lots of gaps)."""
    keys = [f'{tag}:a', f'{tag}:b', f'{tag}:c']
    for k in keys:
        r.execute_command('TS.CREATE', k)
    for ts in range(0, 5):          # a: 0..4
        r.execute_command('TS.ADD', keys[0], ts, 100 + ts)
    for ts in range(2, 7):          # b: 2..6
        r.execute_command('TS.ADD', keys[1], ts, 200 + ts)
    for ts in [0, 3, 6]:            # c: sparse
        r.execute_command('TS.ADD', keys[2], ts, 300 + ts)
    return keys


# ---------------------------------------------------------------------------
# raw (no aggregation)
# ---------------------------------------------------------------------------

def test_rangex_raw_explicit():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_ohlcv(r, '{rx_raw}')
        res = r.execute_command('TS.RANGEX', 5, *keys, '-', '+')
        # raw mode: one row per DISTINCT timestamp across all keys (0,3,6,9,10,13,17)
        assert len(res) == 7
        assert res[0][0] == 0
        assert [_norm(x) for x in res[0][1]] == [10.0, 10.0, 10.0, 10.0, 10.0]
        # from ts 10 onward, close has no samples -> NaN in its column
        assert res[4][0] == 10
        assert [_norm(x) for x in res[4][1]] == [12.0, 12.0, 12.0, 'NAN', 12.0]
        assert res[6][0] == 17
        assert [_norm(x) for x in res[6][1]] == [15.0, 15.0, 15.0, 'NAN', 15.0]


def test_rangex_raw_matches_range():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_rawd}')
        res = r.execute_command('TS.RANGEX', len(keys), *keys, '-', '+')
        _assert_pivot(res, _pivot_ref(r, keys, '-', '+'))


def test_revrangex_matches_revrange():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_rev}')
        res = r.execute_command('TS.REVRANGEX', len(keys), *keys, '-', '+')
        _assert_pivot(res, _pivot_ref(r, keys, '-', '+', rev=True))


def test_rangex_single_key_matches_range():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_one}')
        res = r.execute_command('TS.RANGEX', 1, keys[0], '-', '+')
        _assert_pivot(res, _pivot_ref(r, [keys[0]], '-', '+'))


# ---------------------------------------------------------------------------
# aggregation
# ---------------------------------------------------------------------------

def test_rangex_ohlcv_aggregators_explicit():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_ohlcv(r, '{rx_ohlcv}')
        res = r.execute_command('TS.RANGEX', 5, *keys, '-', '+',
                                'AGGREGATION', 'first,max,min,last,sum', 10)
        assert len(res) == 2
        assert res[0][0] == 0
        # bucket [0,10): first=10 max=14 min=8 last=11 sum=43
        assert [_norm(x) for x in res[0][1]] == [10.0, 14.0, 8.0, 11.0, 43.0]
        assert res[1][0] == 10
        # bucket [10,20): first=12 max=15 min=9 last=NaN(close empty) sum=36
        assert [_norm(x) for x in res[1][1]] == [12.0, 15.0, 9.0, 'NAN', 36.0]


def test_rangex_per_key_agg_matches_range():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_agg}')
        aggs = ['sum', 'min', 'max']
        res = r.execute_command('TS.RANGEX', len(keys), *keys, '-', '+',
                                'AGGREGATION', ','.join(aggs), 2)
        _assert_pivot(res, _pivot_ref(r, keys, '-', '+', aggs=aggs, bucket=2))


def test_rangex_single_aggregator_broadcast():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_bcast}')
        # one aggregator applies to all keys
        res = r.execute_command('TS.RANGEX', len(keys), *keys, '-', '+',
                                'AGGREGATION', 'avg', 2)
        _assert_pivot(res, _pivot_ref(r, keys, '-', '+', aggs=['avg'], bucket=2))


def test_revrangex_agg_matches_revrange():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_revagg}')
        aggs = ['count', 'sum', 'last']
        res = r.execute_command('TS.REVRANGEX', len(keys), *keys, '-', '+',
                                'AGGREGATION', ','.join(aggs), 3)
        _assert_pivot(res, _pivot_ref(r, keys, '-', '+', rev=True, aggs=aggs, bucket=3))


def test_rangex_empty_matches():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_ohlcv(r, '{rx_empty}')
        # range spans an empty bucket [20,30) -> EMPTY emits an all-NaN row
        res = r.execute_command('TS.RANGEX', len(keys), *keys, 0, 30,
                                'AGGREGATION', 'last', 10, 'EMPTY')
        _assert_pivot(res, _pivot_ref_empty(r, keys, 0, 30, 'last', 10))


def _pivot_ref_empty(r, keys, lo, hi, agg, bucket):
    maps = []
    for k in keys:
        rows = r.execute_command('TS.RANGE', k, lo, hi, 'AGGREGATION', agg, bucket, 'EMPTY')
        maps.append({row[0]: row[1] for row in rows})
    all_ts = set()
    for m in maps:
        all_ts |= set(m.keys())
    ordered = sorted(all_ts)
    return [[ts, [m.get(ts, b'NaN') for m in maps]] for ts in ordered]


# ---------------------------------------------------------------------------
# shared options: COUNT / FILTER_BY_TS / FILTER_BY_VALUE
# ---------------------------------------------------------------------------

def test_rangex_count_limits_rows():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_count}')
        full = _pivot_ref(r, keys, '-', '+')
        res = r.execute_command('TS.RANGEX', len(keys), *keys, '-', '+', 'COUNT', 2)
        _assert_pivot(res, full[:2])


def test_rangex_filter_by_ts_matches():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_fbts}')
        res = r.execute_command('TS.RANGEX', len(keys), *keys, '-', '+',
                                'FILTER_BY_TS', 0, 3, 6)
        _assert_pivot(res, _pivot_ref(r, keys, '-', '+', extra=['FILTER_BY_TS', 0, 3, 6]))


def test_rangex_filter_by_value_matches():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_fbv}')
        # value window that drops some samples per key -> turns into NaN gaps
        res = r.execute_command('TS.RANGEX', len(keys), *keys, '-', '+',
                                'FILTER_BY_VALUE', 102, 302)
        _assert_pivot(res, _pivot_ref(r, keys, '-', '+',
                                      extra=['FILTER_BY_VALUE', 102, 302]))


# ---------------------------------------------------------------------------
# equivalence with TS.RANGE on the "empty terminal bucket" edge case
#
# These pin down the one place the rangex path differs structurally from
# TS.RANGE: rangex wraps the aggregation chain in a SeriesSampleIterator
# (chunk -> per-sample adapter) whose GetNext treats a non-NULL chunk with
# zero samples as end-of-stream (sample_iterator.c). TS.RANGE instead reads
# chunks directly and just skips an empty chunk.
#
# The aggregation iterator (filter_iterator.c) only ever returns a non-NULL
# zero-sample chunk from agg_iter_finalize (terminal: input already
# exhausted) — the mid-stream emitter agg_iter_try_emit_partial returns NULL
# when it has zero samples. So the early end-of-stream can only coincide with
# the true end, and rangex must equal TS.RANGE. A regression that made the
# aggregation iterator emit an empty chunk mid-stream would truncate rangex
# early while TS.RANGE kept going — these tests would catch exactly that.
#
# `count` on an all-NaN bucket is 0, so without EMPTY that bucket is dropped,
# which is what produces the terminal zero-sample chunk we want to exercise.
# ---------------------------------------------------------------------------

def test_rangex_trailing_all_nan_bucket_matches_range():
    """Last bucket in the window is all-NaN (dropped without EMPTY).

    This is the agg_iter_finalize zero-sample terminal chunk. rangex (single
    key) must emit the earlier valid buckets and stop exactly where TS.RANGE
    stops — no extra trailing row, no early truncation of the valid buckets.
    """
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        key = '{rx_tnan}:k'
        r.execute_command('TS.CREATE', key)
        r.execute_command('TS.MADD',
                          key, 0, 1, key, 5, 1,        # bucket [0,10):  count 2
                          key, 10, 1, key, 15, 1,      # bucket [10,20): count 2
                          key, 20, 'nan', key, 25, 'nan')  # bucket [20,30): count 0 -> dropped

        rng = r.execute_command('TS.RANGE', key, 0, 30, 'AGGREGATION', 'count', 10)
        # Sanity: the trailing all-NaN bucket must be absent in TS.RANGE.
        assert [row[0] for row in rng] == [0, 10], f'unexpected TS.RANGE: {rng!r}'

        res = r.execute_command('TS.RANGEX', 1, key, 0, 30, 'AGGREGATION', 'count', 10)
        _assert_pivot(res, _pivot_ref(r, [key], 0, 30, aggs=['count'], bucket=10))


def test_rangex_interior_all_nan_bucket_no_early_truncation():
    """All-NaN bucket sits BETWEEN two valid buckets (dropped without EMPTY).

    The valid bucket AFTER the gap must still appear: proves the rangex
    sample-iterator does not hit a premature end-of-stream on the dropped
    interior bucket (it never sees a mid-stream zero-sample chunk).
    """
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        key = '{rx_inan}:k'
        r.execute_command('TS.CREATE', key)
        r.execute_command('TS.MADD',
                          key, 0, 1, key, 5, 1,            # [0,10):  count 2
                          key, 10, 'nan', key, 15, 'nan',  # [10,20): count 0 -> dropped
                          key, 20, 1, key, 25, 1)          # [20,30): count 2 (AFTER the gap)

        rng = r.execute_command('TS.RANGE', key, 0, 30, 'AGGREGATION', 'count', 10)
        # The dropped interior bucket (10) is gone; the bucket after it (20) survives.
        assert [row[0] for row in rng] == [0, 20], f'unexpected TS.RANGE: {rng!r}'

        res = r.execute_command('TS.RANGEX', 1, key, 0, 30, 'AGGREGATION', 'count', 10)
        _assert_pivot(res, _pivot_ref(r, [key], 0, 30, aggs=['count'], bucket=10))


def test_revrangex_trailing_all_nan_bucket_matches_revrange():
    """Reverse direction of the terminal zero-sample chunk path.

    In reverse the "last" bucket consumed is the lowest-timestamp one; making
    it all-NaN exercises agg_iter_finalize on the reverse chain. rangex must
    match TS.REVRANGE.
    """
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        key = '{rx_rtnan}:k'
        r.execute_command('TS.CREATE', key)
        r.execute_command('TS.MADD',
                          key, 0, 'nan', key, 5, 'nan',  # [0,10):  count 0 -> dropped
                          key, 10, 1, key, 15, 1,        # [10,20): count 2
                          key, 20, 1, key, 25, 1)        # [20,30): count 2

        rev = r.execute_command('TS.REVRANGE', key, 0, 30, 'AGGREGATION', 'count', 10)
        assert [row[0] for row in rev] == [20, 10], f'unexpected TS.REVRANGE: {rev!r}'

        res = r.execute_command('TS.REVRANGEX', 1, key, 0, 30, 'AGGREGATION', 'count', 10)
        _assert_pivot(res, _pivot_ref(r, [key], 0, 30, rev=True, aggs=['count'], bucket=10))


# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------

def test_rangex_errors():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_err}')

        # non-integer numkeys
        with pytest.raises(redis.ResponseError, match="numkeys"):
            r.execute_command('TS.RANGEX', 'x', keys[0], keys[1], '-', '+')

        # non-positive numkeys (enough args to pass arity)
        with pytest.raises(redis.ResponseError, match="numkeys"):
            r.execute_command('TS.RANGEX', -1, keys[0], keys[1], '-', '+')

        # numkeys larger than the keys actually provided -> wrong arity
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.RANGEX', 3, keys[0], '-', '+')

        # aggregator count must be 1 or numkeys
        with pytest.raises(redis.ResponseError, match="aggregators"):
            r.execute_command('TS.RANGEX', 3, *keys, '-', '+',
                              'AGGREGATION', 'min,max', 10)

        # missing key
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.RANGEX', 1, '{rx_err}:nope', '-', '+')
