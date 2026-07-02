import math
import pytest
import redis
from includes import *

# Tests for TS.NRANGE / TS.NREVRANGE.
#
# Strategy ("follow the DB and commands"): the source of truth is the existing
# single-key TS.RANGE / TS.REVRANGE. For every scenario we query each key with
# TS.RANGE (with its own aggregator when relevant) and merge those result-sets by
# timestamp in Python (outer join, NaN for gaps, key order preserved). TS.NRANGE
# must produce exactly that pivot.
#
# TS.NRANGE/TS.NREVRANGE are single-shard: all keys must live in the same slot
# (the keynum key-spec makes the cluster route to one shard and reject cross-slot
# with CROSSSLOT). Once routed, execution is identical to standalone, so the logic
# tests below run on a single shard (shared hash tag) and skipOnCluster(); cluster
# coverage is just the cross-slot rejection in test_nrange_crossslot.


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
    """Expected TS.NRANGE result, derived from per-key TS.RANGE/TS.REVRANGE."""
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
    # b'NaN' is the sentinel TS.NRANGE uses for a key with no sample at a ts.
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

def test_nrange_raw_explicit():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_ohlcv(r, '{rx_raw}')
        res = r.execute_command('TS.NRANGE', 5, *keys, '-', '+')
        # raw mode: one row per DISTINCT timestamp across all keys (0,3,6,9,10,13,17)
        assert len(res) == 7
        assert res[0][0] == 0
        assert [_norm(x) for x in res[0][1]] == [10.0, 10.0, 10.0, 10.0, 10.0]
        # from ts 10 onward, close has no samples -> NaN in its column
        assert res[4][0] == 10
        assert [_norm(x) for x in res[4][1]] == [12.0, 12.0, 12.0, 'NAN', 12.0]
        assert res[6][0] == 17
        assert [_norm(x) for x in res[6][1]] == [15.0, 15.0, 15.0, 'NAN', 15.0]


def test_nrange_raw_matches_range():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_rawd}')
        res = r.execute_command('TS.NRANGE', len(keys), *keys, '-', '+')
        _assert_pivot(res, _pivot_ref(r, keys, '-', '+'))


def test_nrevrange_matches_revrange():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_rev}')
        res = r.execute_command('TS.NREVRANGE', len(keys), *keys, '-', '+')
        _assert_pivot(res, _pivot_ref(r, keys, '-', '+', rev=True))


def test_nrange_single_key_matches_range():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_one}')
        res = r.execute_command('TS.NRANGE', 1, keys[0], '-', '+')
        _assert_pivot(res, _pivot_ref(r, [keys[0]], '-', '+'))


# ---------------------------------------------------------------------------
# aggregation
# ---------------------------------------------------------------------------

def test_nrange_ohlcv_aggregators_explicit():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_ohlcv(r, '{rx_ohlcv}')
        res = r.execute_command('TS.NRANGE', 5, *keys, '-', '+',
                                'AGGREGATION', 'first', 'max', 'min', 'last', 'sum', 10)
        assert len(res) == 2
        assert res[0][0] == 0
        # bucket [0,10): first=10 max=14 min=8 last=11 sum=43
        assert [_norm(x) for x in res[0][1]] == [10.0, 14.0, 8.0, 11.0, 43.0]
        assert res[1][0] == 10
        # bucket [10,20): first=12 max=15 min=9 last=NaN(close empty) sum=36
        assert [_norm(x) for x in res[1][1]] == [12.0, 15.0, 9.0, 'NAN', 36.0]


def test_nrange_per_key_agg_matches_range():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_agg}')
        aggs = ['sum', 'min', 'max']
        res = r.execute_command('TS.NRANGE', len(keys), *keys, '-', '+',
                                'AGGREGATION', *aggs, 2)
        _assert_pivot(res, _pivot_ref(r, keys, '-', '+', aggs=aggs, bucket=2))


def test_nrange_single_aggregator_rejected():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_bcast}')
        # a single aggregator for multiple keys is rejected: the count must
        # equal numkeys (one aggregator per key)
        with pytest.raises(redis.ResponseError, match="aggregators"):
            r.execute_command('TS.NRANGE', len(keys), *keys, '-', '+',
                              'AGGREGATION', 'avg', 2)


def test_nrevrange_agg_matches_revrange():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_revagg}')
        aggs = ['count', 'sum', 'last']
        res = r.execute_command('TS.NREVRANGE', len(keys), *keys, '-', '+',
                                'AGGREGATION', *aggs, 3)
        _assert_pivot(res, _pivot_ref(r, keys, '-', '+', rev=True, aggs=aggs, bucket=3))


def test_nrange_empty_matches():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_ohlcv(r, '{rx_empty}')
        # range spans an empty bucket [20,30) -> EMPTY emits an all-NaN row.
        # NRANGE requires one aggregator per key, so repeat 'last' numkeys times.
        res = r.execute_command('TS.NRANGE', len(keys), *keys, 0, 30,
                                'AGGREGATION', *(['last'] * len(keys)), 10, 'EMPTY')
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

def test_nrange_count_limits_rows():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_count}')
        full = _pivot_ref(r, keys, '-', '+')
        res = r.execute_command('TS.NRANGE', len(keys), *keys, '-', '+', 'COUNT', 2)
        _assert_pivot(res, full[:2])


def test_nrange_filter_by_ts_matches():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_fbts}')
        res = r.execute_command('TS.NRANGE', len(keys), *keys, '-', '+',
                                'FILTER_BY_TS', 0, 3, 6)
        _assert_pivot(res, _pivot_ref(r, keys, '-', '+', extra=['FILTER_BY_TS', 0, 3, 6]))


def test_nrange_filter_by_value_matches():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_fbv}')
        # value window that drops some samples per key -> turns into NaN gaps
        res = r.execute_command('TS.NRANGE', len(keys), *keys, '-', '+',
                                'FILTER_BY_VALUE', 102, 302)
        _assert_pivot(res, _pivot_ref(r, keys, '-', '+',
                                      extra=['FILTER_BY_VALUE', 102, 302]))


# ---------------------------------------------------------------------------
# equivalence with TS.RANGE on the "empty terminal bucket" edge case
#
# These pin down the one place the nrange path differs structurally from
# TS.RANGE: nrange wraps the aggregation chain in a SeriesSampleIterator
# (chunk -> per-sample adapter) whose GetNext treats a non-NULL chunk with
# zero samples as end-of-stream (sample_iterator.c). TS.RANGE instead reads
# chunks directly and just skips an empty chunk.
#
# The aggregation iterator (filter_iterator.c) only ever returns a non-NULL
# zero-sample chunk from agg_iter_finalize (terminal: input already
# exhausted) — the mid-stream emitter agg_iter_try_emit_partial returns NULL
# when it has zero samples. So the early end-of-stream can only coincide with
# the true end, and nrange must equal TS.RANGE. A regression that made the
# aggregation iterator emit an empty chunk mid-stream would truncate nrange
# early while TS.RANGE kept going — these tests would catch exactly that.
#
# `count` on an all-NaN bucket is 0, so without EMPTY that bucket is dropped,
# which is what produces the terminal zero-sample chunk we want to exercise.
# ---------------------------------------------------------------------------

def test_nrange_trailing_all_nan_bucket_matches_range():
    """Last bucket in the window is all-NaN (dropped without EMPTY).

    This is the agg_iter_finalize zero-sample terminal chunk. nrange (single
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

        res = r.execute_command('TS.NRANGE', 1, key, 0, 30, 'AGGREGATION', 'count', 10)
        _assert_pivot(res, _pivot_ref(r, [key], 0, 30, aggs=['count'], bucket=10))


def test_nrange_interior_all_nan_bucket_no_early_truncation():
    """All-NaN bucket sits BETWEEN two valid buckets (dropped without EMPTY).

    The valid bucket AFTER the gap must still appear: proves the nrange
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

        res = r.execute_command('TS.NRANGE', 1, key, 0, 30, 'AGGREGATION', 'count', 10)
        _assert_pivot(res, _pivot_ref(r, [key], 0, 30, aggs=['count'], bucket=10))


def test_nrevrange_trailing_all_nan_bucket_matches_revrange():
    """Reverse direction of the terminal zero-sample chunk path.

    In reverse the "last" bucket consumed is the lowest-timestamp one; making
    it all-NaN exercises agg_iter_finalize on the reverse chain. nrange must
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

        res = r.execute_command('TS.NREVRANGE', 1, key, 0, 30, 'AGGREGATION', 'count', 10)
        _assert_pivot(res, _pivot_ref(r, [key], 0, 30, rev=True, aggs=['count'], bucket=10))


# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------

def test_nrange_errors():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_err}')

        # non-integer numkeys
        with pytest.raises(redis.ResponseError, match="numkeys"):
            r.execute_command('TS.NRANGE', 'x', keys[0], keys[1], '-', '+')

        # non-positive numkeys (enough args to pass arity)
        with pytest.raises(redis.ResponseError, match="numkeys"):
            r.execute_command('TS.NRANGE', -1, keys[0], keys[1], '-', '+')

        # numkeys near LLONG_MAX must not overflow the arity check (was a
        # crash: 2+numKeys+2 wrapped negative, skipped the guard, then
        # calloc(numKeys, ...) returned NULL and got dereferenced).
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.NRANGE', 9223372036854775807, keys[0], '-', '+')

        # numkeys larger than the keys actually provided -> wrong arity
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.NRANGE', 3, keys[0], '-', '+')

        # aggregator count must equal numkeys (too few valid aggregators)
        with pytest.raises(redis.ResponseError, match="aggregators"):
            r.execute_command('TS.NRANGE', 3, *keys, '-', '+',
                              'AGGREGATION', 'min', 'max', 10)

        # an undefined aggregator is rejected as an unknown aggregation type, not a count error
        with pytest.raises(redis.ResponseError, match="[Uu]nknown aggregation"):
            r.execute_command('TS.NRANGE', 3, *keys, '-', '+',
                              'AGGREGATION', 'min', 'bogus', 'max', 10)
        # comma-joined token with bucket immediately after is a count mismatch (1 token for 3 keys)
        with pytest.raises(redis.ResponseError, match="number of aggregators"):
            r.execute_command('TS.NRANGE', 3, *keys, '-', '+',
                              'AGGREGATION', 'min,max,avg', 10)

        # missing key
        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.NRANGE', 1, '{rx_err}:nope', '-', '+')


# ---------------------------------------------------------------------------
# cluster: keys must share a slot -> cross-slot keys are rejected
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# multi-agg per key (MOD-16299)
# ---------------------------------------------------------------------------

def _pivot_ref_multi(r, keys, lo, hi, aggs_per_key, bucket, rev=False):
    """Reference pivot for multi-agg specs, matching the flat NRANGE reply format.

    aggs_per_key: list of agg-spec strings, one per key, e.g. ['avg,max', 'sum']
    Returns: [[ts, [v0a, v0b, v1a, ...]], ...]
    """
    cmd = 'TS.REVRANGE' if rev else 'TS.RANGE'
    n_aggs = [spec.count(',') + 1 for spec in aggs_per_key]
    maps = []
    for k, spec in zip(keys, aggs_per_key):
        rows = r.execute_command(cmd, k, lo, hi, 'AGGREGATION', spec, bucket)
        maps.append({row[0]: list(row[1:]) for row in rows})
    all_ts = set()
    for m in maps:
        all_ts |= set(m.keys())
    result = []
    for ts in sorted(all_ts, reverse=rev):
        per_key = [m.get(ts, [b'NaN'] * n) for m, n in zip(maps, n_aggs)]
        flat = [v for sublist in per_key for v in sublist]
        result.append([ts, flat])
    return result


def test_nrange_multi_agg_mixed():
    """key0 gets 2 aggs, key1 gets 1 agg -> flat format."""
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_magg}')[:2]
        res = r.execute_command('TS.NRANGE', 2, *keys, '-', '+',
                                'AGGREGATION', 'avg,max', 'sum', 2)
        _assert_pivot(res, _pivot_ref_multi(r, keys, '-', '+',
                                            ['avg,max', 'sum'], 2))


def test_nrange_multi_agg_all_same():
    """All keys get the same multi-agg spec -> flat format."""
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_magg2}')[:2]
        res = r.execute_command('TS.NRANGE', 2, *keys, '-', '+',
                                'AGGREGATION', 'min,max', 'min,max', 2)
        _assert_pivot(res, _pivot_ref_multi(r, keys, '-', '+',
                                            ['min,max', 'min,max'], 2))


def test_nrange_multi_agg_single_key():
    """Single key: splice is bypassed, comma-agg goes directly to the range
    parser -> flat format."""
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_magg3}')
        res = r.execute_command('TS.NRANGE', 1, keys[0], '-', '+',
                                'AGGREGATION', 'avg,sum', 2)
        _assert_pivot(res, _pivot_ref_multi(r, [keys[0]], '-', '+',
                                            ['avg,sum'], 2))


def test_nrevrange_multi_agg():
    """NREVRANGE with multi-agg per key -> flat format, descending order."""
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_maggrev}')[:2]
        res = r.execute_command('TS.NREVRANGE', 2, *keys, '-', '+',
                                'AGGREGATION', 'sum,count', 'max', 3)
        _assert_pivot(res, _pivot_ref_multi(r, keys, '-', '+',
                                            ['sum,count', 'max'], 3, rev=True))


def test_nrange_multi_agg_nan_fills_all_slots():
    """When a key has no sample at a timestamp, ALL its agg slots are NaN."""
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = ['{rx_nan2}:a', '{rx_nan2}:b']
        for k in keys:
            r.execute_command('TS.CREATE', k)
        # key0 in [0,10) only; key1 spans both [0,10) and [10,20)
        r.execute_command('TS.MADD', keys[0], 0, 1, keys[0], 5, 2)
        r.execute_command('TS.MADD', keys[1], 0, 10, keys[1], 5, 20,
                          keys[1], 10, 100)
        res = r.execute_command('TS.NRANGE', 2, *keys, '-', '+',
                                'AGGREGATION', 'sum', 'sum,count', 10)
        assert len(res) == 2
        # bucket [0,10): flat=[sum_k0=3, sum_k1=30, count_k1=2]
        assert res[0][0] == 0
        assert _norm(res[0][1][0]) == 3.0
        assert _norm(res[0][1][1]) == 30.0 and _norm(res[0][1][2]) == 2.0
        # bucket [10,20): key0 missing -> NaN; key1=[sum=100, count=1]
        assert res[1][0] == 10
        assert _norm(res[1][1][0]) == 'NAN'
        assert _norm(res[1][1][1]) == 100.0 and _norm(res[1][1][2]) == 1.0


def test_nrange_multi_agg_too_many_tokens():
    """More agg tokens than numkeys -> count mismatch error."""
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_magerr}')[:2]
        with pytest.raises(redis.ResponseError, match="number of aggregators"):
            r.execute_command('TS.NRANGE', 2, *keys, '-', '+',
                              'AGGREGATION', 'avg,max', 'sum', 'min', 10)


def test_nrange_all_single_agg_stays_flat():
    """All single-agg per key -> flat format unchanged (backward compat)."""
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        keys = _setup_distinct(r, '{rx_flat}')
        aggs = ['sum', 'min', 'max']
        res = r.execute_command('TS.NRANGE', len(keys), *keys, '-', '+',
                                'AGGREGATION', *aggs, 2)
        for row in res:
            for v in row[1]:
                assert not isinstance(v, list), \
                    f"expected flat values but got nested list at ts {row[0]}"
        _assert_pivot(res, _pivot_ref(r, keys, '-', '+', aggs=aggs, bucket=2))


# ---------------------------------------------------------------------------
# cluster: keys must share a slot -> cross-slot keys are rejected
# ---------------------------------------------------------------------------

def test_nrange_crossslot():
    env = Env(decodeResponses=True)
    if not env.isCluster():
        env.skip()
    with env.getClusterConnectionIfNeeded() as r:
        # Different hash tags -> different slots; the keynum key-spec must make
        # the command be rejected for spanning slots. Depending on the client,
        # this is caught either client-side (redis-py's cluster client computes
        # the slots from the key-spec and raises RedisClusterException before
        # the command leaves the client) or server-side (CROSSSLOT ResponseError).
        r.execute_command('TS.CREATE', 'cs{a}')
        r.execute_command('TS.CREATE', 'cs{b}')
        for cmd in ('TS.NRANGE', 'TS.NREVRANGE'):
            with pytest.raises((redis.ResponseError, redis.exceptions.RedisClusterException),
                               match='CROSSSLOT|same key slot'):
                r.execute_command(cmd, 2, 'cs{a}', 'cs{b}', '-', '+')
