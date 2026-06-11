import math
import os
import random
from contextlib import contextmanager
import pytest
import redis
from includes import *

# Exhaustive cross-checks for TS.NRANGE / TS.NREVRANGE.
#
# Ground truth = the existing single-key TS.RANGE / TS.REVRANGE. For any set of
# options we query each key individually with those same options, merge the
# per-key result-sets by timestamp (outer join, key order preserved, NaN for a
# key with no value at a timestamp), apply COUNT to the merged rows, and assert
# TS.NRANGE produces exactly that.
#
# Same hash tag => same slot (TS.NRANGE requires it). Cluster skipped until the
# keynum key-spec lands.

AGGS = ['min', 'max', 'sum', 'avg', 'count', 'first', 'last', 'range',
        'std.p', 'std.s', 'var.p', 'var.s', 'twa', 'countNaN', 'countAll']


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _norm(v):
    if isinstance(v, bytes):
        v = v.decode()
    if isinstance(v, str):
        try:
            f = float(v)
        except ValueError:
            return v
        return 'NAN' if math.isnan(f) else round(f, 9)
    if isinstance(v, float):
        return 'NAN' if math.isnan(v) else round(v, 9)
    return v


def _single_args(name, key, lo, hi, *, aggs=None, idx=0, bucket=None,
                 fbt=None, fbv=None, align=None, bts=None, empty=False, latest=False):
    a = [name, key, lo, hi]
    if latest:
        a.append('LATEST')
    if fbt is not None:
        a += ['FILTER_BY_TS', *fbt]
    if fbv is not None:
        a += ['FILTER_BY_VALUE', fbv[0], fbv[1]]
    if aggs is not None:
        if align is not None:
            a += ['ALIGN', align]
        agg = aggs[0] if len(aggs) == 1 else aggs[idx]
        a += ['AGGREGATION', agg, bucket]
        if bts is not None:
            a += ['BUCKETTIMESTAMP', bts]
        if empty:
            a.append('EMPTY')
    return a


def _nrange_args(keys, lo, hi, rev, *, aggs=None, bucket=None, count=None,
                 fbt=None, fbv=None, align=None, bts=None, empty=False, latest=False):
    name = 'TS.NREVRANGE' if rev else 'TS.NRANGE'
    cmd = [name, len(keys), *keys, lo, hi]
    if latest:
        cmd.append('LATEST')
    if fbt is not None:
        cmd += ['FILTER_BY_TS', *fbt]
    if fbv is not None:
        cmd += ['FILTER_BY_VALUE', fbv[0], fbv[1]]
    if count is not None:
        cmd += ['COUNT', count]
    if aggs is not None:
        if align is not None:
            cmd += ['ALIGN', align]
        # NRANGE requires one aggregator per key; a test that passes a single
        # aggregator means "the same one for every key", so replicate it.
        cmd_aggs = aggs * len(keys) if len(aggs) == 1 else aggs
        cmd += ['AGGREGATION', ','.join(cmd_aggs), bucket]
        if bts is not None:
            cmd += ['BUCKETTIMESTAMP', bts]
        if empty:
            cmd.append('EMPTY')
    return cmd


def _expected(r, keys, lo, hi, rev, *, aggs=None, bucket=None, count=None,
              fbt=None, fbv=None, align=None, bts=None, empty=False, latest=False):
    sname = 'TS.REVRANGE' if rev else 'TS.RANGE'
    maps = []
    for i, k in enumerate(keys):
        rows = r.execute_command(*_single_args(sname, k, lo, hi, aggs=aggs, idx=i,
                                                bucket=bucket, fbt=fbt, fbv=fbv,
                                                align=align, bts=bts, empty=empty,
                                                latest=latest))
        maps.append({row[0]: row[1] for row in rows})
    all_ts = set()
    for m in maps:
        all_ts |= set(m.keys())
    order = sorted(all_ts, reverse=rev)
    rows = [[ts, [m.get(ts, b'NaN') for m in maps]] for ts in order]
    if count is not None and count >= 0:
        rows = rows[:count]
    return rows


def _cmp(r, keys, lo='-', hi='+', rev=False, **kw):
    cmd = _nrange_args(keys, lo, hi, rev, **kw)
    actual = r.execute_command(*cmd)
    expected = _expected(r, keys, lo, hi, rev, **kw)
    label = ' '.join(str(x) for x in cmd)
    assert len(actual) == len(expected), \
        f"row count {len(actual)} != {len(expected)} for: {label}\n{actual}\nvs\n{expected}"
    for ai, ei in zip(actual, expected):
        assert ai[0] == ei[0], f"ts {ai[0]} != {ei[0]} for: {label}"
        av, ev = ai[1], ei[1]
        assert len(av) == len(ev), f"width at ts {ai[0]} for: {label}"
        for j, (a, e) in enumerate(zip(av, ev)):
            assert _norm(a) == _norm(e), \
                f"ts {ai[0]} col {j}: {a!r} != {e!r} for: {label}"


_counter = [0]


def _mk(r, point_lists, dp=None, chunk_size=None, enc=None):
    """Create N keys under one shared hash tag; feed each its (ts,val) points.

    chunk_size / enc let a test force many chunks per key (CHUNK_SIZE 48 with
    UNCOMPRESSED holds 3 samples/chunk), exercising the cross-chunk GetNext path.
    Both may be a single value (applied to every key) or a per-key list.
    """
    _counter[0] += 1
    tag = f"rxc{_counter[0]}"
    keys = []
    for i, pts in enumerate(point_lists):
        k = f"{{{tag}}}:{i}"
        args = ['TS.CREATE', k]
        if enc is not None:
            args += ['ENCODING', enc[i] if isinstance(enc, list) else enc]
        if chunk_size is not None:
            args += ['CHUNK_SIZE', chunk_size[i] if isinstance(chunk_size, list) else chunk_size]
        if dp:
            args += ['DUPLICATE_POLICY', dp]
        r.execute_command(*args)
        for ts, v in pts:
            r.execute_command('TS.ADD', k, ts, v)
        keys.append(k)
    return keys


@contextmanager
def _conn():
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        r.execute_command('FLUSHALL')  # RLTest may reuse the env across tests
        yield r


# ---------------------------------------------------------------------------
# raw scenarios
# ---------------------------------------------------------------------------

def test_compare_raw_patterns():
    with _conn() as r:
        # identical timestamps across keys
        k = _mk(r, [[(t, t) for t in range(5)],
                    [(t, t * 2) for t in range(5)],
                    [(t, -t) for t in range(5)]])
        _cmp(r, k); _cmp(r, k, rev=True)

        # disjoint timestamp ranges (no overlap => every cell is the lone key)
        k = _mk(r, [[(t, t) for t in range(0, 5)],
                    [(t, t) for t in range(10, 15)],
                    [(t, t) for t in range(20, 25)]])
        _cmp(r, k); _cmp(r, k, rev=True)

        # partial, ragged overlap
        k = _mk(r, [[(t, 100 + t) for t in [0, 1, 2, 3, 4]],
                    [(t, 200 + t) for t in [2, 3, 4, 5, 6]],
                    [(t, 300 + t) for t in [0, 3, 6, 9]]])
        _cmp(r, k); _cmp(r, k, rev=True)

        # one empty key among populated ones
        k = _mk(r, [[(t, t) for t in range(4)], [], [(t, t) for t in range(4)]])
        _cmp(r, k); _cmp(r, k, rev=True)

        # all keys empty
        k = _mk(r, [[], [], []])
        _cmp(r, k); _cmp(r, k, rev=True)

        # single sample per key, scattered
        k = _mk(r, [[(5, 1)], [(7, 2)], [(5, 3)], [(99, 4)]])
        _cmp(r, k); _cmp(r, k, rev=True)

        # negative + float + NaN values
        k = _mk(r, [[(0, -1.5), (1, 'nan'), (2, 3.25)],
                    [(0, 1000000.0), (2, 'nan'), (4, -7)]])
        _cmp(r, k); _cmp(r, k, rev=True)


def test_compare_raw_windows():
    with _conn() as r:
        k = _mk(r, [[(t, 100 + t) for t in range(0, 20)],
                    [(t, 200 + t) for t in range(5, 25)]])
        for lo, hi in [('-', '+'), (0, 9), (5, 14), (8, 8), (100, 200), (15, 5)]:
            _cmp(r, k, lo=lo, hi=hi)
            _cmp(r, k, lo=lo, hi=hi, rev=True)


def test_compare_count():
    with _conn() as r:
        k = _mk(r, [[(t, t) for t in range(0, 12)],
                    [(t, t) for t in range(3, 15)],
                    [(t, t) for t in [0, 5, 10]]])
        for c in [1, 2, 3, 5, 7, 100]:
            _cmp(r, k, count=c)
            _cmp(r, k, count=c, rev=True)


def test_compare_single_key_equiv_range():
    with _conn() as r:
        k = _mk(r, [[(t, t * t) for t in range(0, 30)]])
        _cmp(r, k)
        _cmp(r, k, rev=True)
        _cmp(r, k, aggs=['sum'], bucket=5)
        _cmp(r, k, aggs=['avg'], bucket=7, rev=True)
        _cmp(r, k, count=4)


def test_compare_duplicate_and_many_keys():
    with _conn() as r:
        # the same key repeated -> every column identical
        k = _mk(r, [[(t, 10 + t) for t in range(6)]])
        _cmp(r, [k[0], k[0], k[0]])
        _cmp(r, [k[0], k[0], k[0]], aggs=['min', 'max', 'avg'], bucket=2)

        # many keys (15) with varied data
        pts = []
        for i in range(15):
            pts.append([(t, i * 100 + t) for t in range(i % 5, i % 5 + 8)])
        k = _mk(r, pts)
        _cmp(r, k)
        _cmp(r, k, rev=True)
        _cmp(r, k, aggs=['sum'], bucket=3)


# ---------------------------------------------------------------------------
# multiple chunks per key
#
# ReplySeriesNRange holds only one "front" sample per key and relies on
# SeriesSampleIterator_GetNext to pull the next chunk transparently when the
# current one is drained. CHUNK_SIZE 48 + UNCOMPRESSED packs only 3 samples per
# chunk, so a handful of samples already spans several chunks. These check that
# the k-way merge stays correct across chunk boundaries (both encodings).
# ---------------------------------------------------------------------------

def test_compare_multi_chunk_uncompressed():
    with _conn() as r:
        # 30 samples/key @ 3 samples/chunk => ~10 chunks each, ragged overlap
        k = _mk(r, [[(t, t) for t in range(0, 30)],
                    [(t, t * 2) for t in range(5, 35)],
                    [(t, -t) for t in [0, 7, 14, 21, 28]]],
                chunk_size=48, enc='UNCOMPRESSED')
        _cmp(r, k); _cmp(r, k, rev=True)
        _cmp(r, k, aggs=['sum', 'min', 'max'], bucket=4)
        _cmp(r, k, aggs=['avg'], bucket=4, rev=True)
        _cmp(r, k, count=7)


def test_compare_multi_chunk_compressed():
    with _conn() as r:
        k = _mk(r, [[(t, (t * 13) % 40 - 5) for t in range(0, 60)],
                    [(t, (t * 7) % 30) for t in range(10, 70)]],
                chunk_size=64, enc='COMPRESSED')
        _cmp(r, k); _cmp(r, k, rev=True)
        _cmp(r, k, aggs=['sum'], bucket=5)
        _cmp(r, k, aggs=['last'], bucket=5, empty=True)


def test_compare_multi_chunk_boundary_alignment():
    with _conn() as r:
        # Chunk boundaries fall at different absolute timestamps per key (offset
        # starts + 3 samples/chunk), so the merge crosses a boundary in one key
        # while mid-chunk in another -> stresses the front/NaN pivot.
        k = _mk(r, [[(t, 100 + t) for t in range(0, 18)],
                    [(t, 200 + t) for t in range(1, 19)],
                    [(t, 300 + t) for t in range(2, 20)]],
                chunk_size=48, enc='UNCOMPRESSED')
        _cmp(r, k); _cmp(r, k, rev=True)
        for c in [1, 2, 3, 4, 5, 9, 100]:
            _cmp(r, k, count=c)
            _cmp(r, k, count=c, rev=True)


def test_compare_multi_chunk_mixed_encoding():
    with _conn() as r:
        # one compressed key + one uncompressed key, both spanning many chunks
        k = _mk(r, [[(t, t) for t in range(0, 40)],
                    [(t, t * 3) for t in range(0, 40)]],
                chunk_size=[48, 64], enc=['UNCOMPRESSED', 'COMPRESSED'])
        _cmp(r, k); _cmp(r, k, rev=True)
        _cmp(r, k, aggs=['min', 'max'], bucket=6)


def test_compare_single_key_multi_chunk():
    with _conn() as r:
        # A single key spanning many chunks must still equal plain TS.RANGE.
        for enc, cs in [('UNCOMPRESSED', 48), ('COMPRESSED', 64)]:
            k = _mk(r, [[(t, (t * t) % 97 - 30) for t in range(0, 80)]],
                    chunk_size=cs, enc=enc)
            _cmp(r, k); _cmp(r, k, rev=True)
            # windows that start/end mid-chunk
            for lo, hi in [('-', '+'), (0, 9), (7, 41), (50, 79), (38, 38)]:
                _cmp(r, k, lo=lo, hi=hi)
                _cmp(r, k, lo=lo, hi=hi, rev=True)
            for agg in ['first', 'last', 'min', 'max', 'sum', 'avg', 'count']:
                _cmp(r, k, aggs=[agg], bucket=7)
                _cmp(r, k, aggs=[agg], bucket=7, rev=True)
                _cmp(r, k, aggs=[agg], bucket=7, empty=True)
            for c in [1, 5, 13, 1000]:
                _cmp(r, k, count=c)
                _cmp(r, k, count=c, rev=True)


def test_compare_multi_chunk_per_key_aggregators():
    with _conn() as r:
        # 6 keys, ragged + tiny chunks (3 samples/chunk) => many chunks each,
        # with a DISTINCT aggregator per key so each column is reduced
        # differently while the merge crosses chunk boundaries.
        pts = []
        for i in range(6):
            pts.append([(t, (t * 7 + i * 5) % 50 - 10) for t in range(i, 45, (i % 2) + 1)])
        k = _mk(r, pts,
                chunk_size=[48, 48, 64, 64, 48, 64],
                enc=['UNCOMPRESSED', 'COMPRESSED', 'UNCOMPRESSED',
                     'COMPRESSED', 'UNCOMPRESSED', 'COMPRESSED'])
        aggs = ['first', 'max', 'min', 'last', 'sum', 'avg']
        for bucket in [1, 4, 7, 10]:
            _cmp(r, k, aggs=aggs, bucket=bucket)
            _cmp(r, k, aggs=aggs, bucket=bucket, rev=True)
            _cmp(r, k, aggs=aggs, bucket=bucket, empty=True)
            _cmp(r, k, aggs=aggs, bucket=bucket, empty=True, rev=True)
        # combined with COUNT
        _cmp(r, k, aggs=aggs, bucket=5, count=6)
        _cmp(r, k, aggs=aggs, bucket=5, count=6, rev=True)


# ---------------------------------------------------------------------------
# aggregation scenarios
# ---------------------------------------------------------------------------

def _agg_dataset(r, nkeys=3):
    # multiple samples per bucket so aggregators differ; ragged across keys
    pts = []
    base = [(t, t) for t in range(0, 40)]
    for i in range(nkeys):
        pts.append([(t, (t * 7 + i * 3) % 50 - 10) for t in range(i, 40, (i % 3) + 1)])
    return _mk(r, pts)


def test_compare_each_aggregator_single_key():
    # (RLTest injects env positionally, so we loop instead of parametrizing.)
    with _conn() as r:
        for agg in AGGS:
            k = _mk(r, [[(t, (t * 13) % 40 - 5) for t in range(0, 50)]])
            for bucket in [1, 3, 10]:
                _cmp(r, k, aggs=[agg], bucket=bucket)
                _cmp(r, k, aggs=[agg], bucket=bucket, rev=True)
                _cmp(r, k, aggs=[agg], bucket=bucket, empty=True)
                _cmp(r, k, aggs=[agg], bucket=bucket, empty=True, rev=True)


def test_compare_per_key_aggregators():
    with _conn() as r:
        k = _agg_dataset(r, 5)
        aggs = ['first', 'max', 'min', 'last', 'sum']
        for bucket in [1, 2, 5, 10]:
            _cmp(r, k, aggs=aggs, bucket=bucket)
            _cmp(r, k, aggs=aggs, bucket=bucket, rev=True)
            _cmp(r, k, aggs=aggs, bucket=bucket, empty=True)
            _cmp(r, k, aggs=aggs, bucket=bucket, empty=True, rev=True)


def test_compare_same_aggregator_per_key():
    with _conn() as r:
        k = _agg_dataset(r, 4)
        for agg in ['avg', 'sum', 'count', 'last']:
            _cmp(r, k, aggs=[agg], bucket=4)
            _cmp(r, k, aggs=[agg], bucket=4, rev=True)
            _cmp(r, k, aggs=[agg], bucket=4, empty=True)


def test_compare_agg_bucket_timestamp():
    with _conn() as r:
        k = _agg_dataset(r, 3)
        for bts in ['start', 'mid', 'end']:
            _cmp(r, k, aggs=['min', 'max', 'avg'], bucket=5, bts=bts)
            _cmp(r, k, aggs=['min', 'max', 'avg'], bucket=5, bts=bts, rev=True)
            _cmp(r, k, aggs=['sum'], bucket=5, bts=bts, empty=True)


def test_compare_agg_align():
    with _conn() as r:
        k = _agg_dataset(r, 3)
        # numeric alignment is always valid; start/end need explicit bounds
        _cmp(r, k, aggs=['sum'], bucket=7, align=3)
        _cmp(r, k, aggs=['sum'], bucket=7, align=3, rev=True)
        _cmp(r, k, lo=0, hi=39, aggs=['min', 'max', 'sum'], bucket=6, align='start')
        _cmp(r, k, lo=0, hi=39, aggs=['min', 'max', 'sum'], bucket=6, align='end')
        _cmp(r, k, lo=0, hi=39, aggs=['avg'], bucket=6, align='start', empty=True)


def test_compare_agg_empty_with_internal_gaps():
    with _conn() as r:
        # big gaps between data => many internal empty buckets
        k = _mk(r, [[(0, 1), (1, 2), (30, 9), (31, 10)],
                    [(0, 5), (15, 6), (31, 7)],
                    [(2, 3), (30, 4)]])
        for agg in ['last', 'first', 'sum', 'avg', 'count', 'min', 'max']:
            _cmp(r, k, lo=0, hi=40, aggs=[agg], bucket=5, empty=True)
            _cmp(r, k, lo=0, hi=40, aggs=[agg], bucket=5, empty=True, rev=True)


# ---------------------------------------------------------------------------
# filters
# ---------------------------------------------------------------------------

def test_compare_filter_by_ts():
    with _conn() as r:
        k = _mk(r, [[(t, 100 + t) for t in range(0, 20)],
                    [(t, 200 + t) for t in range(0, 20, 2)],
                    [(t, 300 + t) for t in [0, 5, 10, 15]]])
        for fbt in [[0], [0, 5, 10, 15], [1, 3, 5, 7, 9], [100, 200]]:
            _cmp(r, k, fbt=fbt)
            _cmp(r, k, fbt=fbt, rev=True)


def test_compare_filter_by_value():
    with _conn() as r:
        k = _mk(r, [[(t, t) for t in range(0, 20)],
                    [(t, 19 - t) for t in range(0, 20)],
                    [(t, (t * 3) % 20) for t in range(0, 20)]])
        for fbv in [(5, 14), (0, 0), (-100, 100), (100, 200), (10, 10)]:
            _cmp(r, k, fbv=fbv)
            _cmp(r, k, fbv=fbv, rev=True)
        # filter_by_value combined with aggregation
        _cmp(r, k, fbv=(5, 14), aggs=['sum', 'avg', 'count'], bucket=4)
        _cmp(r, k, fbv=(5, 14), aggs=['sum'], bucket=4, empty=True)


def test_compare_combined_options():
    with _conn() as r:
        k = _agg_dataset(r, 4)
        _cmp(r, k, lo=2, hi=35, aggs=['min', 'max', 'sum', 'avg'], bucket=4,
             empty=True, bts='mid', count=5)
        _cmp(r, k, lo=2, hi=35, aggs=['last'], bucket=3, empty=True, count=3, rev=True)
        _cmp(r, k, fbv=(-5, 30), aggs=['sum', 'count', 'min', 'max'], bucket=6, count=4)


# ---------------------------------------------------------------------------
# LATEST (compaction destination: partial current bucket)
# ---------------------------------------------------------------------------

def test_compare_latest():
    # LATEST only affects a downsampled (compaction) series: it reports the
    # still-open current bucket. NRANGE must apply it per-key exactly like
    # TS.RANGE -- otherwise the `latest` plumbing is untested.
    with _conn() as r:
        tag = 'rxlatest'
        dests = []
        for i in range(3):
            src, dst = f'{{{tag}}}:src{i}', f'{{{tag}}}:dst{i}'
            r.execute_command('TS.CREATE', src)
            r.execute_command('TS.CREATE', dst)
            r.execute_command('TS.CREATERULE', src, dst, 'AGGREGATION', 'sum', 10)
            # bucket [0,10) closes when 11 arrives; [10,20) stays open (partial),
            # so it is visible ONLY with LATEST. Values differ per key (the +i).
            for ts, v in [(1, 1 + i), (2, 3), (11, 7), (13, 1 + i)]:
                r.execute_command('TS.ADD', src, ts, v)
            dests.append(dst)

        # Without LATEST the open bucket is hidden; with LATEST it appears.
        # NRANGE/NREVRANGE must match per-key TS.RANGE/TS.REVRANGE in both cases.
        _cmp(r, dests)
        _cmp(r, dests, latest=True)
        _cmp(r, dests, latest=True, rev=True)
        _cmp(r, dests, latest=True, aggs=['sum'], bucket=10)


# ---------------------------------------------------------------------------
# fuzz
# ---------------------------------------------------------------------------

def test_compare_fuzz():
    # Randomized stress: each iteration builds a random multi-key dataset + random
    # option set and asserts TS.NRANGE/NREVRANGE == the per-key TS.RANGE/REVRANGE
    # results merged by timestamp. Tunable via env: NRANGE_FUZZ_ITERS (default 200),
    # NRANGE_FUZZ_SEED (default 20260602).
    iters = int(os.getenv('NRANGE_FUZZ_ITERS', '200'))
    rnd = random.Random(int(os.getenv('NRANGE_FUZZ_SEED', '20260602')))
    with _conn() as r:
        for _ in range(iters):
            nkeys = rnd.randint(1, 8)
            # mostly small/dense data (fast); occasionally a larger, sparser pool
            pool_hi, max_samples = rnd.choice([(45, 18), (45, 18), (45, 18), (500, 80)])
            pts = []
            for _ki in range(nkeys):
                ns = rnd.randint(0, min(max_samples, pool_hi))
                row = []
                for ts in sorted(rnd.sample(range(0, pool_hi), ns)):
                    row.append((ts, 'nan') if rnd.random() < 0.08 else (ts, rnd.randint(-1000, 1000)))
                pts.append(row)
            # Half the time force tiny chunks (many chunks/key) with a random
            # per-key encoding, to fuzz the cross-chunk GetNext merge path.
            if rnd.random() < 0.5:
                cs = [rnd.choice([48, 56, 64, 128]) for _ in range(nkeys)]
                enc = [rnd.choice(['COMPRESSED', 'UNCOMPRESSED']) for _ in range(nkeys)]
                k = _mk(r, pts, chunk_size=cs, enc=enc)
            else:
                k = _mk(r, pts)

            rev = rnd.random() < 0.5
            kw = {}
            # window
            lo = '-' if rnd.random() < 0.5 else rnd.randint(0, pool_hi)
            hi = '+' if rnd.random() < 0.5 else (lo if isinstance(lo, int) else 0) + rnd.randint(0, pool_hi)
            kw['lo'], kw['hi'] = lo, hi
            # aggregation: either the same aggregator for every key (a single
            # entry, replicated by _nrange_args) or a distinct one per key
            aggs = None
            if rnd.random() < 0.7:
                aggs = [rnd.choice(AGGS)] if rnd.random() < 0.5 else [rnd.choice(AGGS) for _ in range(nkeys)]
                kw['aggs'] = aggs
                kw['bucket'] = rnd.randint(1, 12)
                if rnd.random() < 0.4:
                    kw['empty'] = True
                if rnd.random() < 0.3:
                    kw['bts'] = rnd.choice(['start', 'mid', 'end'])
                if rnd.random() < 0.3:
                    kw['align'] = rnd.randint(0, 12)
            # value filter — combined with aggregation too
            if rnd.random() < 0.35:
                a = rnd.randint(-1000, 500)
                kw['fbv'] = (a, a + rnd.randint(0, 1500))
            # Skip a PRE-EXISTING engine crash (not nrange): TS.RANGE itself aborts on
            # AGGREGATION twa + FILTER_BY_VALUE + EMPTY (fillEmptyBuckets/ReallocSamplesArray).
            if kw.get('empty') and 'fbv' in kw and aggs and 'twa' in aggs:
                kw['empty'] = False
            # count
            if rnd.random() < 0.4:
                kw['count'] = rnd.randint(1, 20)

            _cmp(r, k, rev=rev, **kw)
