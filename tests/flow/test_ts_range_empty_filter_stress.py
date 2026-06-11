# Stress tests for AGGREGATION + EMPTY combined with FILTER_BY_VALUE / FILTER_BY_TS,
# for every aggregator EXCEPT twa.
#
# Background: MOD-8187 extended EMPTY-bucket filling to all aggregators (not just twa). The
# edge-gap "is there a neighbour?" check scanned the raw series and ignored the value/ts filters,
# so a filtered-out sample beyond the last KEPT sample made an edge gap look interior and the fill
# ran to endTimestamp (UINT64_MAX with '+') -> unsigned overflow -> OOM abort. These tests would
# crash the server pre-fix; they also assert exact values against an INDEPENDENT oracle (computed
# from first principles, never from the engine) so a wrong-value regression is caught too.
#
# twa is intentionally excluded: its interpolation still reads raw (unfiltered) neighbours, a
# separate pre-existing issue tracked apart from the OOM fix.

import math
import random

from RLTest import Env

NON_TWA_AGGS = ['avg', 'sum', 'min', 'max', 'range', 'count', 'countAll',
                'first', 'last', 'std.p', 'std.s', 'var.p', 'var.s']

# empty interior bucket -> 0 for these; 'last' carries the previous value (LOCF); rest -> NaN
_ZERO_EMPTY = {'count', 'countAll', 'sum'}


def _pvar(xs):
    n = len(xs)
    m = sum(xs) / n
    return sum((x - m) ** 2 for x in xs) / n


def _svar(xs):
    n = len(xs)
    if n < 2:
        return 0.0
    m = sum(xs) / n
    return sum((x - m) ** 2 for x in xs) / (n - 1)


_AGG = {
    'avg': lambda xs: sum(xs) / len(xs),
    'sum': lambda xs: float(sum(xs)),
    'min': lambda xs: float(min(xs)),
    'max': lambda xs: float(max(xs)),
    'range': lambda xs: float(max(xs) - min(xs)),
    'count': lambda xs: float(len(xs)),
    'countAll': lambda xs: float(len(xs)),  # data has no NaN, so == count
    'first': lambda xs: float(xs[0]),       # xs are in ascending-ts order
    'last': lambda xs: float(xs[-1]),
    'std.p': lambda xs: math.sqrt(_pvar(xs)),
    'std.s': lambda xs: math.sqrt(_svar(xs)),
    'var.p': lambda xs: _pvar(xs),
    'var.s': lambda xs: _svar(xs),
}


def _oracle(samples, agg, bucket, lo, hi, rev, fbv, fbts, empty, bts):
    """Expected TS.(REV)RANGE output, derived independently of the engine.

    samples: list of (ts, value), value finite (no NaN in these tests).
    Returns list of (label_ts, expected_value_float) in result order.
    """
    lo_n = -(1 << 62) if lo == '-' else lo
    hi_n = (1 << 62) if hi == '+' else hi
    fset = set(fbts) if fbts is not None else None
    kept = sorted((t, v) for t, v in samples
                  if lo_n <= t <= hi_n
                  and (fbv is None or fbv[0] <= v <= fbv[1])
                  and (fset is None or t in fset))
    if not kept:
        return []

    def bs(t):
        return (t // bucket) * bucket

    by = {}
    for t, v in kept:           # kept is sorted -> values land in ascending-ts order
        by.setdefault(bs(t), []).append(v)
    first_b, last_b = bs(kept[0][0]), bs(kept[-1][0])

    rows = []
    last_val = None
    b = first_b
    while b <= last_b:
        if b in by:
            xs = by[b]
            val = _AGG[agg](xs)
            last_val = xs[-1]
        elif not empty:
            b += bucket
            continue            # without EMPTY, interior gaps are not emitted
        elif agg in _ZERO_EMPTY:
            val = 0.0
        elif agg == 'last':
            val = float(last_val)   # LOCF: carry the most recent kept value
        else:
            val = float('nan')
        if bts == 'mid':
            label = b + bucket // 2
        elif bts == 'end':
            label = b + bucket
        else:
            label = b
        rows.append((label, val))
        b += bucket
    if rev:
        rows.reverse()
    return rows


def _cmd(key, agg, bucket, lo, hi, rev, fbv, fbts, empty, bts):
    args = ['TS.REVRANGE' if rev else 'TS.RANGE', key, lo, hi]
    if fbts is not None:
        args += ['FILTER_BY_TS', *fbts]
    if fbv is not None:
        args += ['FILTER_BY_VALUE', fbv[0], fbv[1]]
    args += ['AGGREGATION', agg, bucket]
    if bts is not None:
        args += ['BUCKETTIMESTAMP', bts]
    if empty:
        args += ['EMPTY']
    return args


def _parse_val(raw):
    s = raw.decode() if isinstance(raw, bytes) else str(raw)
    return float('nan') if s.lower() == 'nan' else float(s)


def _eq(a, e):
    if math.isnan(e):
        return math.isnan(a)
    if math.isnan(a):
        return False
    return abs(a - e) <= 1e-6 + 1e-6 * abs(e)


_counter = [0]


def _check(r, samples, agg, bucket, lo='-', hi='+', rev=False,
           fbv=None, fbts=None, empty=True, bts=None):
    _counter[0] += 1
    key = 'fes:%d' % _counter[0]
    r.execute_command('TS.CREATE', key)
    madd = []
    for t, v in samples:
        madd += [key, t, v]
    if madd:
        r.execute_command('TS.MADD', *madd)

    expected = _oracle(samples, agg, bucket, lo, hi, rev, fbv, fbts, empty, bts)
    cmd = _cmd(key, agg, bucket, lo, hi, rev, fbv, fbts, empty, bts)
    actual = r.execute_command(*cmd)
    label = '%s | samples=%s' % (' '.join(str(x) for x in cmd), samples)

    assert len(actual) == len(expected), \
        "row count %d != %d for: %s\n%s\nvs\n%s" % (len(actual), len(expected), label, actual, expected)
    for (arow, erow) in zip(actual, expected):
        ats, av = arow[0], _parse_val(arow[1])
        ets, ev = erow
        assert int(ats) == ets, "ts %s != %s for: %s\n%s\nvs\n%s" % (ats, ets, label, actual, expected)
        assert _eq(av, ev), "value %r != %r at ts %s for: %s\n%s\nvs\n%s" % (av, ev, ats, label, actual, expected)
    r.execute_command('DEL', key)


# ---------------------------------------------------------------------------
# explicit, hand-checked OOM scenarios (one per aggregator)
# ---------------------------------------------------------------------------

def test_oom_filter_strips_suffix_explicit():
    """The exact OOM shape: one kept sample, one filtered-out sample beyond it, open '+' bound,
    EMPTY on. Pre-fix this aborts the server; post-fix the trailing edge gap is dropped."""
    env = Env(decodeResponses=False)
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('FLUSHALL')
        # sample 5 -> 10 is kept by [0,100]; sample 50 -> 9999 is filtered out (beyond, no value)
        for agg in NON_TWA_AGGS:
            _check(r, [(5, 10), (50, 9999)], agg, bucket=4,
                   lo='-', hi='+', fbv=(0, 100), empty=True)
            _check(r, [(5, 10), (50, 9999)], agg, bucket=4,
                   lo='-', hi='+', fbv=(0, 100), empty=True, rev=True)
            _check(r, [(5, 10), (50, 9999)], agg, bucket=4,
                   lo='-', hi='+', fbv=(0, 100), empty=True, bts='end')


def test_filter_strips_prefix_and_interior_explicit():
    """Filter removes the first sample (prefix edge gap dropped) and a middle one (interior gap
    kept as empty)."""
    env = Env(decodeResponses=False)
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('FLUSHALL')
        # values: 1 (removed, prefix), 5 & 7 (kept), 9 (removed, interior between 5 and 7)
        samples = [(2, 1), (10, 5), (20, 9), (30, 7)]
        for agg in NON_TWA_AGGS:
            _check(r, samples, agg, bucket=4, lo='-', hi='+', fbv=(2, 8), empty=True)
            _check(r, samples, agg, bucket=4, lo='-', hi='+', fbv=(2, 8), empty=True, rev=True)
            _check(r, samples, agg, bucket=5, lo='-', hi='+', fbv=(2, 8), empty=True, bts='mid')


def test_filter_by_ts_strips_edges_explicit():
    env = Env(decodeResponses=False)
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('FLUSHALL')
        samples = [(2, 1), (10, 5), (20, 9), (30, 7), (45, 3)]
        # keep only ts 10 and 30 -> 2/20/45 dropped (2 prefix-edge, 45 suffix-edge, 20 interior)
        for agg in NON_TWA_AGGS:
            _check(r, samples, agg, bucket=4, lo='-', hi='+', fbts=[10, 30], empty=True)
            _check(r, samples, agg, bucket=4, lo='-', hi='+', fbts=[10, 30], empty=True, rev=True)


# ---------------------------------------------------------------------------
# randomized stress
# ---------------------------------------------------------------------------

def test_range_empty_filter_stress():
    """Randomized: data + filters that frequently strip edge samples, EMPTY on, open/wide bounds,
    every non-twa aggregator. Asserts engine == independent oracle and that the server survives."""
    import os
    iters = int(os.getenv('EMPTY_FILTER_FUZZ_ITERS', '400'))
    rnd = random.Random(int(os.getenv('EMPTY_FILTER_FUZZ_SEED', '20260611')))

    env = Env(decodeResponses=False)
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('FLUSHALL')
        for _ in range(iters):
            hi_pool = rnd.choice([40, 60, 120])
            n = rnd.randint(1, 14)
            ts = sorted(rnd.sample(range(0, hi_pool), n))
            samples = [(t, rnd.randint(-50, 50)) for t in ts]

            bucket = rnd.randint(1, 9)
            empty = rnd.random() < 0.85           # EMPTY is the OOM trigger -> usually on
            bts = rnd.choice([None, 'start', 'mid', 'end'])
            rev = rnd.random() < 0.5

            # Bounds must fully contain the data so the emitted span is purely data-driven (first
            # kept .. last kept). A numeric lo strictly inside the data, or hi below the last
            # sample, makes leading/trailing partial buckets depend on out-of-window neighbours --
            # a separate query-boundary question, out of scope for the OOM. So: lo <= every ts
            # (0 or '-') and hi >= every ts (hi_pool > max ts, or '+'). This still fully exercises
            # the OOM (open '+' suffix + a filter that strips the last kept sample) and edge drops.
            lo = '-' if rnd.random() < 0.5 else 0
            hi = '+' if rnd.random() < 0.5 else hi_pool

            fbv = None
            fbts = None
            pick = rnd.random()
            if pick < 0.55:
                a = rnd.randint(-50, 40)
                fbv = (a, a + rnd.randint(0, 60))   # often strips edge samples
            elif pick < 0.75 and ts:
                k = rnd.randint(1, len(ts))
                fbts = sorted(rnd.sample(ts, k))

            for agg in NON_TWA_AGGS:
                _check(r, samples, agg, bucket=bucket, lo=lo, hi=hi, rev=rev,
                       fbv=fbv, fbts=fbts, empty=empty, bts=bts)
