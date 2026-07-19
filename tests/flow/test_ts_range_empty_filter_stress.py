# Stress tests for AGGREGATION + EMPTY combined with FILTER_BY_VALUE / FILTER_BY_TS.
#
# Background: MOD-8187 extended EMPTY-bucket filling to all aggregators (not just twa). The
# edge-gap "is there a neighbour?" check scanned the raw series and ignored the value/ts filters,
# so a filtered-out sample beyond the last KEPT sample made an edge gap look interior and the fill
# ran to endTimestamp (UINT64_MAX with '+') -> unsigned overflow -> OOM abort. These tests would
# crash the server pre-fix; they also assert exact values against an INDEPENDENT oracle (computed
# from first principles, never from the engine) so a wrong-value regression is caught too.
#
# twa is covered separately (test_twa_filter_equivalence_stress + the explicit crash test below):
# this PR makes twa's neighbour lookups filter-aware too, so it can't be value-checked against the
# same first-principles oracle (no hand-derived twa integral). Instead we verify it by EQUIVALENCE
# -- twa(full data + filter) == twa(only the kept samples, no filter) -- plus a server-survives
# check on the OOM repro. A full twa value-oracle is tracked as a follow-up.

import math
import random

# Use the includes Env wrapper (NOT `from RLTest import Env`). RLTest's raw Env
# constructor assigns terminateRetries directly with no Defaults fallback, so a
# raw Env gets terminateRetries=None -> _stopProcess() takes the unbounded
# "send one SIGTERM, then wait forever" branch. If a replica is slow to exit on
# shutdown, env teardown then hangs until the per-test timeout (~30 min) and the
# job fails. The includes wrapper passes terminateRetries=20, so teardown
# escalates to SIGKILL after ~20s instead of hanging.
from includes import Env

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


# ---------------------------------------------------------------------------
# twa: filter-equivalence oracle
#
# twa is excluded from the value oracle above (we don't reimplement its integral). Instead we
# verify the filter is applied correctly by EQUIVALENCE: twa over the full data WITH the filter must
# equal twa over only the kept samples WITHOUT a filter. This uses the engine's own (trusted)
# no-filter twa as the reference, so it needs no hand-derived twa math. Bounds may be anything --
# they apply identically to both sides, so the equivalence is independent of query-boundary rules.
# ---------------------------------------------------------------------------

def _kept_for_filter(samples, fbv, fbts):
    fset = set(fbts) if fbts is not None else None
    return [(t, v) for (t, v) in samples
            if (fbv is None or fbv[0] <= v <= fbv[1])
            and (fset is None or t in fset)]


def test_twa_filter_equivalence_stress():
    import os
    iters = int(os.getenv('TWA_FILTER_FUZZ_ITERS', '400'))
    rnd = random.Random(int(os.getenv('TWA_FILTER_FUZZ_SEED', '20260612')))

    env = Env(decodeResponses=False)
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('FLUSHALL')
        kid = 0
        for _ in range(iters):
            hi_pool = rnd.choice([40, 60, 120])
            n = rnd.randint(1, 14)
            ts = sorted(rnd.sample(range(0, hi_pool), n))
            samples = [(t, rnd.randint(-50, 50)) for t in ts]

            bucket = rnd.randint(1, 9)
            empty = rnd.random() < 0.85
            bts = rnd.choice([None, 'start', 'mid', 'end'])
            rev = rnd.random() < 0.5
            lo = '-' if rnd.random() < 0.5 else rnd.randint(0, hi_pool)
            hi = '+' if rnd.random() < 0.5 else (lo if isinstance(lo, int) else 0) + rnd.randint(0, hi_pool)

            # always apply a filter (no-filter would make the equivalence trivial)
            fbv = None
            fbts = None
            if rnd.random() < 0.6 or not ts:
                a = rnd.randint(-50, 40)
                fbv = (a, a + rnd.randint(0, 60))
            else:
                k = rnd.randint(1, len(ts))
                fbts = sorted(rnd.sample(ts, k))

            kept = _kept_for_filter(samples, fbv, fbts)

            kid += 1
            full = 'twf:%d' % kid
            keptk = 'twk:%d' % kid
            r.execute_command('TS.CREATE', full)
            r.execute_command('TS.CREATE', keptk)
            m = []
            for t, v in samples:
                m += [full, t, v]
            if m:
                r.execute_command('TS.MADD', *m)
            m = []
            for t, v in kept:
                m += [keptk, t, v]
            if m:
                r.execute_command('TS.MADD', *m)

            # A: full data WITH the filter
            a_args = ['TS.REVRANGE' if rev else 'TS.RANGE', full, lo, hi]
            if fbts is not None:
                a_args += ['FILTER_BY_TS', *fbts]
            if fbv is not None:
                a_args += ['FILTER_BY_VALUE', fbv[0], fbv[1]]
            a_args += ['AGGREGATION', 'twa', bucket]
            if bts is not None:
                a_args += ['BUCKETTIMESTAMP', bts]
            if empty:
                a_args += ['EMPTY']
            # B: only the kept samples, NO filter
            b_args = ['TS.REVRANGE' if rev else 'TS.RANGE', keptk, lo, hi, 'AGGREGATION', 'twa', bucket]
            if bts is not None:
                b_args += ['BUCKETTIMESTAMP', bts]
            if empty:
                b_args += ['EMPTY']

            a = r.execute_command(*a_args)
            b = r.execute_command(*b_args)
            label = ' '.join(str(x) for x in a_args)
            assert len(a) == len(b), \
                "row count %d \!= %d for: %s\n%s\nvs\n%s" % (len(a), len(b), label, a, b)
            for ra, rb in zip(a, b):
                assert int(ra[0]) == int(rb[0]), \
                    "ts %s \!= %s for: %s\n%s\nvs\n%s" % (ra[0], rb[0], label, a, b)
                assert _eq(_parse_val(ra[1]), _parse_val(rb[1])), \
                    "twa %r \!= %r at ts %s for: %s\n%s\nvs\n%s" % (ra[1], rb[1], ra[0], label, a, b)
            r.execute_command('DEL', full, keptk)


def test_twa_oom_repro_survives():
    """twa hits the same OOM path as the other aggregators; lock in the crash fix with a
    server-survives check (no oracle needed). The [(5,10),(50,9999)] + FILTER_BY_VALUE 0 100 +
    EMPTY shape aborted the server pre-fix: the filtered-out 9999 made the trailing edge gap look
    interior, so the empty fill ran toward '+' (UINT64_MAX) and the bucket count overflowed."""
    env = Env(decodeResponses=False)
    env.skipOnCluster()
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('FLUSHALL')
        r.execute_command('TS.CREATE', 'twaoom')
        r.execute_command('TS.MADD', 'twaoom', 5, 10, 'twaoom', 50, 9999)  # 9999 filtered out
        variants = [
            ['TS.RANGE', 'twaoom', '-', '+', 'FILTER_BY_VALUE', 0, 100,
             'AGGREGATION', 'twa', 4, 'EMPTY'],
            ['TS.REVRANGE', 'twaoom', '-', '+', 'FILTER_BY_VALUE', 0, 100,
             'AGGREGATION', 'twa', 4, 'EMPTY'],
            ['TS.RANGE', 'twaoom', '-', '+', 'FILTER_BY_VALUE', 0, 100,
             'AGGREGATION', 'twa', 4, 'BUCKETTIMESTAMP', 'end', 'EMPTY'],
            ['TS.REVRANGE', 'twaoom', '-', '+', 'FILTER_BY_VALUE', 0, 100,
             'AGGREGATION', 'twa', 4, 'BUCKETTIMESTAMP', 'end', 'EMPTY'],
            ['TS.RANGE', 'twaoom', '-', '+', 'FILTER_BY_TS', 5,
             'AGGREGATION', 'twa', 4, 'EMPTY'],
        ]
        for cmd in variants:
            r.execute_command(*cmd)              # pre-fix: server aborts here -> ConnectionError
            assert r.execute_command('PING')     # must still be alive
