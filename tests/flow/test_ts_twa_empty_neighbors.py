# TWA + EMPTY on a bucket that holds only NaN samples, when the samples needed to interpolate it are
# missing on one side or lie outside the query range.
#
# A NaN-only bucket [ta, tb] (clipped to the query range) is filled from the two nearest non-NaN
# samples on each side, looked up in the whole series, not only inside the query range. When one
# side has no sample at all, the other side needs two samples, s1 (nearest) and s2, with
# gap = |s2 - s1|. Integer division is used for gap / 2:
#   - only samples after:  NaN if tb + gap <= s1 or tb + gap / 2 <= s1, else the value of s1;
#   - only samples before: NaN if s1 + gap <= ta or s1 + gap / 2 <= ta, else the value of s1;
#   - a single sample on the only side: NaN.
# A bucket with real samples is the trapezoid integral of its samples divided by its span.
# All expected values below are worked out by hand from these rules.

from includes import *

BUCKET = 10

SERIES = {
    # Nothing before the NaN bucket [0, 10); two samples after it.
    'lead': [(0, 'NaN'), (10, 1), (14, 3)],
    # Same, shifted 1 ms later: s1 = 11.
    'lead_late': [(0, 'NaN'), (11, 1), (15, 3)],
    # Only one sample after.
    'lead_single': [(0, 'NaN'), (10, 1)],
    # Nothing after the NaN bucket [10, 20); two samples before it.
    'trail': [(0, 1), (8, 3), (10, 'NaN')],
    # s2 at 2: gap 6, gap / 2 = 3.
    'trail_gap6': [(2, 1), (8, 3), (10, 'NaN')],
    # s2 at 3: gap 5, gap / 2 = 2 (integer division).
    'trail_gap5': [(3, 1), (8, 3), (10, 'NaN')],
    # Only one sample before.
    'trail_single': [(8, 3), (10, 'NaN')],
}


class testTwaEmptyNeighbors:
    def __init__(self):
        self.env = Env(decodeResponses=True)
        with self.env.getClusterConnectionIfNeeded() as r:
            for key, samples in SERIES.items():
                r.execute_command('TS.CREATE', key)
                for ts, value in samples:
                    r.execute_command('TS.ADD', key, ts, value)

    def _assert_both_directions(self, key, start, end, expected, agg='twa', *extra):
        with self.env.getClusterConnectionIfNeeded() as r:
            fwd = r.execute_command('TS.RANGE', key, start, end, 'AGGREGATION', agg, BUCKET, 'EMPTY', *extra)
            rev = r.execute_command('TS.REVRANGE', key, start, end, 'AGGREGATION', agg, BUCKET, 'EMPTY', *extra)
        self.env.assertEqual(fwd, expected, message=f'TS.RANGE {key} {start} {end} {agg} {extra}')
        self.env.assertEqual(rev, expected[::-1], message=f'TS.REVRANGE {key} {start} {end} {agg} {extra}')

    def test_leading_nan_bucket_uses_samples_after_the_query(self):
        # Query [0, 9]: tb = 9. s1 = 10, s2 = 14, gap 4. 9 + 4 = 13 > 10 and 9 + 2 = 11 > 10,
        # so the bucket takes s1's value. Both samples are outside the query.
        self._assert_both_directions('lead', 0, 9, [[0, '1']])
        # Query [0, 19] adds the real bucket [10, 20): trapezoid (1 + 3) / 2 * 4 / 4 = 2. The NaN
        # bucket before it gives it no left neighbor, and nothing follows 14.
        self._assert_both_directions('lead', 0, 19, [[0, '1'], [10, '2']])
        self._assert_both_directions('lead', 0, 19, [[0, 'NaN', '1'], [10, '2', '2']], 'avg,twa')

    def test_leading_nan_bucket_half_gap_boundary(self):
        # s1 = 11, s2 = 15, gap 4. Query end 9: tb = 9, 9 + 2 = 11 <= 11 -> NaN.
        self._assert_both_directions('lead_late', 0, 9, [[0, 'NaN']])
        # Query end 10: tb = 10, 10 + 4 = 14 > 11 and 10 + 2 = 12 > 11 -> s1's value. The empty
        # bucket [10, 20) has no non-NaN sample before it, so it is dropped as an edge gap.
        self._assert_both_directions('lead_late', 0, 10, [[0, '1']])

    def test_trailing_nan_bucket_uses_samples_before_the_query(self):
        # Query [10, 19]: ta = 10. s1 = 8, s2 = 0, gap 8. 8 + 8 = 16 > 10 and 8 + 4 = 12 > 10,
        # so the bucket takes s1's value. Both samples are outside the query.
        self._assert_both_directions('trail', 10, 19, [[10, '3']])
        # Query [0, 19] adds the real bucket [0, 10): trapezoid (1 + 3) / 2 * 8 / 8 = 2.
        self._assert_both_directions('trail', 0, 19, [[0, '2'], [10, '3']])
        self._assert_both_directions('trail', 10, 19, [[10, 'NaN', '3']], 'avg,twa')
        # BUCKETTIMESTAMP moves only the label, not ta / tb.
        self._assert_both_directions('trail', 10, 19, [[15, '3']], 'twa', 'BUCKETTIMESTAMP', 'mid')
        self._assert_both_directions('trail', 10, 19, [[20, '3']], 'twa', 'BUCKETTIMESTAMP', 'end')

    def test_trailing_nan_bucket_half_gap_boundary(self):
        # ta = 10, s1 = 8. gap 6: 8 + 3 = 11 > 10 -> s1's value.
        self._assert_both_directions('trail_gap6', 10, 19, [[10, '3']])
        # gap 5: 5 / 2 = 2, 8 + 2 = 10 <= 10 -> NaN.
        self._assert_both_directions('trail_gap5', 10, 19, [[10, 'NaN']])

    def test_single_neighbor_on_the_only_side_gives_nan(self):
        # One sample is not enough to measure a gap, so the bucket stays NaN.
        self._assert_both_directions('lead_single', 0, 9, [[0, 'NaN']])
        self._assert_both_directions('trail_single', 10, 19, [[10, 'NaN']])
