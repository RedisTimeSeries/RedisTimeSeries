# from utils import Env
import math
from includes import *


def test_revrange():
    start_ts = 1511885908
    samples_count = 200
    expected_results = []

    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'tester1', 'uncompressed')
        for i in range(samples_count):
            r.execute_command('TS.ADD', 'tester1', start_ts + i, i)
        actual_results = r.execute_command('TS.RANGE', 'tester1', 0, "+")
        actual_results_rev = r.execute_command('TS.REVRANGE', 'tester1', 0, "+")
        actual_results_rev.reverse()
        assert actual_results == actual_results_rev

        actual_results = r.execute_command('TS.RANGE', 'tester1', 1511885910, 1511886000)
        actual_results_rev = r.execute_command('TS.REVRANGE', 'tester1', 1511885910, 1511886000)
        actual_results_rev.reverse()
        assert actual_results == actual_results_rev

        actual_results = r.execute_command('TS.RANGE', 'tester1', 0, '+', 'AGGREGATION', 'sum', 50)
        actual_results_rev = r.execute_command('TS.REVRANGE', 'tester1', 0, '+', 'AGGREGATION', 'sum', 50)
        actual_results_rev.reverse()
        assert actual_results == actual_results_rev

        # with compression
        r.execute_command('DEL', 'tester1')
        r.execute_command('TS.CREATE', 'tester1')
        for i in range(samples_count):
            r.execute_command('TS.ADD', 'tester1', start_ts + i, i)
        actual_results = r.execute_command('TS.RANGE', 'tester1', 0, '+')
        actual_results_rev = r.execute_command('TS.REVRANGE', 'tester1', 0, '+')
        actual_results_rev.reverse()
        assert actual_results == actual_results_rev

        actual_results = r.execute_command('TS.RANGE', 'tester1', 1511885910, 1511886000)
        actual_results_rev = r.execute_command('TS.REVRANGE', 'tester1', 1511885910, 1511886000)
        actual_results_rev.reverse()
        assert actual_results == actual_results_rev

        actual_results = r.execute_command('TS.RANGE', 'tester1', 0, '+', 'AGGREGATION', 'sum', 50)
        actual_results_rev = r.execute_command('TS.REVRANGE', 'tester1', 0, '+', 'AGGREGATION', 'sum', 50)
        actual_results_rev.reverse()
        assert actual_results == actual_results_rev

        actual_results_rev = r.execute_command('TS.REVRANGE', 'tester1', 0, '+', 'COUNT', 5)
        actual_results = r.execute_command('TS.RANGE', 'tester1', 0, '+')
        actual_results.reverse()
        assert len(actual_results_rev) == 5
        assert actual_results[0:5] == actual_results_rev[0:5]


def test_issue400():
    with Env().getClusterConnectionIfNeeded() as r:
        times = 300
        r.execute_command('ts.create', 'issue376', 'UNCOMPRESSED')
        for i in range(1, times):
            r.execute_command('ts.add', 'issue376', i * 5, i)
        for i in range(1, times):
            range_res = r.execute_command('ts.range', 'issue376', i * 5 - 1, i * 5 + 60)
            assert len(range_res) > 0
        for i in range(1, times):
            range_res = r.execute_command('ts.revrange', 'issue376', i * 5 - 1, i * 5 + 60)
            assert len(range_res) > 0


def test_revrange_aggregation_empty_fill_no_corruption():
    """TS.REVRANGE with AGGREGATION avg 1 EMPTY over a range with a gap (fillEmptyBuckets path).
    Same scenario as test_heap_corruption.sh: data at 191-195 and 346-350, gap in between."""
    # Range and layout (one bucket per ts, bucket_duration=1)
    range_start = 191
    range_end = 350
    first_block_end = 195   # samples 191..195
    second_block_start = 346  # samples 346..350
    expected_bucket_count = range_end - range_start + 1
    chunk_size = 16384

    def has_data(ts):
        return (range_start <= ts <= first_block_end) or (second_block_start <= ts <= range_end)

    with Env().getClusterConnectionIfNeeded() as r:
        key = 'revrange_agg_empty'
        r.execute_command('DEL', key)
        r.execute_command('TS.CREATE', key, 'CHUNK_SIZE', chunk_size)
        for ts in range(1, first_block_end + 1):
            r.execute_command('TS.ADD', key, ts, float(ts))
        for ts in range(second_block_start, range_end + 1):
            r.execute_command('TS.ADD', key, ts, float(ts))
        res = r.execute_command(
            'TS.REVRANGE', key, range_start, range_end, 'AGGREGATION', 'avg', 1, 'EMPTY'
        )
        assert len(res) == expected_bucket_count, f"expected {expected_bucket_count} buckets, got {len(res)}"
        for i, (ts, val) in enumerate(res):
            expected_ts = range_end - i
            assert ts == expected_ts, f"index {i}: expected ts {expected_ts}, got {ts}"
            v = float(val)
            if has_data(ts):
                assert v == float(ts), f"index {i} ts {ts}: expected value {ts}, got {v}"
            else:
                assert math.isnan(v), f"index {i} ts {ts}: expected NaN (empty bucket), got {v}"
        r.ping()
