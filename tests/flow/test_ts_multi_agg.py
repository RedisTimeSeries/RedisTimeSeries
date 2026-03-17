import pytest
import redis
from includes import *


def test_multi_agg_basic_range():
    """Test TS.RANGE with multiple aggregators (min,max)."""
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_basic{a}')
        for i in range(10):
            r.execute_command('TS.ADD', 'magg_basic{a}', 1000 + i, 100 + i)
        for i in range(10):
            r.execute_command('TS.ADD', 'magg_basic{a}', 2000 + i, 200 + i)

        result = r.execute_command('TS.RANGE', 'magg_basic{a}', '-', '+', 'AGGREGATION', 'min,max', 1000)
        assert len(result) == 2
        assert result[0][0] == 1000
        assert result[0][1] == b'100'
        assert result[0][2] == b'109'
        assert result[1][0] == 2000
        assert result[1][1] == b'200'
        assert result[1][2] == b'209'


def test_multi_agg_three_aggregators():
    """Test TS.RANGE with three aggregators (min,max,avg)."""
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_three{a}')
        for i in range(10):
            r.execute_command('TS.ADD', 'magg_three{a}', 1000 + i, 100 + i)

        result = r.execute_command('TS.RANGE', 'magg_three{a}', '-', '+', 'AGGREGATION', 'min,max,avg', 1000)
        assert len(result) == 1
        assert result[0][0] == 1000
        assert result[0][1] == b'100'
        assert result[0][2] == b'109'
        assert float(result[0][3]) == pytest.approx(104.5, abs=0.01)


def test_multi_agg_revrange():
    """Test TS.REVRANGE with multiple aggregators."""
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_rev{a}')
        for i in range(10):
            r.execute_command('TS.ADD', 'magg_rev{a}', 1000 + i, 100 + i)
        for i in range(10):
            r.execute_command('TS.ADD', 'magg_rev{a}', 2000 + i, 200 + i)

        result = r.execute_command('TS.REVRANGE', 'magg_rev{a}', '-', '+', 'AGGREGATION', 'min,max', 1000)
        assert len(result) == 2
        assert result[0][0] == 2000
        assert result[0][1] == b'200'
        assert result[0][2] == b'209'
        assert result[1][0] == 1000
        assert result[1][1] == b'100'
        assert result[1][2] == b'109'


def test_multi_agg_mrange():
    """Test TS.MRANGE with multiple aggregators."""
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_mr1{a}', 'LABELS', 'type', 'maggstock')
        r.execute_command('TS.CREATE', 'magg_mr2{a}', 'LABELS', 'type', 'maggstock')
        for i in range(10):
            r.execute_command('TS.ADD', 'magg_mr1{a}', 1000 + i, 100 + i)
            r.execute_command('TS.ADD', 'magg_mr2{a}', 1000 + i, 200 + i)

        result = r.execute_command('TS.MRANGE', '-', '+',
                                   'AGGREGATION', 'min,max', 1000,
                                   'FILTER', 'type=maggstock')
        assert len(result) == 2

        series_by_key = {s[0]: s for s in result}
        s1 = series_by_key[b'magg_mr1{a}']
        s2 = series_by_key[b'magg_mr2{a}']

        assert s1[2] == [[1000, b'100', b'109']]
        assert s2[2] == [[1000, b'200', b'209']]


def test_multi_agg_mrevrange():
    """Test TS.MREVRANGE with multiple aggregators."""
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_mrv{a}', 'LABELS', 'type', 'maggstock2')
        for i in range(10):
            r.execute_command('TS.ADD', 'magg_mrv{a}', 1000 + i, 100 + i)
        for i in range(10):
            r.execute_command('TS.ADD', 'magg_mrv{a}', 2000 + i, 200 + i)

        result = r.execute_command('TS.MREVRANGE', '-', '+',
                                   'AGGREGATION', 'sum,count', 1000,
                                   'FILTER', 'type=maggstock2')
        assert len(result) == 1
        series = result[0]
        assert series[0] == b'magg_mrv{a}'
        data_points = series[2]
        assert len(data_points) == 2
        # reversed order: bucket 2000 first, then 1000
        assert data_points[0][0] == 2000
        assert data_points[0][1] == b'2045'  # sum(200..209)
        assert data_points[0][2] == b'10'    # count
        assert data_points[1][0] == 1000
        assert data_points[1][1] == b'1045'  # sum(100..109)
        assert data_points[1][2] == b'10'    # count


def test_multi_agg_groupby_error():
    """Multiple aggregators with GROUPBY should return an error."""
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_grp{a}', 'LABELS', 'type', 'maggrp')
        r.execute_command('TS.ADD', 'magg_grp{a}', 1000, 100)

        with pytest.raises(redis.ResponseError, match="GROUPBY"):
            r.execute_command('TS.MRANGE', '-', '+',
                              'AGGREGATION', 'min,max', 1000,
                              'FILTER', 'type=maggrp',
                              'GROUPBY', 'type', 'REDUCE', 'max')


def test_multi_agg_groupby_single_ok():
    """Single aggregator with GROUPBY should still work."""
    e = Env()
    e.skipOnCluster()
    with e.getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_grps1{a}', 'LABELS', 'type', 'maggrps')
        r.execute_command('TS.CREATE', 'magg_grps2{a}', 'LABELS', 'type', 'maggrps')
        r.execute_command('TS.ADD', 'magg_grps1{a}', 1000, 100)
        r.execute_command('TS.ADD', 'magg_grps1{a}', 1001, 200)
        r.execute_command('TS.ADD', 'magg_grps2{a}', 1000, 150)
        r.execute_command('TS.ADD', 'magg_grps2{a}', 1001, 50)

        result = r.execute_command('TS.MRANGE', '-', '+',
                                   'AGGREGATION', 'max', 1000,
                                   'FILTER', 'type=maggrps',
                                   'GROUPBY', 'type', 'REDUCE', 'max')
        assert len(result) == 1
        group = result[0]
        assert group[0] == b'type=maggrps'
        data_points = group[2]
        assert len(data_points) == 1
        assert data_points[0][0] == 1000
        assert data_points[0][1] == b'200'  # max(200, 150) across the two series


def test_multi_agg_with_count():
    """COUNT should limit the number of buckets returned."""
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_cnt{a}')
        for i in range(50):
            r.execute_command('TS.ADD', 'magg_cnt{a}', 1000 + i, i)

        # Without COUNT: 5 buckets (1000-1009, 1010-1019, 1020-1029, 1030-1039, 1040-1049)
        full_result = r.execute_command('TS.RANGE', 'magg_cnt{a}', '-', '+',
                                        'AGGREGATION', 'min,max', 10)
        assert len(full_result) == 5
        assert full_result[0] == [1000, b'0', b'9']
        assert full_result[1] == [1010, b'10', b'19']

        # With COUNT 2: only first 2 buckets
        result = r.execute_command('TS.RANGE', 'magg_cnt{a}', '-', '+',
                                   'AGGREGATION', 'min,max', 10, 'COUNT', 2)
        assert len(result) == 2
        assert result[0] == [1000, b'0', b'9']
        assert result[1] == [1010, b'10', b'19']


def test_multi_agg_sum_count():
    """Test sum,count aggregation."""
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_sc{a}')
        r.execute_command('TS.ADD', 'magg_sc{a}', 1000, 10)
        r.execute_command('TS.ADD', 'magg_sc{a}', 1001, 20)
        r.execute_command('TS.ADD', 'magg_sc{a}', 1002, 30)

        result = r.execute_command('TS.RANGE', 'magg_sc{a}', '-', '+', 'AGGREGATION', 'sum,count', 1000)
        assert len(result) == 1
        assert result[0][0] == 1000
        assert result[0][1] == b'60'
        assert result[0][2] == b'3'


def test_multi_agg_candlestick():
    """Test min,max,first,last aggregation for candlestick charts."""
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_cs{a}')
        r.execute_command('TS.ADD', 'magg_cs{a}', 1000, 50)
        r.execute_command('TS.ADD', 'magg_cs{a}', 1001, 30)
        r.execute_command('TS.ADD', 'magg_cs{a}', 1002, 80)
        r.execute_command('TS.ADD', 'magg_cs{a}', 1003, 60)

        result = r.execute_command('TS.RANGE', 'magg_cs{a}', '-', '+',
                                   'AGGREGATION', 'min,max,first,last', 1000)
        assert len(result) == 1
        assert result[0][0] == 1000
        assert result[0][1] == b'30'
        assert result[0][2] == b'80'
        assert result[0][3] == b'50'
        assert result[0][4] == b'60'


def test_multi_agg_invalid_type_error():
    """Invalid aggregation type in the list should return error."""
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_inv{a}')
        r.execute_command('TS.ADD', 'magg_inv{a}', 1000, 100)

        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.RANGE', 'magg_inv{a}', '-', '+',
                              'AGGREGATION', 'min,invalid', 1000)


def test_multi_agg_empty_type_error():
    """Empty aggregation type (trailing comma) should return error."""
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_empty{a}')
        r.execute_command('TS.ADD', 'magg_empty{a}', 1000, 100)

        with pytest.raises(redis.ResponseError):
            r.execute_command('TS.RANGE', 'magg_empty{a}', '-', '+',
                              'AGGREGATION', 'min,', 1000)


def test_multi_agg_duplicate_types():
    """Duplicate aggregation types should be allowed."""
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_dup{a}')
        r.execute_command('TS.ADD', 'magg_dup{a}', 1000, 100)

        result = r.execute_command('TS.RANGE', 'magg_dup{a}', '-', '+',
                                   'AGGREGATION', 'min,min', 1000)
        assert len(result) == 1
        assert result[0][1] == b'100'
        assert result[0][2] == b'100'


def test_multi_agg_with_twa():
    """Test multi-agg when one of the aggregators is TWA."""
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_twa{a}')
        r.execute_command('TS.ADD', 'magg_twa{a}', 8, 8)
        r.execute_command('TS.ADD', 'magg_twa{a}', 9, 9)
        r.execute_command('TS.ADD', 'magg_twa{a}', 10, 10)
        r.execute_command('TS.ADD', 'magg_twa{a}', 13, 13)
        r.execute_command('TS.ADD', 'magg_twa{a}', 14, 14)
        r.execute_command('TS.ADD', 'magg_twa{a}', 23, 23)

        # Compute expected TWA for bucket [10,20) (same as test_agg_twa case 1)
        v1, v2, v3, v4, v5 = 9.0, 10.0, 13.0, 14.0, 23.0
        t1, t2, t3, t4, t5 = 9.0, 10.0, 13.0, 14.0, 23.0
        ta, tb = 10.0, 20.0
        va = v1 + (v2 - v1) * (ta - t1) / (t2 - t1)
        vb = v4 + (v5 - v4) * (tb - t4) / (t5 - t4)
        s = (va + v2) * (t2 - ta) + (v2 + v3) * (t3 - t2) + (v3 + v4) * (t4 - t3) + (vb + v4) * (tb - t4)
        expected_twa = s / (2 * (tb - ta))

        result = r.execute_command('TS.RANGE', 'magg_twa{a}', 10, 20,
                                   'AGGREGATION', 'min,max,twa', 10)
        assert len(result) == 1
        assert result[0][0] == 10
        assert result[0][1] == b'10'    # min
        assert result[0][2] == b'14'    # max
        assert result[0][3] == str(int(expected_twa)).encode('ascii')  # twa


def test_multi_agg_twa_not_first_multi_bucket():
    """Test that TWA produces correct results in multi-agg when it is NOT the first aggregation
    and there are multiple buckets (exercising the bucket-boundary getLastSample code path)."""
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_twa_nf{a}')
        # Data spanning 3 buckets of size 10: [0,10), [10,20), [20,30)
        r.execute_command('TS.ADD', 'magg_twa_nf{a}', 2, 5)
        r.execute_command('TS.ADD', 'magg_twa_nf{a}', 7, 10)
        r.execute_command('TS.ADD', 'magg_twa_nf{a}', 12, 20)
        r.execute_command('TS.ADD', 'magg_twa_nf{a}', 17, 30)
        r.execute_command('TS.ADD', 'magg_twa_nf{a}', 22, 40)
        r.execute_command('TS.ADD', 'magg_twa_nf{a}', 27, 50)

        # Get reference TWA values from single-agg query
        twa_only = r.execute_command('TS.RANGE', 'magg_twa_nf{a}', '-', '+',
                                     'AGGREGATION', 'twa', 10)
        assert len(twa_only) == 3

        # TWA as second aggregation (non-first position triggers the bug)
        result = r.execute_command('TS.RANGE', 'magg_twa_nf{a}', '-', '+',
                                   'AGGREGATION', 'sum,twa', 10)
        assert len(result) == 3
        for i in range(3):
            assert result[i][0] == twa_only[i][0]
            assert float(result[i][2]) == pytest.approx(float(twa_only[i][1]), abs=0.01), \
                f"TWA mismatch in bucket {i}: multi-agg={result[i][2]}, single={twa_only[i][1]}"

        # Also verify sum values are correct
        assert float(result[0][1]) == pytest.approx(15.0, abs=0.01)   # 5 + 10
        assert float(result[1][1]) == pytest.approx(50.0, abs=0.01)   # 20 + 30
        assert float(result[2][1]) == pytest.approx(90.0, abs=0.01)   # 40 + 50

        # Same test with REVRANGE
        twa_only_rev = r.execute_command('TS.REVRANGE', 'magg_twa_nf{a}', '-', '+',
                                         'AGGREGATION', 'twa', 10)
        result_rev = r.execute_command('TS.REVRANGE', 'magg_twa_nf{a}', '-', '+',
                                       'AGGREGATION', 'sum,twa', 10)
        assert len(result_rev) == 3
        for i in range(3):
            assert result_rev[i][0] == twa_only_rev[i][0]
            assert float(result_rev[i][2]) == pytest.approx(float(twa_only_rev[i][1]), abs=0.01), \
                f"REVRANGE TWA mismatch in bucket {i}: multi-agg={result_rev[i][2]}, single={twa_only_rev[i][1]}"

        # Test with TWA in the middle: avg,twa,count
        result_mid = r.execute_command('TS.RANGE', 'magg_twa_nf{a}', '-', '+',
                                       'AGGREGATION', 'avg,twa,count', 10)
        assert len(result_mid) == 3
        for i in range(3):
            assert float(result_mid[i][2]) == pytest.approx(float(twa_only[i][1]), abs=0.01), \
                f"TWA-in-middle mismatch in bucket {i}"


def test_multi_agg_empty_buckets_with_gaps():
    """Test multi-agg with EMPTY flag and gaps between populated buckets.

    This exercises fillEmptyBuckets in the multiAgg path where the output is
    written to aux_chunk (a separate buffer) rather than in-place.
    """
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_gap{a}')
        # Bucket [1000, 2000): values 100-109
        for i in range(10):
            r.execute_command('TS.ADD', 'magg_gap{a}', 1000 + i, 100 + i)
        # Bucket [2000, 3000): empty (gap)
        # Bucket [3000, 4000): values 300-309
        for i in range(10):
            r.execute_command('TS.ADD', 'magg_gap{a}', 3000 + i, 300 + i)
        # Bucket [4000, 5000): empty (gap)
        # Bucket [5000, 6000): values 500-509
        for i in range(10):
            r.execute_command('TS.ADD', 'magg_gap{a}', 5000 + i, 500 + i)

        agg_types = ['min', 'max', 'count']
        multi_agg_str = ','.join(agg_types)

        # Verify multi-agg RANGE with EMPTY matches individual single-agg results
        refs = [r.execute_command('TS.RANGE', 'magg_gap{a}', '-', '+',
                                  'AGGREGATION', agg, 1000, 'EMPTY') for agg in agg_types]
        result = r.execute_command('TS.RANGE', 'magg_gap{a}', '-', '+',
                                   'AGGREGATION', multi_agg_str, 1000, 'EMPTY')
        assert len(result) == len(refs[0]), \
            f"bucket count mismatch: multi-agg={len(result)}, single-agg={len(refs[0])}"
        for i in range(len(result)):
            assert result[i][0] == refs[0][i][0], f"timestamp mismatch at bucket {i}"
            for a, agg in enumerate(agg_types):
                assert result[i][1 + a] == refs[a][i][1], \
                    f"{agg} mismatch at bucket {i}: multi-agg={result[i][1 + a]}, single={refs[a][i][1]}"

        # Verify multi-agg REVRANGE with EMPTY matches individual single-agg results
        refs_rev = [r.execute_command('TS.REVRANGE', 'magg_gap{a}', '-', '+',
                                      'AGGREGATION', agg, 1000, 'EMPTY') for agg in agg_types]
        result_rev = r.execute_command('TS.REVRANGE', 'magg_gap{a}', '-', '+',
                                       'AGGREGATION', multi_agg_str, 1000, 'EMPTY')
        assert len(result_rev) == len(refs_rev[0]), \
            f"REVRANGE bucket count mismatch: multi-agg={len(result_rev)}, single-agg={len(refs_rev[0])}"
        for i in range(len(result_rev)):
            assert result_rev[i][0] == refs_rev[0][i][0], f"REVRANGE timestamp mismatch at bucket {i}"
            for a, agg in enumerate(agg_types):
                assert result_rev[i][1 + a] == refs_rev[a][i][1], \
                    f"REVRANGE {agg} mismatch at bucket {i}: multi-agg={result_rev[i][1 + a]}, single={refs_rev[a][i][1]}"


def test_multi_agg_twa_empty_prefix_gap():
    """Test multi-agg with TWA + EMPTY where there are empty buckets before the first data point.

    This exercises the TWA_EMPTY_RANGE prefix path in AggregationIterator_GetNextChunk,
    ensuring the output goes to aux_chunk (not enrichedChunk) when multiAgg is true.
    """
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_twa_pfx{a}')
        # Data starts at timestamp 30, query starts at 0 → buckets [0,10) and [10,20) are empty prefix
        r.execute_command('TS.ADD', 'magg_twa_pfx{a}', 30, 100)
        r.execute_command('TS.ADD', 'magg_twa_pfx{a}', 35, 200)
        r.execute_command('TS.ADD', 'magg_twa_pfx{a}', 45, 300)

        agg_types = ['min', 'max', 'twa']
        multi_agg_str = ','.join(agg_types)

        # Get single-agg reference results
        refs = [r.execute_command('TS.RANGE', 'magg_twa_pfx{a}', 0, 50,
                                  'AGGREGATION', agg, 10, 'EMPTY') for agg in agg_types]

        result = r.execute_command('TS.RANGE', 'magg_twa_pfx{a}', 0, 50,
                                   'AGGREGATION', multi_agg_str, 10, 'EMPTY')

        assert len(result) == len(refs[0]), \
            f"bucket count mismatch: multi-agg={len(result)}, single-agg={len(refs[0])}"
        for i in range(len(result)):
            assert result[i][0] == refs[0][i][0], f"timestamp mismatch at bucket {i}"
            for a, agg in enumerate(agg_types):
                assert result[i][1 + a] == refs[a][i][1], \
                    f"{agg} mismatch at bucket {i}: multi-agg={result[i][1 + a]}, single={refs[a][i][1]}"

        # Also test REVRANGE
        refs_rev = [r.execute_command('TS.REVRANGE', 'magg_twa_pfx{a}', 0, 50,
                                      'AGGREGATION', agg, 10, 'EMPTY') for agg in agg_types]
        result_rev = r.execute_command('TS.REVRANGE', 'magg_twa_pfx{a}', 0, 50,
                                       'AGGREGATION', multi_agg_str, 10, 'EMPTY')

        assert len(result_rev) == len(refs_rev[0]), \
            f"REVRANGE bucket count mismatch: multi-agg={len(result_rev)}, single-agg={len(refs_rev[0])}"
        for i in range(len(result_rev)):
            assert result_rev[i][0] == refs_rev[0][i][0], f"REVRANGE timestamp mismatch at bucket {i}"
            for a, agg in enumerate(agg_types):
                assert result_rev[i][1 + a] == refs_rev[a][i][1], \
                    f"REVRANGE {agg} mismatch at bucket {i}: multi-agg={result_rev[i][1 + a]}, single={refs_rev[a][i][1]}"


def test_multi_agg_countnan_mixed():
    """Test multi-agg mixing countnan/countall with standard aggregations.

    countnan uses nanValueValid (only NaN passes), countall uses allValueValid (everything passes).
    The per-aggregation isValueValid gate must be checked independently for each aggregation,
    not just the first one.
    """
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_cnan{a}')
        r.execute_command('TS.ADD', 'magg_cnan{a}', 1000, 10)
        r.execute_command('TS.ADD', 'magg_cnan{a}', 1001, 'NaN')
        r.execute_command('TS.ADD', 'magg_cnan{a}', 1002, 20)
        r.execute_command('TS.ADD', 'magg_cnan{a}', 1003, 'NaN')
        r.execute_command('TS.ADD', 'magg_cnan{a}', 1004, 30)

        agg_types = ['sum', 'countnan', 'countall']
        multi_agg_str = ','.join(agg_types)

        refs = [r.execute_command('TS.RANGE', 'magg_cnan{a}', '-', '+',
                                  'AGGREGATION', agg, 1000) for agg in agg_types]

        result = r.execute_command('TS.RANGE', 'magg_cnan{a}', '-', '+',
                                   'AGGREGATION', multi_agg_str, 1000)

        assert len(result) == len(refs[0]), \
            f"bucket count mismatch: multi-agg={len(result)}, single-agg={len(refs[0])}"
        for i in range(len(result)):
            assert result[i][0] == refs[0][i][0], f"timestamp mismatch at bucket {i}"
            for a, agg in enumerate(agg_types):
                assert result[i][1 + a] == refs[a][i][1], \
                    f"{agg} mismatch at bucket {i}: multi-agg={result[i][1 + a]}, single={refs[a][i][1]}"

        # Also test with countnan as the FIRST aggregation (gates all others in buggy code)
        agg_types2 = ['countnan', 'min', 'max']
        multi_agg_str2 = ','.join(agg_types2)

        refs2 = [r.execute_command('TS.RANGE', 'magg_cnan{a}', '-', '+',
                                   'AGGREGATION', agg, 1000) for agg in agg_types2]

        result2 = r.execute_command('TS.RANGE', 'magg_cnan{a}', '-', '+',
                                    'AGGREGATION', multi_agg_str2, 1000)

        assert len(result2) == len(refs2[0]), \
            f"bucket count mismatch (countnan-first): multi-agg={len(result2)}, single-agg={len(refs2[0])}"
        for i in range(len(result2)):
            assert result2[i][0] == refs2[0][i][0], f"timestamp mismatch at bucket {i}"
            for a, agg in enumerate(agg_types2):
                assert result2[i][1 + a] == refs2[a][i][1], \
                    f"{agg} mismatch at bucket {i} (countnan-first): multi-agg={result2[i][1 + a]}, single={refs2[a][i][1]}"


def test_multi_agg_nan_only_bucket_mixed_validators():
    """Test multi-agg where a bucket has ONLY NaN values and validators disagree.

    When mixing countall (accepts NaN) with min (rejects NaN), a NaN-only bucket must
    use finalizeEmpty for min (producing NaN) rather than finalize (which would produce
    DBL_MAX from an empty context). This verifies per-aggregation validity tracking.

    Uses EMPTY so all aggregations produce the same bucket count in both single-agg
    and multi-agg modes (without EMPTY, single-agg min/max skip NaN-only buckets while
    multi-agg emits them because countall had valid samples).
    """
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_nanonly{a}')
        # Bucket [1000, 2000): normal values
        r.execute_command('TS.ADD', 'magg_nanonly{a}', 1000, 10)
        r.execute_command('TS.ADD', 'magg_nanonly{a}', 1001, 20)
        # Bucket [2000, 3000): only NaN values
        r.execute_command('TS.ADD', 'magg_nanonly{a}', 2000, 'NaN')
        r.execute_command('TS.ADD', 'magg_nanonly{a}', 2001, 'NaN')
        # Bucket [3000, 4000): normal values again
        r.execute_command('TS.ADD', 'magg_nanonly{a}', 3000, 30)

        agg_types = ['countall', 'min', 'max']
        multi_agg_str = ','.join(agg_types)

        refs = [r.execute_command('TS.RANGE', 'magg_nanonly{a}', '-', '+',
                                  'AGGREGATION', agg, 1000, 'EMPTY') for agg in agg_types]
        result = r.execute_command('TS.RANGE', 'magg_nanonly{a}', '-', '+',
                                   'AGGREGATION', multi_agg_str, 1000, 'EMPTY')

        assert len(result) == len(refs[0]), \
            f"bucket count mismatch: multi-agg={len(result)}, single-agg={len(refs[0])}"
        for i in range(len(result)):
            assert result[i][0] == refs[0][i][0], f"timestamp mismatch at bucket {i}"
            for a, agg in enumerate(agg_types):
                assert result[i][1 + a] == refs[a][i][1], \
                    f"{agg} mismatch at bucket {i}: multi-agg={result[i][1 + a]}, single={refs[a][i][1]}"


def test_multi_agg_countnan_first_with_twa():
    """Test that TWA init uses correct validator when countnan is the first aggregation.

    When aggs are 'countnan,twa', the TWA init loop must NOT use countnan's
    nanValueValid (only NaN passes) to find the prev-bucket sample for interpolation.
    It must use non-NaN validity (what TWA needs) regardless of aggregation order.

    Uses EMPTY so all aggregations produce the same bucket count (without EMPTY,
    single-agg countnan skips buckets with no NaN values while multi-agg emits them
    because TWA had valid samples).
    """
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_cntwa{a}')
        r.execute_command('TS.ADD', 'magg_cntwa{a}', 5, 50)
        r.execute_command('TS.ADD', 'magg_cntwa{a}', 15, 150)
        r.execute_command('TS.ADD', 'magg_cntwa{a}', 25, 250)

        agg_types = ['countnan', 'twa']
        multi_agg_str = ','.join(agg_types)

        refs = [r.execute_command('TS.RANGE', 'magg_cntwa{a}', 10, 30,
                                  'AGGREGATION', agg, 10, 'EMPTY') for agg in agg_types]
        result = r.execute_command('TS.RANGE', 'magg_cntwa{a}', 10, 30,
                                   'AGGREGATION', multi_agg_str, 10, 'EMPTY')

        assert len(result) == len(refs[0]), \
            f"bucket count mismatch: multi-agg={len(result)}, single-agg={len(refs[0])}"
        for i in range(len(result)):
            assert result[i][0] == refs[0][i][0], f"timestamp mismatch at bucket {i}"
            for a, agg in enumerate(agg_types):
                assert result[i][1 + a] == refs[a][i][1], \
                    f"{agg} mismatch at bucket {i}: multi-agg={result[i][1 + a]}, single={refs[a][i][1]}"


def test_multi_agg_twa_nan_only_bucket_with_countall():
    """TWA must get interpolated value (not finalizeEmpty) in a NaN-only bucket when
    another aggregation with a different validator accepts the NaN values.

    Scenario: countall,twa with a bucket containing only NaN values.
    countall accepts NaN → validSamplesInBucket=true → finalizeBucket enters the
    'else' branch. TWA's validPerAgg is false (it rejected NaN), so it must use
    twa_compute_empty_bucket_value for interpolation, not finalizeEmpty.

    """
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_twa_nan1{a}')
        # Bucket [1000, 2000): normal values (used for TWA interpolation)
        r.execute_command('TS.ADD', 'magg_twa_nan1{a}', 1000, 100)
        r.execute_command('TS.ADD', 'magg_twa_nan1{a}', 1500, 150)
        # Bucket [2000, 3000): NaN-only → triggers the bug
        r.execute_command('TS.ADD', 'magg_twa_nan1{a}', 2000, 'NaN')
        r.execute_command('TS.ADD', 'magg_twa_nan1{a}', 2500, 'NaN')
        # Bucket [3000, 4000): normal values (used for TWA interpolation)
        r.execute_command('TS.ADD', 'magg_twa_nan1{a}', 3000, 300)
        r.execute_command('TS.ADD', 'magg_twa_nan1{a}', 3500, 350)

        agg_types = ['countall', 'twa']
        multi_agg_str = ','.join(agg_types)

        refs = [r.execute_command('TS.RANGE', 'magg_twa_nan1{a}', '-', '+',
                                  'AGGREGATION', agg, 1000, 'EMPTY') for agg in agg_types]
        result = r.execute_command('TS.RANGE', 'magg_twa_nan1{a}', '-', '+',
                                   'AGGREGATION', multi_agg_str, 1000, 'EMPTY')

        assert len(result) == len(refs[0]), \
            f"bucket count mismatch: multi-agg={len(result)}, single-agg={len(refs[0])}"
        for i in range(len(result)):
            assert result[i][0] == refs[0][i][0], f"timestamp mismatch at bucket {i}"
            for a, agg in enumerate(agg_types):
                assert result[i][1 + a] == refs[a][i][1], \
                    f"{agg} mismatch at bucket {i}: multi-agg={result[i][1 + a]}, single={refs[a][i][1]}"

        # Also verify REVRANGE
        refs_rev = [r.execute_command('TS.REVRANGE', 'magg_twa_nan1{a}', '-', '+',
                                      'AGGREGATION', agg, 1000, 'EMPTY') for agg in agg_types]
        result_rev = r.execute_command('TS.REVRANGE', 'magg_twa_nan1{a}', '-', '+',
                                       'AGGREGATION', multi_agg_str, 1000, 'EMPTY')

        assert len(result_rev) == len(refs_rev[0])
        for i in range(len(result_rev)):
            assert result_rev[i][0] == refs_rev[0][i][0]
            for a, agg in enumerate(agg_types):
                assert result_rev[i][1 + a] == refs_rev[a][i][1], \
                    f"REVRANGE {agg} mismatch at bucket {i}: multi={result_rev[i][1 + a]}, single={refs_rev[a][i][1]}"


def test_multi_agg_twa_nan_only_last_bucket():
    """TWA must get interpolated value in a NaN-only LAST bucket when mixed with countall.

    This specifically exercises the _finalize code path (not finalizeBucket), which is
    reached when the last bucket in the iteration has unfinalized context.
    """
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_twa_nan2{a}')
        # Bucket [1000, 2000): normal values
        r.execute_command('TS.ADD', 'magg_twa_nan2{a}', 1000, 100)
        r.execute_command('TS.ADD', 'magg_twa_nan2{a}', 1500, 150)
        # Bucket [2000, 3000): NaN-only, and this is the LAST bucket
        r.execute_command('TS.ADD', 'magg_twa_nan2{a}', 2000, 'NaN')
        r.execute_command('TS.ADD', 'magg_twa_nan2{a}', 2500, 'NaN')

        agg_types = ['countall', 'twa']
        multi_agg_str = ','.join(agg_types)

        refs = [r.execute_command('TS.RANGE', 'magg_twa_nan2{a}', '-', '+',
                                  'AGGREGATION', agg, 1000, 'EMPTY') for agg in agg_types]
        result = r.execute_command('TS.RANGE', 'magg_twa_nan2{a}', '-', '+',
                                   'AGGREGATION', multi_agg_str, 1000, 'EMPTY')

        assert len(result) == len(refs[0]), \
            f"bucket count mismatch: multi-agg={len(result)}, single-agg={len(refs[0])}"
        for i in range(len(result)):
            assert result[i][0] == refs[0][i][0], f"timestamp mismatch at bucket {i}"
            for a, agg in enumerate(agg_types):
                assert result[i][1 + a] == refs[a][i][1], \
                    f"{agg} mismatch at bucket {i}: multi-agg={result[i][1 + a]}, single={refs[a][i][1]}"


def test_multi_agg_twa_nan_only_with_countnan():
    """TWA with countnan (instead of countall) and NaN-only bucket.

    countnan uses nanValueValid (only NaN passes), so in a NaN-only bucket:
    countnan accepts NaN → validSamplesInBucket=true, but TWA rejects NaN.
    TWA must still get the interpolated value, not finalizeEmpty.
    """
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_twa_nan3{a}')
        r.execute_command('TS.ADD', 'magg_twa_nan3{a}', 1000, 100)
        r.execute_command('TS.ADD', 'magg_twa_nan3{a}', 1500, 150)
        r.execute_command('TS.ADD', 'magg_twa_nan3{a}', 2000, 'NaN')
        r.execute_command('TS.ADD', 'magg_twa_nan3{a}', 2500, 'NaN')
        r.execute_command('TS.ADD', 'magg_twa_nan3{a}', 3000, 300)
        r.execute_command('TS.ADD', 'magg_twa_nan3{a}', 3500, 350)

        agg_types = ['countnan', 'twa']
        multi_agg_str = ','.join(agg_types)

        refs = [r.execute_command('TS.RANGE', 'magg_twa_nan3{a}', '-', '+',
                                  'AGGREGATION', agg, 1000, 'EMPTY') for agg in agg_types]
        result = r.execute_command('TS.RANGE', 'magg_twa_nan3{a}', '-', '+',
                                   'AGGREGATION', multi_agg_str, 1000, 'EMPTY')

        assert len(result) == len(refs[0]), \
            f"bucket count mismatch: multi-agg={len(result)}, single-agg={len(refs[0])}"
        for i in range(len(result)):
            assert result[i][0] == refs[0][i][0], f"timestamp mismatch at bucket {i}"
            for a, agg in enumerate(agg_types):
                assert result[i][1 + a] == refs[a][i][1], \
                    f"{agg} mismatch at bucket {i}: multi-agg={result[i][1 + a]}, single={refs[a][i][1]}"


def test_multi_agg_multiple_gaps_memmove():
    """Test multi-agg with multiple gaps that trigger the fillEmptyBuckets overlap path.

    When multiple gaps exist, fillEmptyBuckets is called multiple times within a single
    GetNextChunk invocation. This tests that aux_chunk num_samples is correctly tracked
    and memmove operations don't corrupt data.
    """
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_mgap{a}')
        # Create 5 populated buckets with gaps between each
        for bucket_start in [1000, 3000, 5000, 7000, 9000]:
            for i in range(5):
                r.execute_command('TS.ADD', 'magg_mgap{a}', bucket_start + i,
                                 bucket_start + i)

        agg_types = ['min', 'max', 'count', 'sum']
        multi_agg_str = ','.join(agg_types)

        refs = [r.execute_command('TS.RANGE', 'magg_mgap{a}', '-', '+',
                                  'AGGREGATION', agg, 1000, 'EMPTY') for agg in agg_types]
        result = r.execute_command('TS.RANGE', 'magg_mgap{a}', '-', '+',
                                   'AGGREGATION', multi_agg_str, 1000, 'EMPTY')

        assert len(result) == len(refs[0]), \
            f"bucket count mismatch: multi-agg={len(result)}, single-agg={len(refs[0])}"
        for i in range(len(result)):
            assert result[i][0] == refs[0][i][0], f"timestamp mismatch at bucket {i}"
            for a, agg in enumerate(agg_types):
                assert result[i][1 + a] == refs[a][i][1], \
                    f"{agg} mismatch at bucket {i}: multi-agg={result[i][1 + a]}, single={refs[a][i][1]}"

        # Also verify REVRANGE with multiple gaps
        refs_rev = [r.execute_command('TS.REVRANGE', 'magg_mgap{a}', '-', '+',
                                      'AGGREGATION', agg, 1000, 'EMPTY') for agg in agg_types]
        result_rev = r.execute_command('TS.REVRANGE', 'magg_mgap{a}', '-', '+',
                                       'AGGREGATION', multi_agg_str, 1000, 'EMPTY')

        assert len(result_rev) == len(refs_rev[0]), \
            f"REVRANGE bucket count mismatch: multi-agg={len(result_rev)}, single-agg={len(refs_rev[0])}"
        for i in range(len(result_rev)):
            assert result_rev[i][0] == refs_rev[0][i][0], f"REVRANGE timestamp mismatch at bucket {i}"
            for a, agg in enumerate(agg_types):
                assert result_rev[i][1 + a] == refs_rev[a][i][1], \
                    f"REVRANGE {agg} mismatch at bucket {i}: multi={result_rev[i][1 + a]}, single={refs_rev[a][i][1]}"


def test_multi_agg_twa_with_gaps_and_countall():
    """Combined test: TWA + countall + EMPTY + multiple gaps.

    This exercises both findings simultaneously: TWA interpolation in the
    finalizeBucket else-branch AND the fillEmptyBuckets overlap path with
    aux_chunk, including TWA-specific empty bucket filling.
    """
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_combo{a}')
        # Bucket [0, 10): values
        r.execute_command('TS.ADD', 'magg_combo{a}', 2, 20)
        r.execute_command('TS.ADD', 'magg_combo{a}', 5, 50)
        # Bucket [10, 20): NaN-only → TWA needs interpolation, countall accepts
        r.execute_command('TS.ADD', 'magg_combo{a}', 12, 'NaN')
        # Bucket [20, 30): empty gap (no samples at all)
        # Bucket [30, 40): values
        r.execute_command('TS.ADD', 'magg_combo{a}', 32, 320)
        r.execute_command('TS.ADD', 'magg_combo{a}', 38, 380)

        agg_types = ['countall', 'min', 'twa']
        multi_agg_str = ','.join(agg_types)

        refs = [r.execute_command('TS.RANGE', 'magg_combo{a}', 0, 40,
                                  'AGGREGATION', agg, 10, 'EMPTY') for agg in agg_types]
        result = r.execute_command('TS.RANGE', 'magg_combo{a}', 0, 40,
                                   'AGGREGATION', multi_agg_str, 10, 'EMPTY')

        assert len(result) == len(refs[0]), \
            f"bucket count mismatch: multi-agg={len(result)}, single-agg={len(refs[0])}"
        for i in range(len(result)):
            assert result[i][0] == refs[0][i][0], f"timestamp mismatch at bucket {i}"
            for a, agg in enumerate(agg_types):
                assert result[i][1 + a] == refs[a][i][1], \
                    f"{agg} mismatch at bucket {i}: multi-agg={result[i][1 + a]}, single={refs[a][i][1]}"


def test_createrule_multi_agg_error():
    """TS.CREATERULE should reject multiple aggregators."""
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_cr1{a}')
        r.execute_command('TS.CREATE', 'magg_cr2{a}')

        with pytest.raises(redis.ResponseError, match="exactly one"):
            r.execute_command('TS.CREATERULE', 'magg_cr1{a}', 'magg_cr2{a}',
                              'AGGREGATION', 'min,max', 1000)
