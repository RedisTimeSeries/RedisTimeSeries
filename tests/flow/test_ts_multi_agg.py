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
    with Env().getClusterConnectionIfNeeded() as r:
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
    with Env().getClusterConnectionIfNeeded() as r:
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
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_grp{a}', 'LABELS', 'type', 'maggrp')
        r.execute_command('TS.ADD', 'magg_grp{a}', 1000, 100)

        with pytest.raises(redis.ResponseError, match="GROUPBY"):
            r.execute_command('TS.MRANGE', '-', '+',
                              'AGGREGATION', 'min,max', 1000,
                              'FILTER', 'type=maggrp',
                              'GROUPBY', 'type', 'REDUCE', 'max')


def test_multi_agg_groupby_single_ok():
    """Single aggregator with GROUPBY should still work."""
    with Env().getClusterConnectionIfNeeded() as r:
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


def test_createrule_multi_agg_error():
    """TS.CREATERULE should reject multiple aggregators."""
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', 'magg_cr1{a}')
        r.execute_command('TS.CREATE', 'magg_cr2{a}')

        with pytest.raises(redis.ResponseError, match="exactly one"):
            r.execute_command('TS.CREATERULE', 'magg_cr1{a}', 'magg_cr2{a}',
                              'AGGREGATION', 'min,max', 1000)
