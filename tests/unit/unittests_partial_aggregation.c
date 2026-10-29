/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "compaction.h"
#include <float.h>
#include <math.h>

MU_TEST(test_weighted_partial_average) {
    AggregationClass *avg = GetAggClass(TS_AGG_AVG);
    void *ctx = avg->createContext(false);
    AvgMergePartial(ctx, 10, 1);
    AvgMergePartial(ctx, 20, 9);
    AvgMergePartial(ctx, NAN, 0);
    double value;
    avg->finalize(ctx, &value);
    mu_assert_double_eq(19, value);
    // A legacy shard's uncombined value can follow an upgraded shard's partial result.
    avg->appendValue(ctx, 30, 0);
    avg->finalize(ctx, &value);
    mu_assert_double_eq(20, value);
    avg->freeContext(ctx);
}

MU_TEST(test_partial_average_overflow_and_nan) {
    AggregationClass *avg = GetAggClass(TS_AGG_AVG);
    void *ctx = avg->createContext(false);
    AvgMergePartial(ctx, DBL_MAX, 1000);
    AvgMergePartial(ctx, DBL_MAX, 1000);
    double value;
    avg->finalize(ctx, &value);
    mu_assert(isfinite(value) && value == DBL_MAX,
              "Partial AVG must not overflow its weighted sum");
    AvgMergePartial(ctx, -DBL_MAX, 2000);
    avg->finalize(ctx, &value);
    mu_assert_double_eq(0, value);
    AvgMergePartial(ctx, NAN, 1);
    avg->finalize(ctx, &value);
    mu_assert(isnan(value), "A NaN partial with contributing inputs must propagate");
    avg->freeContext(ctx);
}

MU_TEST(test_partial_count) {
    AggregationClass *count = GetAggClass(TS_AGG_COUNT);
    void *ctx = count->createContext(false);
    CountMergePartial(ctx, 0);
    CountMergePartial(ctx, 9);
    count->appendValue(ctx, 42, 0);
    double value;
    count->finalize(ctx, &value);
    mu_assert_double_eq(10, value);
    count->freeContext(ctx);
}

MU_TEST_SUITE(partial_aggregation_test_suite) {
    MU_RUN_TEST(test_weighted_partial_average);
    MU_RUN_TEST(test_partial_average_overflow_and_nan);
    MU_RUN_TEST(test_partial_count);
}
