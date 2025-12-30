/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "query_language.c"

MU_TEST(test_remove_duplicates) {
    timestamp_t values1[8] = { 1, 1, 3, 4, 4, 4, 5, 6, 6 };
    timestamp_t exp1[8] = { 1, 3, 4, 5, 6 };
    size_t ret = values_remove_duplicates(values1, sizeof(values1)/values1[0]);
    mu_assert_int_eq(ret, 5);
    mu_assert_array_eq(values1, exp1, ret);

    timestamp_t values2[2] = { 1, 1 };
    timestamp_t exp2[8] = { 1 };
    size_t ret = values_remove_duplicates(values1, sizeof(values2)/values2[0]);
    mu_assert_int_eq(ret, 1);
    mu_assert_array_eq(values2, exp2, ret);

    timestamp_t values3[1] = { 1 };
    size_t ret = values_remove_duplicates(values1, sizeof(values3)/values3[0]);
    mu_assert_int_eq(ret, 1);
    mu_assert_array_eq(values3, exp2, ret);
}
