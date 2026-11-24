/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#define REDISMODULE_MAIN
#define REDISMODULE_SDK_RLEC 1

#include "minunit.h"

#include "parse_policies.h"
#include "tsdb.h"
#include "unittests_compressed_chunk.c"
#include "unittests_platform_helpers.c"
#include "unittests_parse_duplicate_policy.c"
#include "unittests_parse_policies.c"
#include "unittests_uncompressed_chunk.c"
#include "unittests_cmd_info.c"
#include "unittests_rdb_load_oom.c"
#include "unittests_heap.c"

#include <stdio.h>
#include <stdlib.h>
#include "rmutil/alloc.h"

MU_TEST(test_signed_bucket_start) {
    mu_check(CalcBucketStart(2, 10, 5) < 0);
    mu_check(CalcBucketStart(2, 10, 5) == -5);
    mu_check(CalcBucketStart(3, 10, 5) == -5);
    mu_check(CalcBucketStart(5, 10, 5) == 5);
    mu_check(CalcBucketStart(15, 10, 5) == 15);
    mu_check(CalcBucketStart(0, 10, 0) == 0);
    mu_check(CalcBucketStart(0, 10, INT64_MAX) == -3);
    mu_check(CalcBucketStart(INT64_MAX, 10, 5) == INT64_MAX - 2);
    mu_check(BucketStartNormalize(CalcBucketStart(2, 10, 5)) == 0);
}

int main(int argc, char *argv[]) {
    RMUTil_InitAlloc();
    MU_DISABLE_PROGRESS_PRINT();
    MU_RUN_TEST(test_signed_bucket_start);
    MU_RUN_SUITE(parse_policies_test_suite);
    MU_RUN_SUITE(uncompressed_chunk_test_suite);
    MU_RUN_SUITE(compressed_chunk_test_suite);
    MU_RUN_SUITE(platform_helpers_test_suite);
    MU_RUN_SUITE(parse_duplicate_policy_test_suite);
    MU_RUN_SUITE(command_info_test_suite);
    MU_RUN_SUITE(rdb_load_oom_test_suite);
    MU_RUN_SUITE(heap_test_suite);
    MU_REPORT();
    return minunit_fail;
}
