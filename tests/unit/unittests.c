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

#include <setjmp.h>

static jmp_buf unittests_assert_jmp;
static int unittests_assert_failed;

static void unittests_assert_handler(const char *estr, const char *file, int line) {
    (void)estr;
    (void)file;
    (void)line;
    unittests_assert_failed = 1;
    longjmp(unittests_assert_jmp, 1);
}

#define mu_assert_redis_assert_fails(expr, message)                                                \
    do {                                                                                           \
        void (*saved_assert)(const char *, const char *, int) = RedisModule__Assert;               \
        RedisModule__Assert = unittests_assert_handler;                                            \
        unittests_assert_failed = 0;                                                               \
        if (setjmp(unittests_assert_jmp) == 0) {                                                   \
            (void)(expr);                                                                          \
        }                                                                                          \
        RedisModule__Assert = saved_assert;                                                        \
        mu_assert(unittests_assert_failed, message);                                               \
    } while (0)

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

int main(int argc, char *argv[]) {
    RMUTil_InitAlloc();
    MU_DISABLE_PROGRESS_PRINT();
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
