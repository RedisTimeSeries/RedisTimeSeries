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
#include "unittests_compressed_chunk.c"
#include "unittests_parse_duplicate_policy.c"
#include "unittests_parse_policies.c"
#include "unittests_uncompressed_chunk.c"
#include "unittests_cmd_info.c"

#include <stdio.h>
#include <stdlib.h>
#include "rmutil/alloc.h"

int main(int argc, char *argv[]) {
    RMUTil_InitAlloc();
    MU_DISABLE_PROGRESS_PRINT();
    MU_RUN_SUITE(parse_policies_test_suite);
    MU_RUN_SUITE(uncompressed_chunk_test_suite);
    MU_RUN_SUITE(compressed_chunk_test_suite);
    MU_RUN_SUITE(parse_duplicate_policy_test_suite);
    MU_RUN_SUITE(command_info_test_suite);
    MU_REPORT();
    return minunit_fail;
}
