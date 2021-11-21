/*
 * Copyright 2018-2020 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "minunit.h"
#include "parse_policies.h"
#include "unittests_compressed_chunk.c"
#include "unittests_parse_duplicate_policy.c"
#include "unittests_parse_policies.c"
#include "unittests_uncompressed_chunk.c"

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
    MU_REPORT();
    return minunit_fail;
}
