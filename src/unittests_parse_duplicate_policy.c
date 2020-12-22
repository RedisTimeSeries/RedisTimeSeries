/*
 * Copyright 2018-2020 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "consts.h"
#include "generic_chunk.h"
#include "minunit.h"
#include "parse_policies.h"

#include <string.h>

#define DuplicatePolicyFromCString(str) DuplicatePolicyFromString(str, strlen(str))

MU_TEST(test_duplicate_policy_parse) {
    mu_check(DuplicatePolicyFromCString("Min") == DP_MIN);
    mu_check(DuplicatePolicyFromCString("MAX") == DP_MAX);
    mu_check(DuplicatePolicyFromCString("sum") == DP_SUM);
    mu_check(DuplicatePolicyFromCString("Last") == DP_LAST);
    mu_check(DuplicatePolicyFromCString("Block") == DP_BLOCK);
    mu_check(DuplicatePolicyFromCString("first") == DP_FIRST);
    mu_check(DuplicatePolicyFromCString("DSADASD") == DP_INVALID);
}

MU_TEST(test_duplicate_policy_to_string) {
    mu_check(strcmp(DuplicatePolicyToString(DP_MIN), "min") == 0);
}

MU_TEST_SUITE(parse_duplicate_policy_test_suite) {
    MU_RUN_TEST(test_duplicate_policy_parse);
    MU_RUN_TEST(test_duplicate_policy_to_string);
}
