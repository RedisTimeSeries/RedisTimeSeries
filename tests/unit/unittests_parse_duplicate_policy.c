/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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
