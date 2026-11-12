/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 */
#include "tsdb.h"
#include "consts.h"
#include "minunit.h"

MU_TEST(test_new_rule_rejects_invalid_aggregation) {
    MU_CHECK(NewRule(NULL, TS_AGG_INVALID, 1, 0) == NULL);
}

MU_TEST_SUITE(tsdb_test_suite) {
    MU_RUN_TEST(test_new_rule_rejects_invalid_aggregation);
}
