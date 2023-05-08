/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */
#include "compaction.h"
#include "compressed_chunk.h"
#include "gorilla.h"
#include "minunit.h"
#include "parse_policies.h"
#include "tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include "rmutil/alloc.h"

MU_TEST(test_valid_policy) {
    SimpleCompactionRule *parsedRules;
    uint64_t rulesCount;
    int result = ParseCompactionPolicy(
        "max:1m:1h:1m;min:10s:10d:0m;last:5M:10m;avg:2h:10d:1d;avg:3d:100d", &parsedRules, &rulesCount);
    mu_check(result == TRUE);
    mu_check(rulesCount == 5);

    mu_check(parsedRules[0].aggType == StringAggTypeToEnum("max"));
    mu_assert_int_eq(parsedRules[0].bucketDuration, 1);
    mu_check(parsedRules[0].timestampAlignment == 1);

    mu_check(parsedRules[1].aggType == StringAggTypeToEnum("min"));
    mu_check(parsedRules[1].bucketDuration == 10 * 1000);
    mu_check(parsedRules[1].timestampAlignment == 0);

    mu_check(parsedRules[2].aggType == StringAggTypeToEnum("last"));
    mu_check(parsedRules[2].bucketDuration == 5 * 60 * 1000);
    mu_check(parsedRules[2].timestampAlignment == 0);

    mu_check(parsedRules[3].aggType == StringAggTypeToEnum("avg"));
    mu_check(parsedRules[3].bucketDuration == 2 * 60 * 60 * 1000);
    mu_check(parsedRules[3].timestampAlignment == 24 * 3600 * 1000);

    mu_check(parsedRules[4].aggType == StringAggTypeToEnum("avg"));
    mu_check(parsedRules[4].bucketDuration == 3 * 60 * 60 * 1000 * 24);
    mu_check(parsedRules[4].timestampAlignment == 0);
    free(parsedRules);
}

MU_TEST(test_invalid_policy) {
    SimpleCompactionRule *parsedRules;
    uint64_t rulesCount;
    int result;
    result = ParseCompactionPolicy("max:1M;mins:10s;avg:2h;avg:1d", &parsedRules, &rulesCount);
    mu_check(result == FALSE);
    mu_check(rulesCount == 0);

    result = ParseCompactionPolicy("max:1M:1h:abc;", &parsedRules, &rulesCount);
    mu_check(result == FALSE);
    mu_check(rulesCount == 0);

    result = ParseCompactionPolicy("max:12hd;", &parsedRules, &rulesCount);
    mu_check(result == FALSE);
    mu_check(rulesCount == 0);

    result = ParseCompactionPolicy("------", &parsedRules, &rulesCount);
    mu_check(result == FALSE);
    mu_check(rulesCount == 0);
    result = ParseCompactionPolicy("max", &parsedRules, &rulesCount);
    mu_check(result == FALSE);
    mu_check(rulesCount == 0);
    result = ParseCompactionPolicy("max:", &parsedRules, &rulesCount);
    mu_check(result == FALSE);
    mu_check(rulesCount == 0);
    result = ParseCompactionPolicy("max:abcdfeffas", &parsedRules, &rulesCount);
    mu_check(result == FALSE);
    mu_check(rulesCount == 0);
    if(parsedRules) {
        free(parsedRules);
    }
}

MU_TEST(test_StringLenAggTypeToEnum) {
    mu_check(StringAggTypeToEnum("min") == TS_AGG_MIN);
    mu_check(StringAggTypeToEnum("max") == TS_AGG_MAX);
    mu_check(StringAggTypeToEnum("sum") == TS_AGG_SUM);
    mu_check(StringAggTypeToEnum("avg") == TS_AGG_AVG);
    mu_check(StringAggTypeToEnum("twa") == TS_AGG_TWA);
    mu_check(StringAggTypeToEnum("count") == TS_AGG_COUNT);
    mu_check(StringAggTypeToEnum("first") == TS_AGG_FIRST);
    mu_check(StringAggTypeToEnum("last") == TS_AGG_LAST);
    mu_check(StringAggTypeToEnum("range") == TS_AGG_RANGE);
}

MU_TEST_SUITE(parse_policies_test_suite) {
    MU_RUN_TEST(test_valid_policy);
    MU_RUN_TEST(test_invalid_policy);
    MU_RUN_TEST(test_StringLenAggTypeToEnum);
}
