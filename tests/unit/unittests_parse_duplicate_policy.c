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
#include "tsdb.h"

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

// Every policy in the enum must parse back from its canonical name. Adding a policy
// without a matching branch in DuplicatePolicyFromString fails here, which is the
// mistake that would otherwise go unnoticed: the new keyword just never matches.
MU_TEST(test_duplicate_policy_round_trip) {
    for (DuplicatePolicy p = DP_BLOCK; p < DP_TYPES_MAX; p++) {
        const char *name = DuplicatePolicyToString(p);
        mu_assert(DuplicatePolicyFromCString(name) == p,
                  "a DUPLICATE_POLICY doesn't parse back from its name - add it to "
                  "DuplicatePolicyFromString");
    }
}

// Same round trip for aggregation types. Both display forms are checked: the
// lower-case one is the exact keyword the parser matches, and the upper-case one
// additionally pins the case-insensitive comparison.
MU_TEST(test_agg_type_round_trip) {
    for (TS_AGG_TYPES_T t = TS_AGG_MIN; t < TS_AGG_TYPES_MAX; t++) {
        const char *lower = AggTypeEnumToStringLowerCase(t);
        mu_assert(StringLenAggTypeToEnum(lower, strlen(lower)) == t,
                  "an aggregation type doesn't parse back from AggTypeEnumToStringLowerCase - "
                  "add it to StringLenAggTypeToEnum");
        const char *upper = AggTypeEnumToString(t);
        mu_assert(StringLenAggTypeToEnum(upper, strlen(upper)) == t,
                  "an aggregation type doesn't parse back from AggTypeEnumToString - the two "
                  "display forms must agree");
    }
}

// Every keyword must resolve regardless of case, for both parsers. Guards against a
// regression from strncasecmp back to a case-sensitive comparison: an alternating-case
// spelling matches nothing if the comparison stops being case-insensitive.
MU_TEST(test_mixed_case_keywords) {
    char mixed[32];
    for (TS_AGG_TYPES_T t = TS_AGG_MIN; t < TS_AGG_TYPES_MAX; t++) {
        const char *name = AggTypeEnumToStringLowerCase(t);
        const size_t len = strlen(name);
        mu_check(len < sizeof(mixed));
        to_alternating_case(name, len, mixed);
        mu_assert(StringLenAggTypeToEnum(mixed, len) == t,
                  "an aggregation type doesn't resolve in mixed case - is the comparison "
                  "still case-insensitive?");
    }
    for (DuplicatePolicy p = DP_BLOCK; p < DP_TYPES_MAX; p++) {
        const char *name = DuplicatePolicyToString(p);
        const size_t len = strlen(name);
        mu_check(len < sizeof(mixed));
        to_alternating_case(name, len, mixed);
        mu_assert(DuplicatePolicyFromString(mixed, len) == p,
                  "a DUPLICATE_POLICY doesn't resolve in mixed case - is the comparison "
                  "still case-insensitive?");
    }
}

// Oversized input must be rejected, not crash: `len` is caller-controlled, so it must
// never reach an allocation sized by it. The degenerate lengths matter too: a zero
// length must not match anything (strncasecmp with n == 0 reports equality, so this
// pins the length dispatch that keeps it from being reached), and a length just past
// the longest keyword must fall through rather than partially match.
MU_TEST(test_oversized_input_rejected) {
    char huge[64 * 1024];
    memset(huge, 'A', sizeof(huge));
    mu_check(DuplicatePolicyFromString(huge, sizeof(huge)) == DP_INVALID);
    mu_check(StringLenAggTypeToEnum(huge, sizeof(huge)) == TS_AGG_INVALID);

    mu_check(DuplicatePolicyFromString("", 0) == DP_INVALID);
    mu_check(StringLenAggTypeToEnum("", 0) == TS_AGG_INVALID);
    mu_check(DuplicatePolicyFromString("blocks", 6) == DP_INVALID);   // one past "block"
    mu_check(StringLenAggTypeToEnum("countnans", 9) == TS_AGG_INVALID); // one past "countnan"
}

// An out-of-range aggType reaches NewRule from a crafted RDB / RESTORE payload.
// GetAggClass returns NULL for it, so NewRule must fail instead of dereferencing it.
MU_TEST(test_new_rule_rejects_unknown_agg_type) {
    // destKey is only consumed on success, so NULL is safe for the rejection paths
    mu_check(NewRule(NULL, TS_AGG_TYPES_MAX, 1000, 0) == NULL);
    mu_check(NewRule(NULL, 9999, 1000, 0) == NULL);
    mu_check(NewRule(NULL, TS_AGG_NONE, 1000, 0) == NULL);
    // a valid type still builds a rule, so the check above isn't rejecting everything
    CompactionRule *rule = NewRule(NULL, TS_AGG_AVG, 1000, 0);
    mu_check(rule != NULL);
    free(rule->aggContext);
    free(rule);
}

MU_TEST_SUITE(parse_duplicate_policy_test_suite) {
    MU_RUN_TEST(test_duplicate_policy_parse);
    MU_RUN_TEST(test_duplicate_policy_to_string);
    MU_RUN_TEST(test_duplicate_policy_round_trip);
    MU_RUN_TEST(test_agg_type_round_trip);
    MU_RUN_TEST(test_mixed_case_keywords);
    MU_RUN_TEST(test_oversized_input_rejected);
    MU_RUN_TEST(test_new_rule_rejects_unknown_agg_type);
}
