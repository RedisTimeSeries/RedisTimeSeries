#include "parse_policies.h"
#include "minunit.h"
#include "compaction.h"
#include "rmutil/alloc.h"

MU_TEST(test_valid_policy) {
    SimpleCompactionRule* parsedRules;
    size_t rulesCount;
    int result = ParseCompactionPolicy("max:1m:1h;min:10s:5d:10d;avg:2h:10d;avg:3d:100d", &parsedRules, &rulesCount);
	mu_check(result == TRUE);
	mu_check(rulesCount == 4);

    mu_check(parsedRules[0].aggType == StringAggTypeToEnum("max"));
    mu_check(parsedRules[0].bucketSizeSec == 60);

    mu_check(parsedRules[1].aggType == StringAggTypeToEnum("min"));
    mu_check(parsedRules[1].bucketSizeSec == 10);

    mu_check(parsedRules[2].aggType == StringAggTypeToEnum("avg"));
    mu_check(parsedRules[2].bucketSizeSec == 3600*2);

    mu_check(parsedRules[3].aggType == StringAggTypeToEnum("avg"));
    mu_check(parsedRules[3].bucketSizeSec == 3*86400);
    free(parsedRules);
}

MU_TEST(test_invalid_policy) {
    SimpleCompactionRule* parsedRules;
    size_t rulesCount;
    int result;
    result = ParseCompactionPolicy("max:1m;mins:10s;avg:2h;avg:1d", &parsedRules, &rulesCount);
	mu_check(result == FALSE);
	mu_check(rulesCount == 0);

    result = ParseCompactionPolicy("max:12hd;", &parsedRules, &rulesCount);
	mu_check(result == FALSE);
	mu_check(rulesCount == 0);
    free(parsedRules);
}

MU_TEST(test_StringLenAggTypeToEnum) {
    mu_check(StringAggTypeToEnum("min") == TS_AGG_MIN);
    mu_check(StringAggTypeToEnum("max") == TS_AGG_MAX);
    mu_check(StringAggTypeToEnum("sum") == TS_AGG_SUM);
    mu_check(StringAggTypeToEnum("avg") == TS_AGG_AVG);
    mu_check(StringAggTypeToEnum("count") == TS_AGG_COUNT);
    mu_check(StringAggTypeToEnum("first") == TS_AGG_FIRST);
    mu_check(StringAggTypeToEnum("last") == TS_AGG_LAST);
}

MU_TEST_SUITE(test_suite) {
	MU_RUN_TEST(test_valid_policy);
	MU_RUN_TEST(test_invalid_policy);
	MU_RUN_TEST(test_StringLenAggTypeToEnum);
}

int main(int argc, char *argv[]) {
    RMUTil_InitAlloc();
    MU_RUN_SUITE(test_suite);
	MU_REPORT();
	return 0;
}