/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "minunit.h"
#include "libmr_integration.h"
#include "LibMR/src/utils/arr.h"

// Test the shared preflight validator with injected shard results. Keeping the
// validator in its own header avoids compiling all command implementations into
// the unit binary and counting a second copy of those functions in coverage.
typedef struct
{
    Record **records;
    size_t len;
} TestGearsResults;

static MRRecordType test_list_type, test_map_type, test_series_type, test_string_type;
static MRRecordType *test_list_record_type(void) {
    return &test_list_type;
}
static MRRecordType *test_map_record_type(void) {
    return &test_map_type;
}
static MRRecordType *test_series_record_type(void) {
    return &test_series_type;
}
static MRRecordType *test_string_record_type(void) {
    return &test_string_type;
}
static size_t test_gears_result_count(ExecutionCtx *ctx) {
    return ((TestGearsResults *)ctx)->len;
}
static Record *test_gears_result(ExecutionCtx *ctx, size_t i) {
    return ((TestGearsResults *)ctx)->records[i];
}

#define GetListRecordType test_list_record_type
#define GetMapRecordType test_map_record_type
#define GetSeriesRecordType test_series_record_type
#define GetStringRecordType test_string_record_type
#define MR_ExecutionCtxGetResultsLen test_gears_result_count
#define MR_ExecutionCtxGetResult test_gears_result
#include "libmr_result_validation.h"
#undef GetListRecordType
#undef GetMapRecordType
#undef GetSeriesRecordType
#undef GetStringRecordType
#undef MR_ExecutionCtxGetResultsLen
#undef MR_ExecutionCtxGetResult

static int gears_error_replies;
static const char *gears_last_error;
static int gears_reply_error(RedisModuleCtx *ctx, const char *error) {
    gears_error_replies++;
    gears_last_error = error;
    return REDISMODULE_OK;
}
static void gears_log(RedisModuleCtx *ctx, const char *level, const char *fmt, ...) {}

static void assert_gears_results_rejected(Record **records, size_t len) {
    TestGearsResults results = { records, len };
    ExecutionCtx *ctx = (ExecutionCtx *)&results;
    // MRANGE, MGET/RESP2, QUERYINDEX/QUERYLABELS, and MGET/RESP3.
    MRRecordType *containers[] = {
        &test_list_type, &test_list_type, &test_list_type, &test_map_type
    };
    MRRecordType *elements[] = {
        &test_series_type, &test_list_type, &test_string_type, &test_list_type
    };
    for (size_t i = 0; i < 4; i++) {
        gears_error_replies = 0;
        mu_check(!validate_gears_results(ctx, NULL, containers[i], elements[i]));
        mu_assert_int_eq(1, gears_error_replies);
        mu_check(strstr(gears_last_error, "Unexpected shard result type") != NULL);
    }
}

MU_TEST(test_gears_bad_records_fail_before_reply) {
    Record wrong = { .recordType = &test_series_type };
    Record *records[] = { &wrong };
    assert_gears_results_rejected(records, 1);
    records[0] = NULL;
    assert_gears_results_rejected(records, 1);

    // A valid empty first shard must not cause a response header to be written
    // before the invalid second shard has been inspected.
    ListRecord empty = { .base.recordType = &test_list_type, .records = array_new(Record *, 0) };
    Record *later_bad[] = { (Record *)&empty, &wrong };
    assert_gears_results_rejected(later_bad, 2);
    array_free(empty.records);
}

MU_TEST(test_gears_bad_children_fail_before_reply) {
    Record wrong = { .recordType = &test_map_type };
    ListRecord list = { .base.recordType = &test_list_type, .records = array_new(Record *, 2) };
    list.records = array_append(list.records, &wrong);
    Record *records[] = { (Record *)&list };
    assert_gears_results_rejected(records, 1);
    list.records[0] = NULL;
    assert_gears_results_rejected(records, 1);
    array_free(list.records);
}

MU_TEST(test_gears_valid_shapes_and_late_bad_children) {
    Record series = { .recordType = &test_series_type };
    Record string = { .recordType = &test_string_type };
    Record nested_list = { .recordType = &test_list_type };
    ListRecord list = { .base.recordType = &test_list_type, .records = array_new(Record *, 2) };
    Record *records[] = { (Record *)&list };
    TestGearsResults results = { records, 1 };
    ExecutionCtx *ctx = (ExecutionCtx *)&results;
    gears_error_replies = 0;
    mu_check(validate_gears_results(ctx, NULL, &test_list_type, &test_series_type));
    list.records = array_append(list.records, &series);
    mu_check(validate_gears_results(ctx, NULL, &test_list_type, &test_series_type));
    list.records[0] = &string;
    mu_check(validate_gears_results(ctx, NULL, &test_list_type, &test_string_type));
    list.records[0] = &nested_list;
    mu_check(validate_gears_results(ctx, NULL, &test_list_type, &test_list_type));
    mu_assert_int_eq(0, gears_error_replies);

    // Valid first child followed by a bad child must reject the entire shard.
    list.records = array_append(list.records, NULL);
    mu_check(!validate_gears_results(ctx, NULL, &test_list_type, &test_list_type));
    list.records[0] = &series;
    mu_check(!validate_gears_results(ctx, NULL, &test_list_type, &test_series_type));
    list.records[0] = &string;
    mu_check(!validate_gears_results(ctx, NULL, &test_list_type, &test_string_type));
    array_free(list.records);

    MapRecord map = { .base.recordType = &test_map_type, .records = array_new(Record *, 2) };
    records[0] = (Record *)&map;
    mu_check(validate_gears_results(ctx, NULL, &test_map_type, &test_list_type));
    map.records = array_append(map.records, &string);
    gears_error_replies = 0;
    // Odd key/value count must fail before any map can be emitted.
    mu_check(!validate_gears_results(ctx, NULL, &test_map_type, &test_list_type));
    mu_assert_int_eq(1, gears_error_replies);
    map.records = array_append(map.records, &nested_list);
    mu_check(validate_gears_results(ctx, NULL, &test_map_type, &test_list_type));
    map.records[0] = &series;
    mu_check(!validate_gears_results(ctx, NULL, &test_map_type, &test_list_type));
    map.records[0] = &string;
    map.records[1] = &series;
    mu_check(!validate_gears_results(ctx, NULL, &test_map_type, &test_list_type));
    array_free(map.records);
}

MU_TEST_SUITE(libmr_results_test_suite) {
    __typeof__(RedisModule_ReplyWithError) saved_error = RedisModule_ReplyWithError;
    __typeof__(RedisModule_Log) saved_log = RedisModule_Log;
    RedisModule_ReplyWithError = gears_reply_error;
    RedisModule_Log = gears_log;
    MU_RUN_TEST(test_gears_bad_records_fail_before_reply);
    MU_RUN_TEST(test_gears_bad_children_fail_before_reply);
    MU_RUN_TEST(test_gears_valid_shapes_and_late_bad_children);
    RedisModule_ReplyWithError = saved_error;
    RedisModule_Log = saved_log;
}
