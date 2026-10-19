/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include <strings.h>

#include "minunit.h"
#include "libmr_integration.h"
#include "LibMR/src/utils/arr.h"

// Compile the callbacks here so malformed shard results can be injected without
// adding a fault-injection command to the module. The other suites use the same
// source-inclusion pattern to mock Redis APIs.
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
static size_t test_gears_error_count(ExecutionCtx *ctx) {
    return 0;
}

#define GetListRecordType test_list_record_type
#define GetMapRecordType test_map_record_type
#define GetSeriesRecordType test_series_record_type
#define GetStringRecordType test_string_record_type
#define MR_ExecutionCtxGetResultsLen test_gears_result_count
#define MR_ExecutionCtxGetResult test_gears_result
#define MR_ExecutionCtxGetErrorsLen test_gears_error_count
#include "libmr_commands.c"
#undef GetListRecordType
#undef GetMapRecordType
#undef GetSeriesRecordType
#undef GetStringRecordType
#undef MR_ExecutionCtxGetResultsLen
#undef MR_ExecutionCtxGetResult
#undef MR_ExecutionCtxGetErrorsLen

static int gears_error_replies, gears_other_replies, gears_resp_flags;
static const char *gears_last_error;
static int gears_reply_error(RedisModuleCtx *ctx, const char *error) {
    gears_error_replies++;
    gears_last_error = error;
    return REDISMODULE_OK;
}
static int gears_reply_container(RedisModuleCtx *ctx, long len) {
    gears_other_replies++;
    return REDISMODULE_OK;
}
static int gears_context_flags(RedisModuleCtx *ctx) {
    return gears_resp_flags;
}
static void gears_log(RedisModuleCtx *ctx, const char *level, const char *fmt, ...) {}

static void assert_gears_callbacks_reject(Record **records, size_t len, bool resp3) {
    TestGearsResults results = { records, len };
    ExecutionCtx *ctx = (ExecutionCtx *)&results;
    gears_resp_flags = resp3 ? REDISMODULE_CTX_FLAGS_RESP3 : 0;
    for (int command = 0; command < 4; command++) {
        gears_error_replies = gears_other_replies = 0;
        switch (command) {
            case 0: {
                QueryPredicateList predicates = { .ref = 2 };
                MRangeData *data = calloc(1, sizeof(MRangeData));
                data->args.queryPredicates = &predicates;
                mrange_done_gears(ctx, NULL, data);
                mu_assert_int_eq(1, predicates.ref);
                break;
            }
            case 1:
                mget_done_gears(ctx, NULL, NULL);
                break;
            case 2:
                queryindex_done_gears(ctx, NULL, NULL);
                break;
            case 3:
                querylabels_done_gears(ctx, NULL);
                break;
        }
        mu_assert_int_eq(1, gears_error_replies);
        mu_check(strstr(gears_last_error, "Unexpected shard result type") != NULL);
        mu_assert_int_eq(0, gears_other_replies);
    }
}

MU_TEST(test_gears_bad_records_fail_before_reply) {
    Record wrong = { .recordType = &test_map_type };
    Record *records[] = { &wrong };
    assert_gears_callbacks_reject(records, 1, false);
    records[0] = NULL;
    assert_gears_callbacks_reject(records, 1, false);
    assert_gears_callbacks_reject(records, 1, true);

    // A valid empty first shard must not cause a response header to be written
    // before the invalid second shard has been inspected.
    ListRecord empty = { .base.recordType = &test_list_type, .records = array_new(Record *, 0) };
    Record *later_bad[] = { (Record *)&empty, &wrong };
    assert_gears_callbacks_reject(later_bad, 2, false);
    array_free(empty.records);
}

MU_TEST(test_gears_bad_children_fail_before_reply) {
    Record wrong = { .recordType = &test_map_type };
    ListRecord list = { .base.recordType = &test_list_type, .records = array_new(Record *, 2) };
    list.records = array_append(list.records, &wrong);
    Record *records[] = { (Record *)&list };
    assert_gears_callbacks_reject(records, 1, false);
    list.records[0] = NULL;
    assert_gears_callbacks_reject(records, 1, false);
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
    gears_resp_flags = REDISMODULE_CTX_FLAGS_RESP3;
    mget_done_gears(ctx, NULL, NULL); // odd key/value count
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
    __typeof__(RedisModule_ReplyWithMap) saved_map = RedisModule_ReplyWithMap;
    __typeof__(RedisModule_ReplyWithSet) saved_set = RedisModule_ReplyWithSet;
    __typeof__(RedisModule_ReplyWithArray) saved_array = RedisModule_ReplyWithArray;
    __typeof__(RedisModule_GetContextFlags) saved_flags = RedisModule_GetContextFlags;
    __typeof__(RedisModule_Log) saved_log = RedisModule_Log;
    RedisModule_ReplyWithError = gears_reply_error;
    RedisModule_ReplyWithMap = RedisModule_ReplyWithSet = RedisModule_ReplyWithArray =
        gears_reply_container;
    RedisModule_GetContextFlags = gears_context_flags;
    RedisModule_Log = gears_log;
    MU_RUN_TEST(test_gears_bad_records_fail_before_reply);
    MU_RUN_TEST(test_gears_bad_children_fail_before_reply);
    MU_RUN_TEST(test_gears_valid_shapes_and_late_bad_children);
    RedisModule_ReplyWithError = saved_error;
    RedisModule_ReplyWithMap = saved_map;
    RedisModule_ReplyWithSet = saved_set;
    RedisModule_ReplyWithArray = saved_array;
    RedisModule_GetContextFlags = saved_flags;
    RedisModule_Log = saved_log;
}
