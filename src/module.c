/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/* REDISMODULE_MAIN is defined by the build (CC_DEFS) so that it is in effect
 * before the force-included common.h pulls in redismodule.h; defining it here
 * would be too late to allocate storage for the API function pointers. */

#include "module.h"

#include "compaction.h"
#include "common.h"
#include "config.h"
#include "indexer.h"
#include "libmr_commands.h"
#include "libmr_integration.h"
#include "query_language.h"
#include "rdb.h"
#include "reply.h"
#include "resultset.h"
#include "short_read.h"
#include "tsdb.h"
#include "version.h"

#include "LibMR/src/cluster.h"
#include "LibMR/src/mr.h"
#include "RedisModulesSDK/redismodule.h"
#include "rmutil/alloc.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "cmd_info/command_info.h"
#include "utils/blocked_client.h"

#include <ctype.h>
#include <inttypes.h>
#include <limits.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#ifndef REDISTIMESERIES_GIT_SHA
#define REDISTIMESERIES_GIT_SHA "unknown"
#endif

#define TIMESERIES_MODULE_ACL_CATEGORY_NAME "timeseries"

#define SetCommandAcls(ctx, cmd, acls)                                                             \
    {                                                                                              \
        if (RedisModule_GetCommand && RedisModule_AddACLCategory &&                                \
            RedisModule_SetCommandACLCategories) {                                                 \
            RedisModuleCommand *command = RedisModule_GetCommand(ctx, cmd);                        \
            if (command == NULL) {                                                                 \
                return REDISMODULE_ERR;                                                            \
            }                                                                                      \
                                                                                                   \
            const char *categories = acls " " TIMESERIES_MODULE_ACL_CATEGORY_NAME;                 \
            const int ret = RedisModule_SetCommandACLCategories(command, categories);              \
                                                                                                   \
            if (ret != REDISMODULE_OK) {                                                           \
                return REDISMODULE_ERR;                                                            \
            }                                                                                      \
        }                                                                                          \
    }

#define RegisterCommandWithModesAndAcls(ctx, cmd, f, mode, acls)                                   \
    __rmutil_register_cmd(ctx, cmd, f, mode);                                                      \
    SetCommandAcls(ctx, cmd, acls);

static bool LoadConfiguration(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    InitConfig();

    const bool isConfigApiSupported = RTS_RedisSupportsModuleConfigApi();

    if (ReadDeprecatedLoadTimeConfig(ctx, argv, argc, isConfigApiSupported) == TSDB_ERROR) {
        RedisModule_Log(
            ctx,
            "warning",
            "Failed to parse the deprecated RedisTimeSeries configurations, aborting...");

        FreeConfig();

        return false;
    }

    if (!isConfigApiSupported) {
        // Nothing else to do here.
        return true;
    }

    if (!RegisterConfigurationOptions(ctx)) {
        RedisModule_Log(
            ctx, "warning", "Failed to register the RedisTimeSeries configurations, aborting...");

        FreeConfig();

        return false;
    }

    if (RedisModule_LoadConfigs(ctx) != REDISMODULE_OK) {
        RedisModule_Log(
            ctx, "warning", "Failed to load the RedisTimeSeries configurations, aborting...");

        FreeConfig();

        return false;
    }

    return true;
}

RedisModuleType *SeriesType;
RedisModuleCtx *rts_staticCtx; // global redis ctx
bool isReshardTrimming = false, isAsmTrimming = false, isAsmImporting = false;

static void FreeConfigAndStaticCtx(void) {
    FreeConfig();
    if (rts_staticCtx != NULL) {
        RedisModule_FreeThreadSafeContext(rts_staticCtx);
        rts_staticCtx = NULL;
    }
}

User_Ctx_t GetUserFromContext(RedisModuleCtx *ctx) {
    const User_Ctx_t empty = { .user = NULL, .is_owned = false };

    if (!API_USER_CONTEXT_SUPPORTED)
        return empty;

    /* Fast path: ctx already has a user attached. Return it borrowed (is_owned=false): the ctx
     * (or whoever attached it via SetContextUser) is responsible for its lifetime, so the caller
     * MUST NOT free it. const is cast away to match the non-const RedisModuleUser* expected by
     * the ACL/free APIs; this mirrors the existing pattern in libmr_integration.c. */
    const RedisModuleUser *ctxUser = RedisModule_GetContextUser(ctx);
    if (ctxUser) {
        return (User_Ctx_t){ .user = (RedisModuleUser *)ctxUser, .is_owned = false };
    }

    /* Slow path: no user on the ctx. Look one up by current username. GetCurrentUserName returns
     * a string registered on the ctx's auto-memory; we free it explicitly so it doesn't linger
     * until the ctx is destroyed (effectively a slow leak on long-lived ctxs like rts_staticCtx).
     * The resulting RedisModuleUser is freshly allocated and owned by the caller. */
    RedisModuleString *userName = RedisModule_GetCurrentUserName(ctx);
    if (!userName)
        return empty;

    RedisModuleUser *user = RedisModule_GetModuleUserFromUserName(userName);
    RedisModule_FreeString(ctx, userName);
    return (User_Ctx_t){ .user = user, .is_owned = (user != NULL) };
}

int TSDB_info(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 2 || argc > 3) {
        return RedisModule_WrongArity(ctx);
    }

    Series *series;
    RedisModuleKey *key;
    const GetSeriesResult status =
        GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ, GetSeriesFlags_DeleteReferences);
    if (status != GetSeriesResult_Success) {
        return REDISMODULE_ERR;
    }

    const bool reply_map = _ReplyMap(ctx);

    const int is_debug = RMUtil_ArgExists("DEBUG", argv, argc, 1);
    if (is_debug) {
        ReplyWithMapOrArray(ctx, 16 * 2, true); // 16 fields x 2 (key + value)
    } else {
        ReplyWithMapOrArray(ctx, 14 * 2, true); // 14 fields x 2 (key + value)
    }

    long long skippedSamples;
    long long firstTimestamp = getFirstValidTimestamp(series, &skippedSamples);

    RedisModule_ReplyWithSimpleString(ctx, "totalSamples");
    RedisModule_ReplyWithLongLong(ctx, SeriesGetNumSamples(series) - skippedSamples);
    RedisModule_ReplyWithSimpleString(ctx, "memoryUsage");
    RedisModule_ReplyWithLongLong(ctx, SeriesMemUsage(series));
    RedisModule_ReplyWithSimpleString(ctx, "firstTimestamp");
    RedisModule_ReplyWithLongLong(ctx, firstTimestamp);
    RedisModule_ReplyWithSimpleString(ctx, "lastTimestamp");
    RedisModule_ReplyWithLongLong(ctx, series->lastTimestamp);
    RedisModule_ReplyWithSimpleString(ctx, "retentionTime");
    RedisModule_ReplyWithLongLong(ctx, series->retentionTime);
    RedisModule_ReplyWithSimpleString(ctx, "chunkCount");
    RedisModule_ReplyWithLongLong(ctx, RedisModule_DictSize(series->chunks));
    RedisModule_ReplyWithSimpleString(ctx, "chunkSize");
    RedisModule_ReplyWithLongLong(ctx, series->chunkSizeBytes);
    RedisModule_ReplyWithSimpleString(ctx, "chunkType");
    RedisModule_ReplyWithSimpleString(ctx, ChunkTypeToString(series->options));
    RedisModule_ReplyWithSimpleString(ctx, "duplicatePolicy");
    if (series->duplicatePolicy != DP_NONE) {
        RedisModule_ReplyWithSimpleString(ctx, DuplicatePolicyToString(series->duplicatePolicy));
    } else {
        RedisModule_ReplyWithNull(ctx);
    }

    RedisModule_ReplyWithSimpleString(ctx, "labels");
    ReplyWithSeriesLabels(ctx, series);

    RedisModule_ReplyWithSimpleString(ctx, "sourceKey");
    if (series->srcKey == NULL) {
        RedisModule_ReplyWithNull(ctx);
    } else {
        RedisModule_ReplyWithString(ctx, series->srcKey);
    }

    RedisModule_ReplyWithSimpleString(ctx, "rules");
    ReplyWithMapOrArray(ctx, REDISMODULE_POSTPONED_LEN, false);
    CompactionRule *rule = series->rules;
    int ruleCount = 0;
    while (rule != NULL) {
        if (!reply_map) {
            RedisModule_ReplyWithArray(ctx, 4);
        }
        RedisModule_ReplyWithString(ctx, rule->destKey);
        if (reply_map) {
            RedisModule_ReplyWithArray(ctx, 3);
        }
        RedisModule_ReplyWithLongLong(ctx, rule->bucketDuration);
        RedisModule_ReplyWithSimpleString(ctx, AggTypeEnumToString(rule->aggType));
        RedisModule_ReplyWithLongLong(ctx, rule->timestampAlignment);

        rule = rule->nextRule;
        ruleCount++;
    }
    ReplySetMapOrArrayLength(ctx, ruleCount, false);

    RedisModule_ReplyWithSimpleString(ctx, "ignoreMaxTimeDiff");
    RedisModule_ReplyWithLongLong(ctx, series->ignoreMaxTimeDiff);
    RedisModule_ReplyWithSimpleString(ctx, "ignoreMaxValDiff");
    RedisModule_ReplyWithDouble(ctx, series->ignoreMaxValDiff);

    if (is_debug) {
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, ">", "", 0);
        Chunk_t *chunk = NULL;
        int chunkCount = 0;
        RedisModule_ReplyWithSimpleString(ctx, "keySelfName");
        RedisModule_ReplyWithString(ctx, series->keyName);
        RedisModule_ReplyWithSimpleString(ctx, "Chunks");
        RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
        while (RedisModule_DictNextC(iter, NULL, (void *)&chunk)) {
            uint64_t numOfSamples = series->funcs->GetNumOfSample(chunk);
            size_t chunkSize = series->funcs->GetChunkSize(chunk, false);
            if (!reply_map) {
                RedisModule_ReplyWithArray(ctx, 5 * 2);
            } else {
                RedisModule_ReplyWithMap(ctx, 5);
            }

            RedisModule_ReplyWithSimpleString(ctx, "startTimestamp");
            RedisModule_ReplyWithLongLong(
                ctx, numOfSamples == 0 ? -1 : series->funcs->GetFirstTimestamp(chunk));
            RedisModule_ReplyWithSimpleString(ctx, "endTimestamp");
            RedisModule_ReplyWithLongLong(
                ctx, numOfSamples == 0 ? -1 : series->funcs->GetLastTimestamp(chunk));
            RedisModule_ReplyWithSimpleString(ctx, "samples");
            RedisModule_ReplyWithLongLong(ctx, numOfSamples);
            RedisModule_ReplyWithSimpleString(ctx, "size");
            RedisModule_ReplyWithLongLong(ctx, chunkSize);
            RedisModule_ReplyWithSimpleString(ctx, "bytesPerSample");
            RedisModule_ReplyWithDouble(
                ctx, (numOfSamples == 0) ? (float)0 : (float)chunkSize / numOfSamples);
            chunkCount++;
        }
        RedisModule_DictIteratorStop(iter);
        RedisModule_ReplySetArrayLength(ctx, chunkCount);
    }
    RedisModule_CloseKey(key);

    return REDISMODULE_OK;
}

void _TSDB_queryindex_impl(RedisModuleCtx *ctx, QueryPredicateList *queries) {
    RedisModuleDict *result = QueryIndex(ctx, queries->list, queries->count, NULL);

    ReplyWithSetOrArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;
    long long replylen = 0;
    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModule_ReplyWithStringBuffer(ctx, currentKey, currentKeyLen);
        replylen++;
    }
    RedisModule_DictIteratorStop(iter);
    ReplySetSetOrArrayLength(ctx, replylen);
}

int TSDB_queryindex(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    int query_count = argc - 1;

    int response = 0;
    QueryPredicateList *queries = parseLabelListFromArgs(ctx, argv, 1, query_count, &response);
    if (response == TSDB_ERROR) {
        QueryPredicateList_Free(queries);
        return RTS_ReplyGeneralError(ctx, "TSDB: failed parsing labels");
    }

    if (CountPredicateType(queries, EQ) + CountPredicateType(queries, LIST_MATCH) == 0) {
        QueryPredicateList_Free(queries);
        return RTS_ReplyGeneralError(ctx, "TSDB: please provide at least one matcher");
    }

    if (IsMRCluster()) {
        int ctxFlags = RedisModule_GetContextFlags(ctx);

        if (ctxFlags & (REDISMODULE_CTX_FLAGS_LUA | REDISMODULE_CTX_FLAGS_MULTI |
                        REDISMODULE_CTX_FLAGS_DENY_BLOCKING)) {
            RedisModule_ReplyWithError(ctx,
                                       "Can not run multi sharded command inside a multi exec, "
                                       "lua, or when blocking is not allowed");
            return REDISMODULE_OK;
        }
        TSDB_queryindex_MR(ctx, queries);
    } else {
        _TSDB_queryindex_impl(ctx, queries);
    }

    QueryPredicateList_Free(queries);
    return REDISMODULE_OK;
}

// multi-series groupby logic
static int replyGroupedMultiRange(RedisModuleCtx *ctx,
                                  TS_ResultSet *resultset,
                                  RedisModuleDict *result,
                                  const MRangeArgs *args) {
    RedisModuleDictIter *iter;
    char *currentKey = NULL;
    size_t currentKeyLen;
    Series *series = NULL;
    int exitStatus = REDISMODULE_OK;

    if (CheckDictSeriesPermissions(
            ctx, result, GetSeriesFlags_CheckForAcls | GetSeriesFlags_SilentOperation) ==
        GetSeriesResult_PermissionError) {
        RTS_ReplyKeyPermissionsError(ctx);
        exitStatus = REDISMODULE_ERR;
        goto exit;
    }

    iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);

    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModuleKey *key;
        // ACL permissions were already validated by CheckDictSeriesPermissions above.
        const GetSeriesResult status =
            GetSeries(ctx,
                      RedisModule_CreateString(ctx, currentKey, currentKeyLen),
                      &key,
                      &series,
                      REDISMODULE_READ,
                      GetSeriesFlags_SilentOperation);
        if (status != GetSeriesResult_Success) {
            // The iterator may have been invalidated, stop and restart from after the current
            // key.
            RedisModule_DictIteratorStop(iter);
            iter = RedisModule_DictIteratorStartC(result, ">", currentKey, currentKeyLen);
            continue;
        }

        ResultSet_AddSeries(resultset, series, RedisModule_StringPtrLen(series->keyName, NULL));
        RedisModule_CloseKey(key);
    }
    RedisModule_DictIteratorStop(iter);

    // todo: this is duplicated in resultset.c
    // Apply the reducer
    ResultSet_ApplyReducer(ctx, resultset, &args->rangeArgs, &args->groupByReducerArgs);

    // Do not apply the aggregation on the resultset, do apply max results on the final result
    RangeArgs minimizedArgs = args->rangeArgs;
    minimizedArgs.startTimestamp = 0;
    minimizedArgs.endTimestamp = UINT64_MAX;
    minimizedArgs.aggregationArgs.numClasses = 0;
    minimizedArgs.aggregationArgs.classes = NULL;
    minimizedArgs.aggregationArgs.timeDelta = 0;
    minimizedArgs.filterByTSArgs.hasValue = false;
    minimizedArgs.filterByValueArgs.hasValue = false;
    minimizedArgs.latest = false;

    replyResultSet(ctx,
                   resultset,
                   args->withLabels,
                   (RedisModuleString **)args->limitLabels,
                   args->numLimitLabels,
                   &minimizedArgs,
                   args->reverse);
exit:
    ResultSet_Free(resultset);
    return exitStatus;
}

GetSeriesResult CheckDictSeriesPermissions(RedisModuleCtx *ctx,
                                           RedisModuleDict *dict,
                                           const GetSeriesFlags flags) {
    // Resolve the user once for the whole dict scan; ACL is checked inline
    // per key below so GetSeries is called without CheckForAcls and doesn't
    // re-resolve the user per key.
    const bool checkAcls = flags & GetSeriesFlags_CheckForAcls;
    User_Ctx_t userCtx = { .user = NULL, .is_owned = false };
    if (checkAcls) {
        userCtx = GetUserFromContext(ctx);
    }
    const GetSeriesFlags childFlags = flags & ~GetSeriesFlags_CheckForAcls;

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(dict, "^", NULL, 0);
    RedisModuleString *currentKey;
    Series *series;
    GetSeriesResult ret = GetSeriesResult_Success;

    while ((currentKey = RedisModule_DictNext(ctx, iter, NULL)) != NULL) {
        if (checkAcls && !CheckKeyIsAllowedToRead(userCtx.user, currentKey)) {
            RedisModule_Log(
                ctx, "warning", "The user lacks the required permissions for the key, stopping.");
            RedisModule_FreeString(ctx, currentKey);
            ret = GetSeriesResult_PermissionError;
            break;
        }
        RedisModuleKey *key;
        const GetSeriesResult status =
            GetSeries(ctx, currentKey, &key, &series, REDISMODULE_READ, childFlags);
        RedisModule_FreeString(ctx, currentKey);
        if (status == GetSeriesResult_Success) {
            RedisModule_CloseKey(key);
        }
    }

    RedisModule_DictIteratorStop(iter);
    FreeUser(&userCtx);
    return ret;
}

int replyUngroupedMultiRange(RedisModuleCtx *ctx, RedisModuleDict *result, const MRangeArgs *args) {
    RedisModuleDictIter *iter;
    RedisModuleString *currentKey;
    long long replylen = 0;
    Series *series;
    if (CheckDictSeriesPermissions(
            ctx, result, GetSeriesFlags_CheckForAcls | GetSeriesFlags_SilentOperation) ==
        GetSeriesResult_PermissionError) {
        RTS_ReplyKeyPermissionsError(ctx);
        return REDISMODULE_ERR;
    }
    iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    ReplyWithMapOrArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN, false);
    while ((currentKey = RedisModule_DictNext(ctx, iter, NULL)) != NULL) {
        RedisModuleKey *key;
        // ACL permissions were already validated by CheckDictSeriesPermissions above.
        const GetSeriesResult status = GetSeries(
            ctx, currentKey, &key, &series, REDISMODULE_READ, GetSeriesFlags_SilentOperation);
        if (status != GetSeriesResult_Success) {
            // The iterator may have been invalidated, stop and restart from after the current key.
            RedisModule_DictIteratorStop(iter);
            iter = RedisModule_DictIteratorStart(result, ">", currentKey);
            RedisModule_FreeString(ctx, currentKey);
            continue;
        }

        ReplySeriesArrayPos(ctx,
                            series,
                            args->withLabels,
                            (RedisModuleString **)args->limitLabels,
                            args->numLimitLabels,
                            &args->rangeArgs,
                            args->reverse,
                            false);
        replylen++;
        RedisModule_CloseKey(key);
        RedisModule_FreeString(ctx, currentKey);
    }

    RedisModule_DictIteratorStop(iter);
    ReplySetMapOrArrayLength(ctx, replylen, false);
    return REDISMODULE_OK;
}

static int TSDB_generic_mrange(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool rev) {
    MRangeArgs args;
    if (parseMRangeCommand(ctx, argv, argc, &args) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    args.reverse = rev;

    bool hasPermissionError = false;
    RedisModuleDict *resultSeries = QueryIndex(
        ctx, args.queryPredicates->list, args.queryPredicates->count, &hasPermissionError);

    if (hasPermissionError) {
        MRangeArgs_Free(&args);
        RTS_ReplyKeyPermissionsError(ctx);
        return REDISMODULE_ERR;
    }

    int result = REDISMODULE_OK;
    if (args.groupByLabel) {
        TS_ResultSet *resultset = ResultSet_Create();
        ResultSet_GroupbyLabel(resultset, args.groupByLabel);

        result = replyGroupedMultiRange(ctx, resultset, resultSeries, &args);
    } else {
        result = replyUngroupedMultiRange(ctx, resultSeries, &args);
    }

    MRangeArgs_Free(&args);
    return result;
}

int TSDB_mrange(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (IsMRCluster()) {
        int ctxFlags = RedisModule_GetContextFlags(ctx);

        if (ctxFlags & (REDISMODULE_CTX_FLAGS_LUA | REDISMODULE_CTX_FLAGS_MULTI |
                        REDISMODULE_CTX_FLAGS_DENY_BLOCKING)) {
            RedisModule_ReplyWithError(ctx,
                                       "Can not run multi sharded command inside a multi exec, "
                                       "lua, or when blocking is not allowed");
            return REDISMODULE_OK;
        }
        return TSDB_mrange_MR(ctx, argv, argc, false);
    }

    return TSDB_generic_mrange(ctx, argv, argc, false);
}

int TSDB_mrevrange(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (IsMRCluster()) {
        int ctxFlags = RedisModule_GetContextFlags(ctx);

        if (ctxFlags & (REDISMODULE_CTX_FLAGS_LUA | REDISMODULE_CTX_FLAGS_MULTI |
                        REDISMODULE_CTX_FLAGS_DENY_BLOCKING)) {
            RedisModule_ReplyWithError(ctx,
                                       "Can not run multi sharded command inside a multi exec, "
                                       "lua, or when blocking is not allowed");
            return REDISMODULE_OK;
        }
        return TSDB_mrange_MR(ctx, argv, argc, true);
    }
    return TSDB_generic_mrange(ctx, argv, argc, true);
}

int TSDB_generic_range(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool rev) {
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    Series *series;
    RedisModuleKey *key;
    const GetSeriesResult status =
        GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ, GetSeriesFlags_CheckForAcls);
    if (status != GetSeriesResult_Success) {
        return REDISMODULE_ERR;
    }

    RangeArgs rangeArgs = { 0 };
    if (parseRangeArguments(ctx, 2, argv, argc, &rangeArgs) != REDISMODULE_OK) {
        goto _out;
    }

    ReplySeriesRange(ctx, series, &rangeArgs, rev);

_out:
    free(rangeArgs.aggregationArgs.classes);
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

int TSDB_range(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return TSDB_generic_range(ctx, argv, argc, false);
}

int TSDB_revrange(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return TSDB_generic_range(ctx, argv, argc, true);
}

// NRANGE takes one aggregator per key, space-separated, instead of the single comma-joined token
// the shared range parser expects. We splice the numKeys aggregator tokens after AGGREGATION into
// one comma-joined token, leaving the rest of the argument vector untouched so parseRangeArguments
// (and its EMPTY/BUCKETTIMESTAMP offset math) keeps working unchanged. Returns a malloc'd argv the
// caller frees (with *out_joined, the synthesized token), *out_argc set; or NULL when no rewrite
// applies. On a bad request it replies, sets *err, and returns NULL.
static RedisModuleString **nrange_splice_aggregators(RedisModuleCtx *ctx,
                                                     RedisModuleString **argv,
                                                     int argc,
                                                     int rangeStart,
                                                     long long numKeys,
                                                     int *out_argc,
                                                     RedisModuleString **out_joined,
                                                     int *err) {
    *err = 0;
    *out_joined = NULL;
    const int aggRel = RMUtil_ArgIndex("AGGREGATION", argv + rangeStart, argc - rangeStart);
    if (numKeys <= 1 || aggRel < 0) {
        return NULL; // a single aggregator is already one token, or there is no aggregation
    }
    const int firstAgg = rangeStart + aggRel + 1; // first aggregator token

    const int n = (int)numKeys;

    // Validate and comma-join the n aggregator tokens (one per key) in a single pass; the numeric
    // bucketDuration follows them. joined starts empty so the error path can free unconditionally.
    RedisModuleString *joined = RedisModule_CreateString(ctx, "", 0);
    for (int i = 0; i < n; i++) {
        if (firstAgg + i >= argc || RMStringLenAggTypeToEnum(argv[firstAgg + i]) < 0) {
            // A missing/numeric slot means the bucketDuration arrived early (too few aggregators);
            // any other token is a genuine unknown aggregator.
            long long bucket;
            if (firstAgg + i >= argc ||
                RedisModule_StringToLongLong(argv[firstAgg + i], &bucket) == REDISMODULE_OK)
                RTS_ReplyGeneralError(ctx,
                                      "TSDB: the number of aggregators must be equal to numkeys");
            else
                RTS_ReplyGeneralError(ctx, "TSDB: Unknown aggregation type");
            goto fail;
        }
        size_t len;
        const char *s = RedisModule_StringPtrLen(argv[firstAgg + i], &len);
        if (i)
            RedisModule_StringAppendBuffer(ctx, joined, ",", 1);
        RedisModule_StringAppendBuffer(ctx, joined, s, len);
    }
    // A valid aggregator where the bucketDuration belongs means more aggregators than keys.
    if (firstAgg + n < argc && RMStringLenAggTypeToEnum(argv[firstAgg + n]) >= 0) {
        RTS_ReplyGeneralError(ctx, "TSDB: the number of aggregators must be equal to numkeys");
        goto fail;
    }

    // Splice: [..AGGREGATION] joined [bucketDuration..end], dropping the n-1 extra agg tokens.
    const int newArgc = argc - (n - 1);
    RedisModuleString **out = malloc(newArgc * sizeof(*out));
    memcpy(out, argv, firstAgg * sizeof(*out));
    out[firstAgg] = joined;
    memcpy(out + firstAgg + 1, argv + firstAgg + n, (argc - firstAgg - n) * sizeof(*out));
    *out_argc = newArgc;
    *out_joined = joined;
    return out;

fail:
    RedisModule_FreeString(ctx, joined);
    *err = 1;
    return NULL;
}

// TS.NRANGE/TS.NREVRANGE numkeys key [key...] fromTimestamp toTimestamp [options]
//   [LATEST] [FILTER_BY_TS ts...] [FILTER_BY_VALUE min max] [COUNT count]
//   [[ALIGN align] AGGREGATION agg [agg ...] bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
// Like TS.RANGE but over an explicit list of same-slot keys, returning results
// pivoted by timestamp: one row [timestamp, [value-per-key...]] in key order,
// NaN where a key has no sample at that timestamp. With AGGREGATION, the aggregators
// are space-separated, one per key in key order, and their count must equal numkeys;
// all share a single bucketDuration.
int TSDB_generic_nrange(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool rev) {
    // argv: [0]=cmd [1]=numkeys [2..1+numkeys]=keys [2+numkeys]=from [3+numkeys]=to ...
    if (argc < 5) {
        return RedisModule_WrongArity(ctx);
    }

    long long numKeys;
    if (RedisModule_StringToLongLong(argv[1], &numKeys) != REDISMODULE_OK || numKeys <= 0) {
        RTS_ReplyGeneralError(ctx, "TSDB: numkeys must be a positive integer");
        return REDISMODULE_ERR;
    }

    // cmd + numkeys + from + to = 4 non-key args. Compare against the argc bound
    // rather than computing 2 + numKeys + 2: numKeys is user-controlled, so that
    // sum overflows for values near LLONG_MAX, wraps negative, and would skip this
    // guard (then calloc(numKeys, ...) returns NULL and gets dereferenced). This
    // form can't overflow (argc is a small int, and argc >= 5 above) and also
    // bounds numKeys <= argc so the per-key allocations below can't realistically fail.
    if (numKeys > (long long)argc - 4) {
        return RedisModule_WrongArity(ctx);
    }

    const int rangeStart = (int)(2 + numKeys);

    // Everything below frees through the cleanup label; declare it up front so the rewrite/parse
    // error paths can goto there too.
    RangeArgs rangeArgs = { 0 };
    RedisModuleKey **keys = NULL;
    Series **series = NULL;
    AbstractSampleIterator **iters = NULL;
    RedisModuleString *joinedAgg = NULL; // synthesized comma token, NULL if no rewrite
    RedisModuleString **rewrittenArgv = NULL;
    size_t numClasses = 0;
    int rv = REDISMODULE_ERR;
    size_t opened = 0;

    // Collapse the space-separated per-key aggregators into the comma form the shared parser wants.
    int rewrite_err = 0, rangeArgc = argc;
    rewrittenArgv = nrange_splice_aggregators(
        ctx, argv, argc, rangeStart, numKeys, &rangeArgc, &joinedAgg, &rewrite_err);
    if (rewrite_err) {
        goto cleanup;
    }

    if (parseRangeArguments(
            ctx, rangeStart, rewrittenArgv ? rewrittenArgv : argv, rangeArgc, &rangeArgs) !=
        REDISMODULE_OK) {
        goto cleanup;
    }

    numClasses = rangeArgs.aggregationArgs.numClasses;
    if (numClasses != 0 && numClasses != (size_t)numKeys) {
        RTS_ReplyGeneralError(ctx, "TSDB: the number of aggregators must be equal to numkeys");
        goto cleanup;
    }

    keys = calloc(numKeys, sizeof(RedisModuleKey *));
    series = calloc(numKeys, sizeof(Series *));

    for (; opened < (size_t)numKeys; opened++) {
        const GetSeriesResult status = GetSeries(ctx,
                                                 argv[2 + opened],
                                                 &keys[opened],
                                                 &series[opened],
                                                 REDISMODULE_READ,
                                                 GetSeriesFlags_CheckForAcls);
        if (status != GetSeriesResult_Success) {
            goto cleanup; // GetSeries already replied with the error
        }
    }

    iters = malloc(numKeys * sizeof(AbstractSampleIterator *));
    for (size_t i = 0; i < (size_t)numKeys; i++) {
        RangeArgs perKey = rangeArgs; // shares filters; classes is read-only here
        if (numClasses == 0) {
            perKey.aggregationArgs.numClasses = 0;
            perKey.aggregationArgs.classes = NULL;
        } else {
            perKey.aggregationArgs.numClasses = 1;
            perKey.aggregationArgs.classes = &rangeArgs.aggregationArgs.classes[i];
        }
        iters[i] = SeriesCreateSampleIterator(series[i], &perKey, rev, true);
    }

    ReplySeriesNRange(ctx, iters, numKeys, rangeArgs.count, rev); // closes each iterator
    rv = REDISMODULE_OK;

cleanup:
    free(iters);
    free(rangeArgs.aggregationArgs.classes);
    free(rewrittenArgv);
    if (joinedAgg)
        RedisModule_FreeString(ctx, joinedAgg);
    for (size_t i = 0; i < opened; i++) {
        RedisModule_CloseKey(keys[i]);
    }
    free(keys);
    free(series);
    return rv;
}

int TSDB_nrange(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return TSDB_generic_nrange(ctx, argv, argc, false);
}

int TSDB_nrevrange(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return TSDB_generic_nrange(ctx, argv, argc, true);
}

static int internalAdd(RedisModuleCtx *ctx,
                       Series *series,
                       api_timestamp_t timestamp,
                       double value,
                       DuplicatePolicy dp_override,
                       bool should_reply);

static void handleCompaction(RedisModuleCtx *ctx,
                             Series *series,
                             CompactionRule *rule,
                             api_timestamp_t timestamp,
                             double value) {
    timestamp_t currentTimestamp =
        CalcBucketStart(timestamp, rule->bucketDuration, rule->timestampAlignment);
    timestamp_t currentTimestampNormalized = BucketStartNormalize(currentTimestamp);

    if (rule->startCurrentTimeBucket == -1LL) {
        // first sample, lets init the startCurrentTimeBucket
        rule->startCurrentTimeBucket = currentTimestampNormalized;

        if (rule->aggClass->type == TS_AGG_TWA) {
            rule->aggClass->addBucketParams(rule->aggContext,
                                            currentTimestampNormalized,
                                            currentTimestamp + rule->bucketDuration);
        }
    }

    if (currentTimestampNormalized > rule->startCurrentTimeBucket) {
        Series *destSeries;
        RedisModuleKey *key;
        const GetSeriesFlags flags = GetSeriesFlags_SilentOperation | GetSeriesFlags_CheckForAcls;
        const GetSeriesResult status = GetSeries(
            ctx, rule->destKey, &key, &destSeries, REDISMODULE_READ | REDISMODULE_WRITE, flags);
        if (status != GetSeriesResult_Success) {
            // key doesn't exist anymore or some other error occurred,
            // and we don't do anything
            return;
        }

        if (rule->aggClass->type == TS_AGG_TWA && rule->aggClass->isValueValid(value)) {
            rule->aggClass->addNextBucketFirstSample(rule->aggContext, value, timestamp);
        }

        bool hadValidSamples = rule->validSamplesInBucket;
        if (hadValidSamples) {
            double aggVal;
            if (rule->aggClass->finalize(rule->aggContext, &aggVal) == TSDB_OK) {
                internalAdd(ctx, destSeries, rule->startCurrentTimeBucket, aggVal, DP_LAST, false);
                RedisModule_NotifyKeyspaceEvent(
                    ctx, REDISMODULE_NOTIFY_MODULE, "ts.add:dest", rule->destKey);
            }
        }
        Sample last_sample;
        if (rule->aggClass->type == TS_AGG_TWA) {
            rule->aggClass->getLastSample(rule->aggContext, &last_sample);
        }
        rule->aggClass->resetContext(rule->aggContext);
        rule->validSamplesInBucket = false;
        if (rule->aggClass->type == TS_AGG_TWA) {
            rule->aggClass->addBucketParams(rule->aggContext,
                                            currentTimestampNormalized,
                                            currentTimestamp + rule->bucketDuration);
        }

        if (rule->aggClass->type == TS_AGG_TWA && hadValidSamples &&
            rule->aggClass->isValueValid(last_sample.value)) {
            rule->aggClass->addPrevBucketLastSample(
                rule->aggContext, last_sample.value, last_sample.timestamp);
        }
        rule->startCurrentTimeBucket = currentTimestampNormalized;
        RedisModule_CloseKey(key);
    }
    if (rule->aggClass->isValueValid(value)) {
        rule->aggClass->appendValue(rule->aggContext, value, timestamp);
        rule->validSamplesInBucket = true;
    }
}

static inline bool filter_close_samples(DuplicatePolicy dp_policy,
                                        const Series *series,
                                        api_timestamp_t timestamp,
                                        double value) {
    if (isnan(value) || isnan(series->lastValue)) {
        return false;
    }

    return dp_policy == DP_LAST && series->totalSamples != 0 &&
           timestamp >= series->lastTimestamp &&
           timestamp - series->lastTimestamp <= series->ignoreMaxTimeDiff &&
           fabs(value - series->lastValue) <= series->ignoreMaxValDiff;
}

static int internalAdd(RedisModuleCtx *ctx,
                       Series *series,
                       api_timestamp_t timestamp,
                       double value,
                       DuplicatePolicy dp_override,
                       bool should_reply) {
    const timestamp_t lastTS = series->lastTimestamp;
    const uint64_t retention = series->retentionTime;
    // ensure inside retention period.
    if (retention && timestamp < lastTS && retention < lastTS - timestamp) {
        RTS_ReplyGeneralError(ctx, "TSDB: Timestamp is older than retention");
        return REDISMODULE_ERR;
    }

    // Use module level configuration if key level configuration doesn't exist
    const DuplicatePolicy dp_policy =
        dp_override ?: series->duplicatePolicy ?: TSGlobalConfig.duplicatePolicy;

    // Insert filter for close samples. If configured, it's used to ignore last measurement if its
    // value is negligible compared to the last sample.
    if (filter_close_samples(dp_policy, series, timestamp, value)) {
        RedisModule_ReplyWithLongLong(ctx, series->lastTimestamp);
        return REDISMODULE_ERR;
    }

    if (timestamp <= series->lastTimestamp && series->totalSamples != 0) {
        if (SeriesUpsertSample(series, timestamp, value, dp_policy) != REDISMODULE_OK) {
            RTS_ReplyGeneralError(ctx,
                                  "TSDB: Error at upsert, update is not supported when "
                                  "DUPLICATE_POLICY is set to BLOCK mode, or either current or new "
                                  "value is NaN and DUPLICATE_POLICY is MAX/MIN/SUM");
            return REDISMODULE_ERR;
        }
    } else {
        SeriesAddSample(series, timestamp, value);
        // handle compaction rules
        if (series->rules) {
            const GetSeriesFlags flags =
                GetSeriesFlags_SilentOperation | GetSeriesFlags_CheckForAcls;
            deleteReferenceToDeletedSeries(ctx, series, flags);
        }

        for (CompactionRule *rule = series->rules; rule != NULL; rule = rule->nextRule) {
            handleCompaction(ctx, series, rule, timestamp, value);
        }
    }
    // Wake any TS.BGET waiters parked on this key. Cheap no-op when no client
    // is blocked; harmless extra try_reply when the upsert was an in-place
    // update (the reply_cb will re-check and stay parked if nothing changed).
    RedisModule_SignalKeyAsReady(ctx, series->keyName);

    if (should_reply) {
        RedisModule_ReplyWithLongLong(ctx, timestamp);
    }
    return REDISMODULE_OK;
}

static inline int add(RedisModuleCtx *ctx,
                      RedisModuleString *keyName,
                      const RedisModuleString *timestampStr,
                      const RedisModuleString *valueStr,
                      RedisModuleString **argv,
                      int argc) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);

    double value;
    if (!parse_double(valueStr, &value)) {
        RTS_ReplyGeneralError(ctx, "TSDB: invalid value");
        return REDISMODULE_ERR;
    }

    long long timestampValue;
    if (RedisModule_StringToLongLong(timestampStr, &timestampValue) != REDISMODULE_OK) {
        RTS_ReplyGeneralError(ctx, "TSDB: invalid timestamp");
        return REDISMODULE_ERR;
    }
    if (timestampValue < 0) {
        RTS_ReplyGeneralError(ctx, "TSDB: invalid timestamp, must be a nonnegative integer");
        return REDISMODULE_ERR;
    }
    const api_timestamp_t timestamp = (api_timestamp_t)timestampValue;

    Series *series = NULL;
    DuplicatePolicy dp = DP_NONE;

    if (argv != NULL && RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        // the key doesn't exist, lets check we have enough information to create one
        CreateCtx cCtx = { 0 };
        if (parseCreateArgs(ctx, argv, argc, &cCtx) != REDISMODULE_OK) {
            return REDISMODULE_ERR;
        }

        CreateTsKey(ctx, keyName, &cCtx, &series, &key);
        SeriesCreateRulesFromGlobalConfig(ctx, keyName, series, cCtx.labels, cCtx.labelsCount);
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType) {
        RTS_ReplyGeneralError(ctx, "TSDB: the key is not a TSDB key");
        return REDISMODULE_ERR;
    } else {
        series = RedisModule_ModuleTypeGetValue(key);
        //  override key and database configuration for DUPLICATE_POLICY
        if (argv != NULL &&
            ParseDuplicatePolicy(ctx, argv, argc, TS_ADD_DUPLICATE_POLICY_ARG, &dp, NULL) !=
                TSDB_OK) {
            return REDISMODULE_ERR;
        }
    }
    const int rv = internalAdd(ctx, series, timestamp, value, dp, true);
    RedisModule_CloseKey(key);
    return rv;
}

static inline RedisModuleString *getCurrentTime(RedisModuleCtx *ctx) {
    return RedisModule_CreateStringPrintf(ctx, "%llu", RedisModule_Milliseconds());
}

int TSDB_madd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4 || (argc - 1) % 3 != 0) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *curTimeStr = NULL;

    RedisModule_ReplyWithArray(ctx, (argc - 1) / 3);
    const RedisModuleString **replArgv = malloc((argc - 1) * sizeof *replArgv);
    const RedisModuleString **offset = replArgv;
    for (int i = 1; i < argc; i += 3) {
        RedisModuleString *keyName = argv[i];
        const RedisModuleString *timestampStr = argv[i + 1];
        const RedisModuleString *valueStr = argv[i + 2];

        if (stringEqualsC(timestampStr, "*")) {
            // if timestamp is "*", take current time (automatic timestamp)
            if (!curTimeStr) {
                curTimeStr = getCurrentTime(ctx);
            }
            timestampStr = curTimeStr;
        }

        if (add(ctx, keyName, timestampStr, valueStr, NULL, -1) == REDISMODULE_OK) {
            *offset++ = keyName;
            *offset++ = timestampStr;
            *offset++ = valueStr;
        }
    }
    const size_t replArgc = offset - replArgv;

    if (replArgc > 0) {
        // we want to replicate only successful sample inserts to avoid errors on the replica, when
        // this errors occurs, redis will CRITICAL error to its log and potentially fill up the disk
        // depending on the actual traffic.
        RedisModule_Replicate(ctx, "TS.MADD", "v", replArgv, replArgc);
    }
    free(replArgv);

    for (int i = 1; i < argc; i += 3) {
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_MODULE, "ts.add", argv[i]);
    }

    return REDISMODULE_OK;
}

int TSDB_add(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *keyName = argv[1];
    const RedisModuleString *timestampStr = argv[2];
    const RedisModuleString *valueStr = argv[3];

    if (stringEqualsC(timestampStr, "*")) {
        // if timestamp is "*", take current time (automatic timestamp)
        timestampStr = getCurrentTime(ctx);
    }

    const int result = add(ctx, keyName, timestampStr, valueStr, argv, argc);
    if (result == REDISMODULE_OK) {
        const size_t replArgc = argc - 1;
        const RedisModuleString **replArgv = malloc(replArgc * sizeof *replArgv);
        for (int i = 0; i < replArgc; i++) { // skip the command name
            replArgv[i] = argv[i + 1];
        }
        replArgv[1] = timestampStr; // In case the timestamp was "*"
        RedisModule_Replicate(ctx, "TS.ADD", "v", replArgv, replArgc);
        free(replArgv);
    }

    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_MODULE, "ts.add", keyName);

    return result;
}

int CreateTsKey(RedisModuleCtx *ctx,
                RedisModuleString *keyName,
                const CreateCtx *cCtx,
                Series **series,
                RedisModuleKey **key) {
    if (*key == NULL) {
        *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);
    }

    RedisModule_RetainString(ctx, keyName);
    *series = NewSeries(keyName, cCtx);
    if (RedisModule_ModuleTypeSetValue(*key, SeriesType, *series) == REDISMODULE_ERR) {
        return TSDB_ERROR;
    }

    IndexMetric(keyName, (*series)->labels, (*series)->labelsCount);

    return TSDB_OK;
}

int TSDB_create(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    Series *series;
    RedisModuleString *keyName = argv[1];
    CreateCtx cCtx = { 0 };
    if (parseCreateArgs(ctx, argv, argc, &cCtx) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(key);
        if (cCtx.labelsCount > 0) {
            FreeLabels(cCtx.labels, cCtx.labelsCount);
        }
        return RTS_ReplyGeneralError(ctx, "TSDB: key already exists");
    }

    CreateTsKey(ctx, keyName, &cCtx, &series, &key);
    RedisModule_CloseKey(key);

    RedisModule_Log(ctx, "verbose", "created new series");
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);

    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_MODULE, "ts.create", keyName);

    return REDISMODULE_OK;
}

int TSDB_alter(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    Series *series;
    RedisModuleKey *key;
    RedisModuleString *keyName = argv[1];
    CreateCtx cCtx = { 0 };
    if (parseCreateArgs(ctx, argv, argc, &cCtx) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    const GetSeriesResult status = GetSeries(
        ctx, argv[1], &key, &series, REDISMODULE_READ | REDISMODULE_WRITE, GetSeriesFlags_None);
    if (status != GetSeriesResult_Success) {
        return REDISMODULE_ERR;
    }
    if (RMUtil_ArgIndex("RETENTION", argv, argc) > 0) {
        series->retentionTime = cCtx.retentionTime;
    }

    if (RMUtil_ArgIndex("CHUNK_SIZE", argv, argc) > 0) {
        series->chunkSizeBytes = cCtx.chunkSizeBytes;
    }

    if (RMUtil_ArgIndex("DUPLICATE_POLICY", argv, argc) > 0) {
        series->duplicatePolicy = cCtx.duplicatePolicy;
    }

    if (RMUtil_ArgIndex("LABELS", argv, argc) > 0) {
        RemoveIndexedMetric(keyName);
        // free current labels
        FreeLabels(series->labels, series->labelsCount);

        // set new newLabels
        series->labels = cCtx.labels;
        series->labelsCount = cCtx.labelsCount;
        IndexMetric(keyName, series->labels, series->labelsCount);
    }

    if (RMUtil_ArgIndex("IGNORE", argv, argc) > 0) {
        series->ignoreMaxTimeDiff = cCtx.ignoreMaxTimeDiff;
        series->ignoreMaxValDiff = cCtx.ignoreMaxValDiff;
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_CloseKey(key);

    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_MODULE, "ts.alter", keyName);

    return REDISMODULE_OK;
}

/*
TS.DELETERULE SOURCE_KEY DEST_KEY
 */
int TSDB_deleteRule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *srcKeyName = argv[1];

    // First try to remove the rule from the source key
    Series *srcSeries;
    RedisModuleKey *srcKey;
    const GetSeriesResult statusS = GetSeries(ctx,
                                              srcKeyName,
                                              &srcKey,
                                              &srcSeries,
                                              REDISMODULE_READ | REDISMODULE_WRITE,
                                              GetSeriesFlags_DeleteReferences);
    if (statusS != GetSeriesResult_Success) {
        return REDISMODULE_ERR;
    }

    RedisModuleString *destKeyName = argv[2];
    if (!SeriesDeleteRule(srcSeries, destKeyName)) {
        RedisModule_CloseKey(srcKey);
        return RTS_ReplyGeneralError(ctx, "TSDB: compaction rule does not exist");
    }

    // If succeed to remove the rule from the source key remove from the destination too
    Series *destSeries;
    RedisModuleKey *destKey;
    const GetSeriesResult statusD = GetSeries(ctx,
                                              destKeyName,
                                              &destKey,
                                              &destSeries,
                                              REDISMODULE_READ | REDISMODULE_WRITE,
                                              GetSeriesFlags_DeleteReferences);
    if (statusD != GetSeriesResult_Success) {
        RedisModule_CloseKey(srcKey);
        return REDISMODULE_ERR;
    }
    SeriesDeleteSrcRule(destSeries, srcKeyName);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_CloseKey(srcKey);
    RedisModule_CloseKey(destKey);

    RedisModule_NotifyKeyspaceEvent(
        ctx, REDISMODULE_NOTIFY_MODULE, "ts.deleterule:src", srcKeyName);
    RedisModule_NotifyKeyspaceEvent(
        ctx, REDISMODULE_NOTIFY_MODULE, "ts.deleterule:dest", destKeyName);

    return REDISMODULE_OK;
}

/*
TS.CREATERULE sourceKey destKey AGGREGATION aggregationType bucketDuration
*/
int TSDB_createRule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 6 && argc != 7) {
        return RedisModule_WrongArity(ctx);
    }

    // Validate aggregation arguments
    api_timestamp_t bucketDuration;
    int aggTypes[TS_AGG_TYPES_MAX];
    size_t numAggTypes = 0;
    timestamp_t alignmentTS;
    const int result = _parseAggregationArgs(
        ctx, argv, argc, &bucketDuration, aggTypes, &numAggTypes, NULL, NULL, &alignmentTS);
    if (result == TSDB_NOTEXISTS) {
        return RedisModule_WrongArity(ctx);
    }
    if (result == TSDB_ERROR) {
        return REDISMODULE_ERR;
    }
    if (numAggTypes != 1) {
        return RTS_ReplyGeneralError(ctx, "TSDB: CREATERULE requires exactly one aggregation type");
    }
    int aggType = aggTypes[0];

    RedisModuleString *srcKeyName = argv[1];
    RedisModuleString *destKeyName = argv[2];
    if (!RedisModule_StringCompare(srcKeyName, destKeyName)) {
        return RTS_ReplyGeneralError(
            ctx, "TSDB: the source key and destination key should be different");
    }

    Series *srcSeries;
    RedisModuleKey *srcKey;
    const GetSeriesFlags flags = GetSeriesFlags_DeleteReferences | GetSeriesFlags_CheckForAcls;
    const GetSeriesResult statusS = GetSeries(
        ctx, srcKeyName, &srcKey, &srcSeries, REDISMODULE_READ | REDISMODULE_WRITE, flags);
    if (statusS != GetSeriesResult_Success) {
        return REDISMODULE_ERR;
    }

    // 1. Verify the source is not a destination
    if (srcSeries->srcKey) {
        RedisModule_CloseKey(srcKey);
        return RTS_ReplyGeneralError(ctx, "TSDB: the source key already has a source rule");
    }

    Series *destSeries;
    RedisModuleKey *destKey;
    const GetSeriesResult statusD = GetSeries(
        ctx, destKeyName, &destKey, &destSeries, REDISMODULE_READ | REDISMODULE_WRITE, flags);
    if (statusD != GetSeriesResult_Success) {
        RedisModule_CloseKey(srcKey);
        return REDISMODULE_ERR;
    }

    // 2. verify dst is not s source
    if (destSeries->rules) {
        RedisModule_CloseKey(srcKey);
        RedisModule_CloseKey(destKey);
        return RTS_ReplyGeneralError(ctx, "TSDB: the destination key already has a dst rule");
    }

    // 3. verify dst doesn't already have src,
    // 4. This covers also the scenario when the rule is already exists
    if (destSeries->srcKey) {
        RedisModule_CloseKey(srcKey);
        RedisModule_CloseKey(destKey);
        return RTS_ReplyGeneralError(ctx, "TSDB: the destination key already has a src rule");
    }

    // add src to dest
    SeriesSetSrcRule(ctx, destSeries, srcSeries->keyName);

    // Last add the rule to source
    if (SeriesAddRule(ctx, srcSeries, destSeries, aggType, bucketDuration, alignmentTS) == NULL) {
        RedisModule_CloseKey(srcKey);
        RedisModule_CloseKey(destKey);
        RedisModule_ReplyWithSimpleString(ctx, "TSDB: ERROR creating rule");
        return REDISMODULE_ERR;
    }
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);

    RedisModule_CloseKey(srcKey);
    RedisModule_CloseKey(destKey);

    RedisModule_NotifyKeyspaceEvent(
        ctx, REDISMODULE_NOTIFY_MODULE, "ts.createrule:src", srcKeyName);
    RedisModule_NotifyKeyspaceEvent(
        ctx, REDISMODULE_NOTIFY_MODULE, "ts.createrule:dest", destKeyName);

    return REDISMODULE_OK;
}

/*
TS.INCRBY ts_key NUMBER [TIMESTAMP timestamp]
*/
int TSDB_incrby(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *keyName = argv[1];
    Series *series;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    const bool keyExists = RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY;
    if (keyExists && RedisModule_ModuleTypeGetType(key) != SeriesType) {
        return RTS_ReplyGeneralError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    double incrby = 0;
    if (RMUtil_ParseArgs(argv, argc, 2, "d", &incrby) != REDISMODULE_OK) {
        return RTS_ReplyGeneralError(ctx, "TSDB: invalid increase/decrease value");
    }

    long long currentUpdatedTime = -1;
    int timestampLoc = RMUtil_ArgIndex("TIMESTAMP", argv, argc);
    if (timestampLoc == -1 || RMUtil_StringEqualsC(argv[timestampLoc + 1], "*")) {
        currentUpdatedTime = RedisModule_Milliseconds();
    } else if (RedisModule_StringToLongLong(argv[timestampLoc + 1],
                                            (long long *)&currentUpdatedTime) != REDISMODULE_OK) {
        return RTS_ReplyGeneralError(ctx, "TSDB: invalid timestamp");
    }

    if (!keyExists) {
        // the key doesn't exist, lets check we have enough information to create one
        CreateCtx cCtx = { 0 };
        if (parseCreateArgs(ctx, argv, argc, &cCtx) != REDISMODULE_OK) {
            return REDISMODULE_ERR;
        }

        CreateTsKey(ctx, keyName, &cCtx, &series, &key);
        SeriesCreateRulesFromGlobalConfig(ctx, keyName, series, cCtx.labels, cCtx.labelsCount);
    } else {
        series = RedisModule_ModuleTypeGetValue(key);
    }

    if (currentUpdatedTime < series->lastTimestamp && series->lastTimestamp != 0) {
        return RedisModule_ReplyWithError(
            ctx, "TSDB: timestamp must be equal to or higher than the maximum existing timestamp");
    }

    double result = series->lastValue;
    if (isnan(result)) {
        return RTS_ReplyGeneralError(ctx, "TSDB: cannot increment/decrement NaN value");
    }
    RMUtil_StringToLower(argv[0]);
    bool isIncr = RMUtil_StringEqualsC(argv[0], "ts.incrby");
    if (isIncr) {
        result += incrby;
    } else {
        result -= incrby;
    }

    int rv = internalAdd(ctx, series, currentUpdatedTime, result, DP_LAST, true);
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_CloseKey(key);

    RedisModule_NotifyKeyspaceEvent(
        ctx, REDISMODULE_NOTIFY_GENERIC, isIncr ? "ts.incrby" : "ts.decrby", argv[1]);

    return rv;
}

int TSDB_get(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 2 || argc > 3) {
        return RedisModule_WrongArity(ctx);
    }

    bool latest = false;
    Series *series;
    RedisModuleKey *key;
    const GetSeriesResult status =
        GetSeries(ctx, argv[1], &key, &series, REDISMODULE_READ, GetSeriesFlags_None);
    if (status != GetSeriesResult_Success) {
        return REDISMODULE_ERR;
    }

    if (argc == 3) {
        if (parseLatestArg(ctx, argv, argc, &latest) != REDISMODULE_OK || !latest) {
            RTS_ReplyGeneralError(ctx, "TSDB: wrong 3rd argument");
            return REDISMODULE_ERR;
        }
    }

    // LATEST is ignored for a series that is not a compaction.
    bool should_finalize_last_bucket = should_finalize_last_bucket_get(latest, series);
    if (should_finalize_last_bucket) {
        Sample sample;
        Sample *sample_ptr = &sample;
        calculate_latest_sample(&sample_ptr, series);
        if (sample_ptr) {
            ReplyWithSample(ctx, sample.timestamp, sample.value);
        } else {
            ReplyWithSeriesLastDatapoint(ctx, series);
        }
    } else {
        ReplyWithSeriesLastDatapoint(ctx, series);
    }

    RedisModule_CloseKey(key);

    return REDISMODULE_OK;
}

/* ============================================================================
 *  TS.BGET — Blocking GET (cursor-style tailing of a series)
 *
 *  Syntax:  TS.BGET key timestamp timeout [MIN_COUNT min_count] [MAX_COUNT max_count]
 *
 *  Returns up to `max_count` samples (default unlimited) with sample-ts greater
 *  than or equal to the resolved cursor, sorted ascending. Blocks up to
 *  `timeout` ms waiting until at least `min_count` qualifying samples exist
 *  (default 1). `timestamp` may be a literal UNIX-ms value or one of the
 *  sentinels `-` (earliest) / `+` (latest existing sample, inclusive) /
 *  `$` (latest sample's ts + 1, i.e. only post-command samples qualify).
 *
 *  Wake-up is driven by RedisModule_SignalKeyAsReady() in the sample-append
 *  chokepoint of the engine (covers TS.ADD / TS.MADD / TS.INCRBY / TS.DECRBY
 *  on append, plus compaction-rule writes to the destination key).
 *
 *  Sentinel support (all resolved once inside parse_bget_args):
 *    `-` -> cursor=0 (no lower bound).
 *    `+` -> cursor=series->lastTimestamp (latest existing sample qualifies,
 *           aligned with TS.RANGE); missing/empty -> 0; wrong-type -> WRONGTYPE.
 *    `$` -> cursor=series->lastTimestamp+1 (only post-command samples qualify);
 *           missing/empty -> 0; wrong-type -> WRONGTYPE.
 * ============================================================================
 */

/// TS.BGET MIN_COUNT default: unblock as soon as one sample qualifies.
#define BGET_DEFAULT_MIN_COUNT 1
/// TS.BGET MAX_COUNT default: unlimited (maps to RangeArgs.count == -1).
#define BGET_DEFAULT_MAX_COUNT (-1)

/// TS.BGET fixed argv layout: TS.BGET key timestamp timeout [MIN_COUNT n] [MAX_COUNT n]
typedef enum BGetArgv
{
    BGET_ARGV_COMMAND = 0,     ///< the command name itself
    BGET_ARGV_KEY = 1,         ///< series key
    BGET_ARGV_TIMESTAMP = 2,   ///< cursor / '-' / '+' / '$'
    BGET_ARGV_TIMEOUT = 3,     ///< timeout in ms
    BGET_ARGV_FIRST_OPTION = 4 ///< first MIN_COUNT/MAX_COUNT keyword (options follow in pairs)
} BGetArgv;

#define BGET_OPTION_STRIDE 2 ///< each option is a keyword + value pair
/// timeout_ms value meaning "do not block": reply with whatever is available now.
#define BGET_NO_BLOCK_TIMEOUT 0
/// Min argc: just the fixed args (command key timestamp timeout), no options.
#define BGET_MIN_ARGC BGET_ARGV_FIRST_OPTION
/// Max argc: the fixed args plus both MIN_COUNT and MAX_COUNT pairs.
#define BGET_MAX_ARGC (BGET_ARGV_FIRST_OPTION + 2 * BGET_OPTION_STRIDE)

/**
 * @brief Parsed TS.BGET arguments, also used as blocked-client privdata.
 *
 * Populated once by parse_bget_args() so callbacks never re-parse argv.
 * `key` is borrowed at parse time; TSDB_bget_block() retains it and
 * TSDB_bget_free_privdata() releases it.
 */
typedef struct BGetCtx
{
    RedisModuleString *key; ///< series key to read from
    api_timestamp_t cursor; ///< inclusive lower bound: only samples with ts >= cursor qualify
    size_t min_count;       ///< unblock threshold: wait until this many samples qualify (default 1)
    long long max_count;    ///< reply cap: max samples to return; -1 = unlimited (default)
    long long timeout_ms;   ///< 0 = do not block; >0 = max time to wait
} BGetCtx;

/**
 * @brief Parse TS.BGET argv into @p out, including sentinel resolution.
 *
 * On error, replies to the client with a descriptive error.
 * @p out->key is borrowed from @p argv (not retained).
 *
 * Timestamp resolution rules (argv[2]):
 *   - literal non-negative integer : @p out->cursor = the integer.
 *   - '-'                          : @p out->cursor = 0 (no lower bound).
 *   - '+'                          : open the series ONCE and snapshot
 *                                    @p out->cursor = lastTimestamp + 1;
 *                                    saturated (lastTimestamp == UINT64_MAX)
 *                                    -> UINT64_MAX to avoid wrap-to-0;
 *                                    missing or empty series -> 0; wrong-type
 *                                    key -> reply WRONGTYPE and return ERR.
 *
 * After OK, @p out->cursor is a plain literal for the rest of the lifecycle.
 *
 * Syntax: TS.BGET key timestamp timeout [MIN_COUNT min_count] [MAX_COUNT max_count]
 *   - argv[3] (timeout): non-negative integer milliseconds; 0 means "do not block".
 *   - MIN_COUNT (optional): unblock threshold, positive integer. Default 1.
 *   - MAX_COUNT (optional): reply cap, positive integer. Default unlimited (-1).
 *   - Requires min_count <= max_count.
 *
 * @param ctx   Redis module context (used to reply on error / WRONGTYPE).
 * @param argv  Command argument vector.
 * @param argc  Number of arguments in @p argv (4 to 8).
 * @param out   Output struct populated on success.
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on any reply written.
 */
static int parse_bget_args(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, BGetCtx *out) {
    // Optional args come only as MIN_COUNT/MAX_COUNT keyword+value pairs, so
    // anything past the fixed args must be a whole number of BGET_OPTION_STRIDE
    // tokens. This rejects a stray trailing token or a lone keyword (e.g. an
    // odd argc of 5 or 7) instead of silently ignoring it.
    if (argc < BGET_MIN_ARGC || argc > BGET_MAX_ARGC ||
        (argc - BGET_MIN_ARGC) % BGET_OPTION_STRIDE != 0) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_ERR;
    }

    // timeout_ms: non-negative integer. BGET_NO_BLOCK_TIMEOUT means "do not block".
    long long timeout_ms = BGET_NO_BLOCK_TIMEOUT;
    if (RedisModule_StringToLongLong(argv[BGET_ARGV_TIMEOUT], &timeout_ms) != REDISMODULE_OK ||
        timeout_ms < 0) {
        RedisModule_ReplyWithError(ctx, "TSDB: timeout must be a non-negative integer");
        return REDISMODULE_ERR;
    }

    // Optional MIN_COUNT / MAX_COUNT, parsed like every other TS optional
    // keyword argument: locate the token with RMUtil_ArgIndex and read its
    // value with RMUtil_ParseArgsAfter.
    long long min_count = BGET_DEFAULT_MIN_COUNT;
    long long max_count = BGET_DEFAULT_MAX_COUNT;

    if (RMUtil_ArgIndex("MIN_COUNT", argv, argc) > 0) {
        if (RMUtil_ParseArgsAfter("MIN_COUNT", argv, argc, "l", &min_count) != REDISMODULE_OK ||
            min_count <= 0) {
            RedisModule_ReplyWithError(ctx, "TSDB: MIN_COUNT must be a positive integer");
            return REDISMODULE_ERR;
        }
    }

    if (RMUtil_ArgIndex("MAX_COUNT", argv, argc) > 0) {
        if (RMUtil_ParseArgsAfter("MAX_COUNT", argv, argc, "l", &max_count) != REDISMODULE_OK ||
            max_count <= 0) {
            RedisModule_ReplyWithError(ctx, "TSDB: MAX_COUNT must be a positive integer");
            return REDISMODULE_ERR;
        }
    }

    // Unlimited max_count is >= any min_count, so only validate when capped.
    if (max_count != BGET_DEFAULT_MAX_COUNT && min_count > max_count) {
        RedisModule_ReplyWithError(ctx, "TSDB: MIN_COUNT must be <= MAX_COUNT");
        return REDISMODULE_ERR;
    }

    // timestamp: literal non-negative integer, or a '-' / '+' / '$' sentinel.
    // '-' is a static substitution: cursor=0 matches every sample under our
    // inclusive ts >= cursor semantic.
    // '+' denotes the latest sample's timestamp (0 when the series is empty),
    // aligned with TS.RANGE; the latest existing sample qualifies.
    // '$' denotes the latest sample's timestamp + 1 (0 when the series is empty):
    // only samples reported after this command qualify (the latest existing
    // sample is excluded).
    // '+' / '$' must be resolved against the live series exactly once (here) so
    // the cursor stays stable across wake-ups; if we re-resolved on every signal,
    // lastTs would chase forward and we'd never reply.
    api_timestamp_t cursor = 0;
    size_t ts_len = 0;
    const char *ts_str = RedisModule_StringPtrLen(argv[BGET_ARGV_TIMESTAMP], &ts_len);
    if (ts_len == 1 && ts_str[0] == '-') {
        cursor = 0;
    } else if (ts_len == 1 && (ts_str[0] == '+' || ts_str[0] == '$')) {
        const bool future_only = (ts_str[0] == '$');
        RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[BGET_ARGV_KEY], REDISMODULE_READ);
        const int kt = key ? RedisModule_KeyType(key) : REDISMODULE_KEYTYPE_EMPTY;

        if (kt == REDISMODULE_KEYTYPE_EMPTY) {
            // Missing key: spec says cursor = 0 when empty.
            cursor = 0;
        } else if (RedisModule_ModuleTypeGetType(key) != SeriesType) {
            RedisModule_CloseKey(key);
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
            return REDISMODULE_ERR;
        } else {
            Series *series = RedisModule_ModuleTypeGetValue(key);
            if (SeriesGetNumSamples(series) == 0) {
                cursor = 0;
            } else if (!future_only) {
                cursor = (api_timestamp_t)series->lastTimestamp;
            } else {
                // '$': saturate at UINT64_MAX so `lastTimestamp + 1` cannot wrap to
                // 0 (which would silently flip the cursor's meaning from "future
                // only" to "every sample"). No timestamp can exceed UINT64_MAX, so
                // cursor=UINT64_MAX is the correct unreachable lower bound.
                cursor = (series->lastTimestamp == UINT64_MAX)
                             ? UINT64_MAX
                             : (api_timestamp_t)(series->lastTimestamp + 1);
            }
        }
        if (key) {
            RedisModule_CloseKey(key);
        }
    } else {
        long long ts_ll = 0;
        if (RedisModule_StringToLongLong(argv[BGET_ARGV_TIMESTAMP], &ts_ll) != REDISMODULE_OK ||
            ts_ll < 0) {
            RedisModule_ReplyWithError(ctx, "TSDB: invalid timestamp");
            return REDISMODULE_ERR;
        }
        cursor = (api_timestamp_t)ts_ll;
    }

    out->key = argv[BGET_ARGV_KEY];
    out->cursor = cursor;
    out->min_count = (size_t)min_count;
    out->max_count = max_count;
    out->timeout_ms = timeout_ms;
    return REDISMODULE_OK;
}

/**
 * @brief Count samples matching @p range, stopping once @p threshold is hit.
 *
 * Cost is bounded by @p threshold (chunk-metadata reads only, no per-sample
 * work). Used to gate strict-mode "do we have >= count?" before any
 * irreversible ReplyWith* call.
 *
 * @param series     Time series to scan.
 * @param range      Range query (typically `[cursor, +inf] LIMIT count`).
 * @param threshold  Early-exit threshold.
 * @return Number of qualifying samples, clamped to @p threshold.
 */
static size_t TSDB_count_samples_up_to(Series *series, const RangeArgs *range, size_t threshold) {
    AbstractIterator *iter =
        SeriesQuery(series, range, /*reverse=*/false, /*check_retention=*/true);
    EnrichedChunk *chunk;
    size_t total = 0;
    while (total < threshold && (chunk = iter->GetNext(iter))) {
        total += chunk->samples.num_samples;
    }
    iter->Close(iter);
    return total;
}

/**
 * @brief Try to satisfy a TS.BGET request from current series state.
 *
 * Single source of truth for "what should we reply with right now?", shared
 * by the fast path, the wake-up callback, and the timeout callback. In
 * strict mode we count qualifying samples first (TSDB_count_samples_up_to)
 * and only start writing once we know the full batch is available — purely
 * a performance optimization to avoid materializing a partial reply that
 * the reply_cb would then discard by returning REDISMODULE_ERR.
 *
 * Modes:
 *   - strict (@p require_full_batch == true): only reply when at least
 *     @p args->min_count samples qualify; otherwise return false so the caller
 *     can block / stay parked. Used on the fast path and on SignalKeyAsReady
 *     wake-ups.
 *   - flush  (@p require_full_batch == false): always reply (possibly empty
 *     or partial). Used by the timeout callback and by the no-block fast
 *     path when `timeout_ms == 0`.
 *
 * In all reply cases at most @p args->max_count samples are returned.
 *
 * Case breakdown:
 *   1. Missing key      : strict -> false; flush -> empty array.
 *   2. Wrong-type key   : always WRONGTYPE error.
 *   3a. lastTs <  cursor: strict -> false; flush -> empty array.
 *   3b. Else            : strict -> false if qualifying < min_count, else reply;
 *                         flush  -> reply (0..max_count samples).
 *
 * @param ctx                Module context for OpenKey/Reply* calls.
 * @param args               Parsed BGET arguments (key, cursor, min/max count).
 * @param require_full_batch Strict (true) vs flush (false) mode.
 * @return true if a reply was written, false only in strict mode when caller
 *         should block / stay parked.
 */
static bool TSDB_bget_try_reply(RedisModuleCtx *ctx, const BGetCtx *args, bool require_full_batch) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, args->key, REDISMODULE_READ);
    const int keyType = key ? RedisModule_KeyType(key) : REDISMODULE_KEYTYPE_EMPTY;

    // Case 1: missing key.
    if (keyType == REDISMODULE_KEYTYPE_EMPTY) {
        if (key) {
            RedisModule_CloseKey(key);
        }
        if (require_full_batch) {
            return false;
        }
        RedisModule_ReplyWithArray(ctx, 0);
        return true;
    }

    // Case 2: wrong type — always fail-fast.
    if (RedisModule_ModuleTypeGetType(key) != SeriesType) {
        RedisModule_CloseKey(key);
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return true;
    }

    Series *series = RedisModule_ModuleTypeGetValue(key);

    // Case 3a: O(1) shortcut. If the latest sample is older than the cursor,
    // no qualifying sample can exist (ts >= cursor is impossible when
    // lastTimestamp < cursor).
    if (series->lastTimestamp < args->cursor) {
        RedisModule_CloseKey(key);
        if (require_full_batch) {
            return false;
        }
        RedisModule_ReplyWithArray(ctx, 0);
        return true;
    }

    // Case 3b: at least one sample may qualify. Build the range query
    // equivalent to TS.RANGE [cursor, +inf] LIMIT max_count (-1 = unlimited).
    const RangeArgs range = {
        .startTimestamp = args->cursor,
        .endTimestamp = LLONG_MAX,
        .latest = false,
        .count = args->max_count,
    };

    // Strict mode: refuse to commit a reply until at least min_count samples
    // qualify. The pre-count walk reads only chunk metadata.
    if (require_full_batch &&
        TSDB_count_samples_up_to(series, &range, args->min_count) < args->min_count) {
        RedisModule_CloseKey(key);
        return false;
    }

    // Either we have >= min_count (strict) or partial flushing is allowed.
    // Either way, deliver up to max_count samples in ascending order.
    ReplySeriesRange(ctx, series, &range, /*reverse=*/false);
    RedisModule_CloseKey(key);
    return true;
}

/**
 * @brief Close the blocked-time stopwatch opened by RTS_BlockClientOnKey.
 *
 * Must be called from a BlockClientOnKeys callback (reply / timeout) on the
 * unblock path — i.e. right before returning REDISMODULE_OK. Accumulates the
 * elapsed wait into bc->background_duration so slowlog / commandstats /
 * latency-history account for blocked TS.BGET time instead of dropping it.
 *
 * Safe no-op when the Redis build doesn't expose the MeasureTime APIs.
 *
 * @param ctx Module context bound to the blocked client.
 */
static void TSDB_bget_account_blocked_time(RedisModuleCtx *ctx) {
    if (CheckVersionForBlockedClientMeasureTime()) {
        RedisModule_BlockedClientMeasureTimeEnd(RedisModule_GetBlockedClientHandle(ctx));
    }
}

/**
 * @brief BlockClientOnKeys reply callback: strict-mode wake-up handler.
 *
 * Re-invoked on every RedisModule_SignalKeyAsReady() on the watched key
 * (and once at block setup).
 *
 * @param ctx   Module context bound to the blocked client.
 * @param argv  Unused (command argv is not forwarded to wake-up callbacks).
 * @param argc  Unused.
 * @return REDISMODULE_OK to unblock and commit the reply, REDISMODULE_ERR to
 *         stay parked until the next signal or the timeout.
 */
static int TSDB_bget_reply_callback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    const BGetCtx *priv = RedisModule_GetBlockedClientPrivateData(ctx);
    // If the key was deleted (DEL/FLUSHALL/expire/eviction) while we were
    // parked, the SignalKeyAsReady in FreeSeries wakes us here. Detect the
    // missing-key case and reply empty instead of re-parking, otherwise the
    // client would stay blocked until its user-supplied timeout.
    RedisModuleKey *probe = RedisModule_OpenKey(ctx, priv->key, REDISMODULE_READ);
    const bool key_gone = (RedisModule_KeyType(probe) == REDISMODULE_KEYTYPE_EMPTY);
    if (probe) {
        RedisModule_CloseKey(probe);
    }
    if (!TSDB_bget_try_reply(ctx, priv, /*require_full_batch=*/!key_gone)) {
        // Stay parked: keep the background timer running so the eventual
        // unblock (next signal or timeout) accounts for the full wait.
        return REDISMODULE_ERR;
    }
    TSDB_bget_account_blocked_time(ctx);
    return REDISMODULE_OK;
}

/**
 * @brief BlockClientOnKeys timeout callback: flush-mode deadline handler.
 *
 * Invoked when `timeout_ms` elapses without the client being unblocked.
 * Flushes whatever is available (possibly empty or fewer than count) per
 * the TS.BGET spec.
 *
 * @param ctx   Module context bound to the blocked client.
 * @param argv  Unused.
 * @param argc  Unused.
 * @return Always REDISMODULE_OK (the client is unblocked unconditionally).
 */
static int TSDB_bget_timeout_callback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    const BGetCtx *priv = RedisModule_GetBlockedClientPrivateData(ctx);
    (void)TSDB_bget_try_reply(ctx, priv, /*require_full_batch=*/false);
    TSDB_bget_account_blocked_time(ctx);
    return REDISMODULE_OK;
}

/**
 * @brief Release the BGetCtx attached to the blocked client.
 *
 * Called by Redis once the client is unblocked (via reply, timeout,
 * disconnect, or abort). Frees the retained key and the heap struct.
 *
 * @param ctx       Module context (used for RedisModule_FreeString).
 * @param privdata  BGetCtx* allocated by TSDB_bget_block(); NULL-safe.
 */
static void TSDB_bget_free_privdata(RedisModuleCtx *ctx, void *privdata) {
    if (!privdata) {
        return;
    }
    BGetCtx *priv = privdata;
    if (priv->key) {
        RedisModule_FreeString(ctx, priv->key);
    }
    free(priv);
}

/**
 * @brief Allocate privdata and park the client on @p args->key.
 *
 * Heap-copies @p args, retains the key for the privdata's lifetime, and
 * registers the BGET reply / timeout / free callbacks. Redis owns the
 * blocked-client lifecycle thereafter.
 *
 * If Redis refuses to block (MULTI / Lua / deny-blocking context),
 * BlockClientOnKeys returns NULL and the free callback is never installed —
 * in that case we free `priv` (and the retained `priv->key`) here to avoid
 * a leak.
 *
 * @param ctx   Module context bound to the calling client.
 * @param args  Parsed BGET arguments to capture into privdata.
 * @return Handle to the blocked client, or NULL if blocking was refused.
 */
static RedisModuleBlockedClient *TSDB_bget_block(RedisModuleCtx *ctx, const BGetCtx *args) {
    BGetCtx *priv = malloc(sizeof(*priv));
    *priv = *args;
    RedisModule_RetainString(ctx, priv->key);

    RedisModuleBlockedClient *bc = RTS_BlockClientOnKey(ctx,
                                                        TSDB_bget_reply_callback,
                                                        TSDB_bget_timeout_callback,
                                                        TSDB_bget_free_privdata,
                                                        priv->timeout_ms,
                                                        priv->key,
                                                        priv);
    if (!bc) {
        TSDB_bget_free_privdata(ctx, priv);
    }
    return bc;
}

/**
 * @brief TS.BGET command entry point.
 *
 * Parses argv into a BGetCtx. If samples at-or-after the cursor already exist
 * (or the key is unusable / `timeout_ms == 0`), replies inline via
 * TSDB_bget_try_reply(); otherwise parks the client via TSDB_bget_block().
 *
 * Blocking refusal in MULTI / EVAL / deny-blocking contexts is handled
 * reactively: BlockClientOnKeys returns NULL, TSDB_bget_block cleans up the
 * privdata, and we deliver an explanatory error here.
 *
 * @param ctx   Module context.
 * @param argv  Command argument vector:
 *              `TS.BGET key timestamp timeout [MIN_COUNT min_count] [MAX_COUNT max_count]`.
 * @param argc  Argument count (4 to 8).
 * @return Always REDISMODULE_OK — errors (parse, WRONGTYPE, blocking refused)
 *         are delivered through RedisModule_ReplyWithError so the client
 *         always sees a terminal reply.
 */
int TSDB_bget(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    BGetCtx args = { 0 };
    if (parse_bget_args(ctx, argv, argc, &args) != REDISMODULE_OK) {
        // parse_bget_args already wrote a reply (validation error, WRONGTYPE
        // during '+' resolution, etc.).
        return REDISMODULE_OK;
    }

    // BGET_NO_BLOCK_TIMEOUT means "do not block" — flush whatever is available
    // now (possibly empty / partial). A positive timeout uses strict mode; if
    // we don't have min_count yet, fall through and park the client until a
    // SignalKeyAsReady wakes us with enough data, or the timeout fires.
    if (TSDB_bget_try_reply(ctx, &args, args.timeout_ms > BGET_NO_BLOCK_TIMEOUT)) {
        return REDISMODULE_OK;
    }

    // Preemptive refusal in MULTI / EVAL / deny-blocking contexts, mirroring
    // TSDB_mget. A timeout_ms == 0 request already resolved synchronously
    // above, so reaching here implies the caller asked to block.
    if (RedisModule_GetContextFlags(ctx) &
        (REDISMODULE_CTX_FLAGS_LUA | REDISMODULE_CTX_FLAGS_MULTI |
         REDISMODULE_CTX_FLAGS_DENY_BLOCKING)) {
        RedisModule_ReplyWithError(ctx,
                                   "TSDB: blocking TS.BGET (timeout > 0) is not allowed "
                                   "inside MULTI, EVAL, or a deny-blocking context");
        return REDISMODULE_OK;
    }

    if (!TSDB_bget_block(ctx, &args)) {
        // Defensive fallback: BlockClientOnKeys refused to park us even
        // though the preemptive context check passed (e.g. a Redis version
        // surfacing a new deny-blocking flag). Reply with an error so the
        // client doesn't hang on an empty pipeline.
        RedisModule_ReplyWithError(ctx,
                                   "TSDB: blocking TS.BGET (timeout > 0) is not allowed "
                                   "inside MULTI, EVAL, or a deny-blocking context");
    }
    return REDISMODULE_OK;
}

int TSDB_mget(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (IsMRCluster()) {
        int ctxFlags = RedisModule_GetContextFlags(ctx);

        if (ctxFlags & (REDISMODULE_CTX_FLAGS_LUA | REDISMODULE_CTX_FLAGS_MULTI |
                        REDISMODULE_CTX_FLAGS_DENY_BLOCKING)) {
            RedisModule_ReplyWithError(ctx,
                                       "Can not run multi sharded command inside a multi exec, "
                                       "lua, or when blocking is not allowed");
            return REDISMODULE_OK;
        }
        return TSDB_mget_MR(ctx, argv, argc);
    }

    MGetArgs args;
    if (parseMGetCommand(ctx, argv, argc, &args) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    const char **limitLabelsStr = calloc(args.numLimitLabels, sizeof(char *));
    for (int i = 0; i < args.numLimitLabels; i++) {
        limitLabelsStr[i] = RedisModule_StringPtrLen(args.limitLabels[i], NULL);
    }

    bool hasPermissionError = false;
    RedisModuleDict *result = QueryIndex(
        ctx, args.queryPredicates->list, args.queryPredicates->count, &hasPermissionError);

    if (hasPermissionError) {
        free(limitLabelsStr);
        MGetArgs_Free(&args);
        RedisModule_FreeDict(ctx, result);
        RTS_ReplyKeyPermissionsError(ctx);
        return REDISMODULE_ERR;
    }

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;
    long long replylen = 0;
    Series *series;
    int exitStatus = REDISMODULE_OK;
    const GetSeriesFlags checkFlags = GetSeriesFlags_SilentOperation | GetSeriesFlags_CheckForAcls;

    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModuleKey *key;
        const GetSeriesResult status =
            GetSeries(ctx,
                      RedisModule_CreateString(ctx, currentKey, currentKeyLen),
                      &key,
                      &series,
                      REDISMODULE_READ,
                      checkFlags);

        switch (status) {
            case GetSeriesResult_Success:
                RedisModule_CloseKey(key);
                break;
            case GetSeriesResult_GenericError:
                RedisModule_Log(ctx, "warning", "couldn't open key or key is not a Timeseries.");
                break;
            case GetSeriesResult_PermissionError:
                RedisModule_Log(ctx,
                                "warning",
                                "The user lacks the required permissions for the key, stopping.");

                RTS_ReplyKeyPermissionsError(ctx);

                exitStatus = REDISMODULE_ERR;
                goto exit;
        }
    }

    ReplyWithMapOrArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN, false);
    RedisModule_DictIteratorStop(iter);
    iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModuleKey *key;

        const GetSeriesResult status =
            GetSeries(ctx,
                      RedisModule_CreateString(ctx, currentKey, currentKeyLen),
                      &key,
                      &series,
                      REDISMODULE_READ,
                      GetSeriesFlags_SilentOperation);

        if (status != GetSeriesResult_Success) {
            continue;
        }

        if (!_ReplyMap(ctx)) {
            RedisModule_ReplyWithArray(ctx, 3);
        }
        RedisModule_ReplyWithStringBuffer(ctx, currentKey, currentKeyLen);
        if (_ReplyMap(ctx)) {
            RedisModule_ReplyWithArray(ctx, 2);
        }
        if (args.withLabels) {
            ReplyWithSeriesLabels(ctx, series);
        } else if (args.numLimitLabels > 0) {
            ReplyWithSeriesLabelsWithLimitC(ctx, series, limitLabelsStr, args.numLimitLabels);
        } else {
            ReplyWithMapOrArray(ctx, 0, false);
        }
        // LATEST is ignored for a series that is not a compaction.
        bool should_finalize_last_bucket = should_finalize_last_bucket_get(args.latest, series);
        if (should_finalize_last_bucket) {
            Sample sample;
            Sample *sample_ptr = &sample;
            calculate_latest_sample(&sample_ptr, series);
            if (sample_ptr) {
                ReplyWithSample(ctx, sample.timestamp, sample.value);
            } else {
                ReplyWithSeriesLastDatapoint(ctx, series);
            }
        } else {
            ReplyWithSeriesLastDatapoint(ctx, series);
        }
        replylen++;
        RedisModule_CloseKey(key);
    }

exit:
    if (exitStatus == REDISMODULE_OK) {
        ReplySetMapOrArrayLength(ctx, replylen, false);
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_FreeDict(ctx, result);
    MGetArgs_Free(&args);
    free(limitLabelsStr);
    return exitStatus;
}

static inline bool is_obsolete(timestamp_t ts,
                               timestamp_t lastTimestamp,
                               timestamp_t retentionTime) {
    return (lastTimestamp > retentionTime) && (ts < lastTimestamp - retentionTime);
}

static inline bool verify_compaction_del_possible(RedisModuleCtx *ctx,
                                                  const Series *series,
                                                  const RangeArgs *args) {
    bool is_valid = true;
    if (!series->rules || !series->retentionTime)
        return true;

    // Verify startTimestamp in retention period
    if (is_obsolete(args->startTimestamp, series->lastTimestamp, series->retentionTime)) {
        is_valid = false;
    }

    // Verify all compaction's buckets are in the retention period
    CompactionRule *rule = series->rules;
    while (rule != NULL) {
        const timestamp_t ruleTimebucket = rule->bucketDuration;
        const timestamp_t curAggWindowStart = BucketStartNormalize(
            CalcBucketStart(args->startTimestamp, ruleTimebucket, rule->timestampAlignment));
        if (is_obsolete(curAggWindowStart, series->lastTimestamp, series->retentionTime)) {
            is_valid = false;
        }
        rule = rule->nextRule;
    }

    if (unlikely(!is_valid)) {
        RTS_ReplyGeneralError(ctx,
                              "TSDB: When a series has compactions, deleting samples or compaction "
                              "buckets beyond the series retention period is not possible");
    }

    return is_valid;
}

int TSDB_delete(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    RangeArgs args = { 0 };
    if (parseRangeArguments(ctx, 2, argv, argc, &args) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    Series *series;
    RedisModuleKey *key;
    const GetSeriesResult status = GetSeries(
        ctx, argv[1], &key, &series, REDISMODULE_READ | REDISMODULE_WRITE, GetSeriesFlags_None);
    if (status != GetSeriesResult_Success) {
        return REDISMODULE_ERR;
    }

    if (unlikely(!verify_compaction_del_possible(ctx, series, &args))) {
        RedisModule_CloseKey(key);
        return REDISMODULE_ERR;
    }

    size_t deleted = SeriesDelRange(series, args.startTimestamp, args.endTimestamp);

    RedisModule_ReplyWithLongLong(ctx, deleted);
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_MODULE, "ts.del", argv[1]);

    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

void FlushEventCallback(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
    if ((!memcmp(&eid, &RedisModuleEvent_FlushDB, sizeof(eid))) &&
        subevent == REDISMODULE_SUBEVENT_FLUSHDB_END) {
        RemoveAllIndexedMetrics();
    }
}

void swapDbEventCallback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data) {
    RedisModule_Log(ctx, "warning", "swapdb isn't supported by redis timeseries");
    if ((!memcmp(&e, &RedisModuleEvent_FlushDB, sizeof(e)))) {
        RedisModuleSwapDbInfo *ei = data;
        REDISMODULE_NOT_USED(ei);
    }
}

void keyAddedToDbDict(RedisModuleCtx *ctx,
                      RedisModuleString *key,
                      void *value,
                      int swap_key_metadata) {
    Series *series = (Series *)value;
    series->in_ram = true;
}

int keyRemovedFromDbDict(RedisModuleCtx *ctx,
                         RedisModuleString *key,
                         void *value,
                         int swap_key_metadata,
                         int writing_to_swap) {
    Series *series = (Series *)value;
    if (!!writing_to_swap) {
        series->in_ram = false;
    }
    return 0;
}

int persistence_in_progress = 0;

void persistCallback(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
    if (memcmp(&eid, &RedisModuleEvent_Persistence, sizeof(eid)) != 0) {
        return;
    }

    if (subevent == REDISMODULE_SUBEVENT_PERSISTENCE_RDB_START ||
        subevent == REDISMODULE_SUBEVENT_PERSISTENCE_AOF_START ||
        subevent == REDISMODULE_SUBEVENT_PERSISTENCE_SYNC_RDB_START ||
        subevent == REDISMODULE_SUBEVENT_PERSISTENCE_SYNC_AOF_START) {
        persistence_in_progress++;
    } else if (subevent == REDISMODULE_SUBEVENT_PERSISTENCE_ENDED ||
               subevent == REDISMODULE_SUBEVENT_PERSISTENCE_FAILED) {
        persistence_in_progress--;
    }

    return;
}

void ShardingEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
    /**
     * On sharding event we need to do couple of things depends on the subevent given:
     *
     * 1. REDISMODULE_SUBEVENT_SHARDING_SLOT_RANGE_CHANGED
     *    On this event we know that the slot range changed and we might have data
     *    which are no longer belong to this shard, we must ignore it on searches
     *
     * 2. REDISMODULE_SUBEVENT_SHARDING_TRIMMING_STARTED
     *    This event tells us that the trimming process has started and keys will start to be
     *    deleted, we do not need to do anything on this event
     *
     * 3. REDISMODULE_SUBEVENT_SHARDING_TRIMMING_ENDED
     *    This event tells us that the trimming process has finished, we are not longer
     *    have data that are not belong to us and its safe to stop checking this on searches.
     */
    if (eid.id != REDISMODULE_EVENT_SHARDING) {
        RedisModule_Log(rts_staticCtx, "warning", "Bad event given, ignored.");
        return;
    }

    switch (subevent) {
        case REDISMODULE_SUBEVENT_SHARDING_SLOT_RANGE_CHANGED:
            RedisModule_Log(
                ctx, "notice", "%s", "Got slot range change event, enter trimming phase.");
            isReshardTrimming = true;
            break;
        case REDISMODULE_SUBEVENT_SHARDING_TRIMMING_STARTED:
            RedisModule_Log(
                ctx, "notice", "%s", "Got trimming started event, enter trimming phase.");
            isReshardTrimming = true;
            break;
        case REDISMODULE_SUBEVENT_SHARDING_TRIMMING_ENDED:
            RedisModule_Log(ctx, "notice", "%s", "Got trimming ended event, exit trimming phase.");
            isReshardTrimming = false;
            break;
        default:
            RedisModule_Log(rts_staticCtx, "warning", "Bad subevent given, ignored.");
    }
}

void ClusterAsmCallback(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
    if (eid.id != REDISMODULE_EVENT_CLUSTER_SLOT_MIGRATION) {
        RedisModule_Log(
            rts_staticCtx, "warning", "Bad event given (id=%" PRIu64 "), ignored.", eid.id);
        return;
    }

    switch (subevent) {
        case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_IMPORT_STARTED:
            RedisModule_Log(ctx,
                            "notice",
                            "Cluster ASM import started (subevent=%" PRIu64 ") received.",
                            subevent);
            isAsmImporting = true;
            break;
        case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_IMPORT_FAILED:
            RedisModule_Log(ctx,
                            "notice",
                            "Cluster ASM import failed (subevent=%" PRIu64 ") received.",
                            subevent);
            isAsmImporting = false;
            break;
        case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_IMPORT_COMPLETED:
            RedisModule_Log(ctx,
                            "notice",
                            "Cluster ASM import completed (subevent=%" PRIu64 ") received.",
                            subevent);
            isAsmImporting = false;
            break;
        case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_MIGRATE_STARTED:
            RedisModule_Log(ctx,
                            "notice",
                            "Cluster ASM migrate started (subevent=%" PRIu64 ") received.",
                            subevent);
            break;
        case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_MIGRATE_FAILED:
            RedisModule_Log(ctx,
                            "notice",
                            "Cluster ASM migrate failed (subevent=%" PRIu64 ") received.",
                            subevent);
            break;
        case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_MIGRATE_COMPLETED:
            RedisModule_Log(ctx,
                            "notice",
                            "Cluster ASM migrate completed (subevent=%" PRIu64 ") received.",
                            subevent);
            break;
        case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_MIGRATE_MODULE_PROPAGATE:
            RedisModule_Log(ctx,
                            "notice",
                            "Cluster ASM module propagate (subevent=%" PRIu64 ") received.",
                            subevent);
            break;
        default:
            RedisModule_Log(rts_staticCtx,
                            "warning",
                            "Bad subevent (%" PRIu64 ") received, ignored.",
                            subevent);
    }
}

void ClusterAsmTrimCallback(RedisModuleCtx *ctx,
                            RedisModuleEvent eid,
                            uint64_t subevent,
                            void *data) {
    if (eid.id != REDISMODULE_EVENT_CLUSTER_SLOT_MIGRATION_TRIM) {
        RedisModule_Log(
            rts_staticCtx, "warning", "Bad event given (id=%" PRIu64 "), ignored.", eid.id);
        return;
    }

    switch (subevent) {
        case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_TRIM_STARTED:
            RedisModule_Log(ctx,
                            "notice",
                            "Cluster ASM trim started (subevent=%" PRIu64 ") received.",
                            subevent);
            isAsmTrimming = true;
            break;
        case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_TRIM_COMPLETED:
            RedisModule_Log(ctx,
                            "notice",
                            "Cluster ASM trim completed (subevent=%" PRIu64 ") received.",
                            subevent);
            isAsmTrimming = false;
            break;
        // Since we subscribed to keyspace event REDISMODULE_NOTIFY_KEY_TRIMMED
        // an active trimming will be used so no need to handle the
        // REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_TRIM_BACKGROUND case.
        default:
            RedisModule_Log(rts_staticCtx,
                            "warning",
                            "Bad subevent (%" PRIu64 ") received, ignored.",
                            subevent);
    }
}

void ReplicaBackupCallback(RedisModuleCtx *ctx,
                           RedisModuleEvent eid,
                           uint64_t subevent,
                           void *data) {
    REDISMODULE_NOT_USED(eid);
    switch (subevent) {
        case REDISMODULE_SUBEVENT_REPL_BACKUP_CREATE:
            Backup_Globals();
            break;
        case REDISMODULE_SUBEVENT_REPL_BACKUP_RESTORE:
            Restore_Globals();
            break;
        case REDISMODULE_SUBEVENT_REPL_BACKUP_DISCARD:
            Discard_Globals_Backup();
            break;
    }
}

bool CheckVersionForBlockedClientMeasureTime() {
    // Minimal versions: 6.2.0
    if (RTS_currVersion.redisMajorVersion > 6)
        return true;
    if (RTS_currVersion.redisMajorVersion == 6 && RTS_currVersion.redisMinorVersion >= 2)
        return true;
    return false;
}

int CheckVersionForShortRead() {
    // Minimal versions: 6.2.5
    // (6.0.15 is not supporting the required event notification for modules)
    if (RTS_currVersion.redisMajorVersion > 6)
        return REDISMODULE_OK;
    if (RTS_currVersion.redisMajorVersion == 6 && RTS_currVersion.redisMinorVersion > 2)
        return REDISMODULE_OK;
    if (RTS_currVersion.redisMajorVersion == 6 && RTS_currVersion.redisMinorVersion == 2 &&
        RTS_currVersion.redisPatchVersion >= 5)
        return REDISMODULE_OK;
    return REDISMODULE_ERR;
}

void Initialize_RdbNotifications(RedisModuleCtx *ctx) {
    if (CheckVersionForShortRead() == REDISMODULE_OK) {
        int success = RedisModule_SubscribeToServerEvent(
            ctx, RedisModuleEvent_ReplBackup, ReplicaBackupCallback);
        RedisModule_Assert(success !=
                           REDISMODULE_ERR); // should be supported in this redis version/release
        RedisModule_SetModuleOptions(ctx, REDISMODULE_OPTIONS_HANDLE_IO_ERRORS);
        RedisModule_Log(ctx, "notice", "Enabled diskless replication");
    }
}

__attribute__((weak)) int (*RedisModule_SetDataTypeExtensions)(
    RedisModuleCtx *ctx,
    RedisModuleType *mt,
    RedisModuleTypeExtMethods *typemethods) REDISMODULE_ATTR = NULL;

int RedisModule_OnUnload(RedisModuleCtx *ctx) {
    if (rts_staticCtx) {
        FreeConfigAndStaticCtx();
    }

    return REDISMODULE_OK;
}

// Some of the defrag funcionality might be missing in different versions of redis (e.g., redis 8 +
// RoF). To avoid calling unimplemented functions we prepare do-nothing stubs for the defrag
// registration functions and pointthe missing function pointers to them.
static int Stub_RegisterDefragFunc(RedisModuleCtx *ctx, RedisModuleDefragFunc func) {
    return REDISMODULE_OK;
}
static int Stub_RegisterDefragFunc2(RedisModuleCtx *ctx, RedisModuleDefragFunc2 func) {
    return REDISMODULE_OK;
}
static int Stub_RegisterDefragCallbacks(RedisModuleCtx *ctx,
                                        RedisModuleDefragFunc start,
                                        RedisModuleDefragFunc end) {
    return REDISMODULE_OK;
}
/*
module loading function, possible arguments:
COMPACTION_POLICY - compaction policy from parse_policies.h
RETENTION_POLICY - long that represents the retention in milliseconds
MAX_SAMPLE_PER_CHUNK - how many samples per chunk
example:
redis-server --loadmodule ./redistimeseries.so COMPACTION_POLICY
"max:1m:1d;min:10s:1h;avg:2h:10d;avg:3d:100d" RETENTION_POLICY 3600 MAX_SAMPLE_PER_CHUNK 1024
*/
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "timeseries", REDISTIMESERIES_MODULE_VERSION, REDISMODULE_APIVER_1) ==
        REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_RegisterDefragFunc == NULL)
        RedisModule_RegisterDefragFunc = Stub_RegisterDefragFunc;
    if (RedisModule_RegisterDefragFunc2 == NULL)
        RedisModule_RegisterDefragFunc2 = Stub_RegisterDefragFunc2;
    if (RedisModule_RegisterDefragCallbacks == NULL)
        RedisModule_RegisterDefragCallbacks = Stub_RegisterDefragCallbacks;

    rts_staticCtx = RedisModule_GetDetachedThreadSafeContext(ctx);

    RedisModule_Log(ctx,
                    "notice",
                    "RedisTimeSeries version %d, git_sha=%s",
                    REDISTIMESERIES_MODULE_VERSION,
                    REDISTIMESERIES_GIT_SHA);

    RTS_GetRedisVersion();
    RedisModule_Log(ctx,
                    "notice",
                    "Redis version found by RedisTimeSeries : %d.%d.%d - %s",
                    RTS_currVersion.redisMajorVersion,
                    RTS_currVersion.redisMinorVersion,
                    RTS_currVersion.redisPatchVersion,
                    RTS_IsEnterprise() ? "enterprise" : "oss");
    if (RTS_IsEnterprise()) {
        RedisModule_Log(ctx,
                        "notice",
                        "Redis Enterprise version found by RedisTimeSeries : %d.%d.%d-%d",
                        RTS_RlecMajorVersion,
                        RTS_RlecMinorVersion,
                        RTS_RlecPatchVersion,
                        RTS_RlecBuild);
    }

    if (RTS_CheckSupportedVestion() != REDISMODULE_OK) {
        RedisModule_Log(ctx,
                        "warning",
                        "Redis version is too old, please upgrade to redis "
                        "%d.%d.%d and above.",
                        RTS_minSupportedVersion.redisMajorVersion,
                        RTS_minSupportedVersion.redisMinorVersion,
                        RTS_minSupportedVersion.redisPatchVersion);
        RedisModule_FreeThreadSafeContext(rts_staticCtx);
        rts_staticCtx = NULL;

        return REDISMODULE_ERR;
    }

    if (!LoadConfiguration(ctx, argv, argc)) {
        RedisModule_FreeThreadSafeContext(rts_staticCtx);
        rts_staticCtx = NULL;

        return REDISMODULE_ERR;
    }

    initGlobalCompactionFunctions();

    if (register_mr(ctx, TSGlobalConfig.numThreads) != REDISMODULE_OK) {
        FreeConfigAndStaticCtx();

        return REDISMODULE_ERR;
    }

    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = series_rdb_load,
        .rdb_save = series_rdb_save,
        .aof_rewrite = RMUtil_DefaultAofRewrite,
        .mem_usage = SeriesMemUsage,
        .copy = CopySeries,
        .free = FreeSeries,
        .defrag = DefragSeries,
    };

    SeriesType = RedisModule_CreateDataType(ctx, "TSDB-TYPE", TS_LATEST_ENCVER, &tm);
    if (SeriesType == NULL) {
        FreeConfigAndStaticCtx();

        return REDISMODULE_ERR;
    }

    RedisModuleTypeExtMethods etm = {
        .version = REDISMODULE_TYPE_EXT_METHOD_VERSION,
        .key_added_to_db_dict = keyAddedToDbDict,
        .removing_key_from_db_dict = keyRemovedFromDbDict,
        .get_key_metadata_for_rdb = NULL,
    };
    if (RedisModule_SetDataTypeExtensions != NULL) {
        if (RedisModule_SetDataTypeExtensions(ctx, SeriesType, &etm) != REDISMODULE_OK) {
            FreeConfigAndStaticCtx();

            return REDISMODULE_ERR;
        }
    }

    if (RedisModule_AddACLCategory &&
        RedisModule_AddACLCategory(ctx, TIMESERIES_MODULE_ACL_CATEGORY_NAME) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "Failed to add ACL category");

        FreeConfigAndStaticCtx();

        return REDISMODULE_ERR;
    }

    IndexInit();
    if (RedisModule_RegisterDefragFunc2(ctx, DefragIndex) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "Failed to register defrag function");
        FreeConfigAndStaticCtx();

        return REDISMODULE_ERR;
    }

    RegisterCommandWithModesAndAcls(ctx, "ts.create", TSDB_create, "write deny-oom", "write fast");
    RegisterCommandWithModesAndAcls(ctx, "ts.alter", TSDB_alter, "write deny-oom", "write");
    RegisterCommandWithModesAndAcls(ctx, "ts.createrule", TSDB_createRule, "write fast", "write");
    RegisterCommandWithModesAndAcls(ctx, "ts.deleterule", TSDB_deleteRule, "write", "write fast");
    RegisterCommandWithModesAndAcls(ctx, "ts.add", TSDB_add, "write deny-oom", "write");
    RegisterCommandWithModesAndAcls(ctx, "ts.incrby", TSDB_incrby, "write deny-oom", "write");
    RegisterCommandWithModesAndAcls(ctx, "ts.decrby", TSDB_incrby, "write deny-oom", "write");
    RegisterCommandWithModesAndAcls(ctx, "ts.range", TSDB_range, "readonly", "read");
    RegisterCommandWithModesAndAcls(ctx, "ts.revrange", TSDB_revrange, "readonly", "read");

    if (RedisModule_CreateCommand(ctx, "ts.queryindex", TSDB_queryindex, "readonly", 0, 0, -1) ==
        REDISMODULE_ERR) {
        FreeConfigAndStaticCtx();

        return REDISMODULE_ERR;
    }

    SetCommandAcls(ctx, "ts.queryindex", "read");

    RegisterCommandWithModesAndAcls(ctx, "ts.info", TSDB_info, "readonly", "read fast");
    RegisterCommandWithModesAndAcls(ctx, "ts.get", TSDB_get, "readonly", "read fast");
    // TS.BGET may block on the key; intentionally NOT flagged "fast".
    RegisterCommandWithModesAndAcls(ctx, "ts.bget", TSDB_bget, "readonly", "read");
    RegisterCommandWithModesAndAcls(ctx, "ts.del", TSDB_delete, "write", "write");

    if (RedisModule_CreateCommand(ctx, "ts.madd", TSDB_madd, "write deny-oom", 1, -1, 3) ==
        REDISMODULE_ERR) {
        FreeConfigAndStaticCtx();

        return REDISMODULE_ERR;
    }

    SetCommandAcls(ctx, "ts.madd", "write");

    if (RedisModule_CreateCommand(ctx, "ts.mrange", TSDB_mrange, "readonly", 0, 0, -1) ==
        REDISMODULE_ERR) {
        FreeConfigAndStaticCtx();

        return REDISMODULE_ERR;
    }

    SetCommandAcls(ctx, "ts.mrange", "read");

    if (RedisModule_CreateCommand(ctx, "ts.mrevrange", TSDB_mrevrange, "readonly", 0, 0, -1) ==
        REDISMODULE_ERR) {
        FreeConfigAndStaticCtx();

        return REDISMODULE_ERR;
    }

    SetCommandAcls(ctx, "ts.mrevrange", "read");

    // TS.NRANGE / TS.NREVRANGE: keys are explicit but at variable positions (after
    // numkeys), so they can't use the fixed first/last/step args; a keynum key-spec
    // is attached via RegisterTSCommandInfos (TS_NRANGE_INFO) for cluster routing.
    if (RedisModule_CreateCommand(ctx, "ts.nrange", TSDB_nrange, "readonly", 0, 0, -1) ==
        REDISMODULE_ERR) {
        FreeConfigAndStaticCtx();

        return REDISMODULE_ERR;
    }

    SetCommandAcls(ctx, "ts.nrange", "read");

    if (RedisModule_CreateCommand(ctx, "ts.nrevrange", TSDB_nrevrange, "readonly", 0, 0, -1) ==
        REDISMODULE_ERR) {
        FreeConfigAndStaticCtx();

        return REDISMODULE_ERR;
    }

    SetCommandAcls(ctx, "ts.nrevrange", "read");

    if (RedisModule_CreateCommand(ctx, "ts.mget", TSDB_mget, "readonly", 0, 0, -1) ==
        REDISMODULE_ERR) {
        FreeConfigAndStaticCtx();

        return REDISMODULE_ERR;
    }

    SetCommandAcls(ctx, "ts.mget", "read");

    if (RegisterTSCommandInfos(ctx) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "Failed to register timeseries command infos");
        FreeConfigAndStaticCtx();

        return REDISMODULE_ERR;
    }

    if (RedisModule_SubscribeToServerEvent) {
        // we have server events support, lets subscribe to relevant events.
        if (RedisModule_ShardingGetKeySlot != NULL) {
            RedisModule_Log(ctx, "notice", "%s", "Subscribe to sharding events");
            RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Sharding, ShardingEvent);
        }
        if (RedisModule_ClusterCanAccessKeysInSlot != NULL) {
            RedisModule_Log(ctx, "notice", "%s", "Subscribe to ASM events");
            RedisModule_SubscribeToServerEvent(
                ctx, RedisModuleEvent_ClusterSlotMigration, ClusterAsmCallback);
            RedisModule_SubscribeToServerEvent(
                ctx, RedisModuleEvent_ClusterSlotMigrationTrim, ClusterAsmTrimCallback);
        }
        RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_FlushDB, FlushEventCallback);
        RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_SwapDB, swapDbEventCallback);
        RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Persistence, persistCallback);
    }

    Initialize_RdbNotifications(ctx);

    return REDISMODULE_OK;
}

#if (defined(DEBUG) || defined(_DEBUG)) && !defined(NDEBUG)
#include "readies/cetara/diag/gdb.c"
#endif
