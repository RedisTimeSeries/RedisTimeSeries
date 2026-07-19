/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "streaming_resultset.h"

#include "compaction.h"
#include "endianconv.h"
#include "indexer.h"
#include "module.h"
#include "reply.h"
#include "resultset.h"
#include "tsdb.h"

#include "RedisModulesSDK/redismodule.h"
#include "rmutil/alloc.h"
#include "rmutil/util.h"
#include "utils/arr.h"

#include <math.h>
#include <string.h>

// Per-(group, timestamp) accumulator entry. `has_value` distinguishes
// "appended at least one non-NaN sample" from "context exists but only NaN
// inputs were fed". Mirrors the is_nan tracking in
// MultiSeriesAggDupSampleIterator_GetNext (src/multiseries_agg_dup_sample_iterator.c:27-50)
// so finalize semantics match.
typedef struct TsCtxEntry
{
    void *aggCtx;
    bool has_value;
} TsCtxEntry;

typedef struct StreamingGroup
{
    char *labelValue;                // strdup'd
    RedisModuleString *sourceLabel;  // accumulated "key1,key2,..." for __source__
    RedisModuleString **sourceArray; // RESP3-only: array of source key names
    RedisModuleDict *tsContexts;     // big-endian timestamp -> TsCtxEntry*
} StreamingGroup;

struct TS_StreamingResultSet
{
    char *labelKey;
    ReducerArgs reducer;
    RangeArgs rangeArgs;
    RedisModuleDict *groups; // labelValue -> StreamingGroup*
    AggregationClass *aggClass;
};

TS_StreamingResultSet *StreamingResultSet_Create(const char *groupbyLabel,
                                                 const ReducerArgs *reducer,
                                                 const RangeArgs *rangeArgs) {
    if (!groupbyLabel || !reducer || !rangeArgs)
        return NULL;
    AggregationClass *aggClass = GetAggClass(reducer->agg_type);
    if (!aggClass)
        return NULL;

    TS_StreamingResultSet *r = malloc(sizeof(*r));
    r->labelKey = strdup(groupbyLabel);
    r->reducer = *reducer;
    r->rangeArgs = *rangeArgs;
    r->groups = RedisModule_CreateDict(NULL);
    r->aggClass = aggClass;
    return r;
}

static StreamingGroup *streaming_group_create(const char *labelValue, size_t labelLen) {
    StreamingGroup *g = malloc(sizeof(*g));
    g->labelValue = strndup(labelValue, labelLen);
    g->sourceLabel = RedisModule_CreateString(NULL, "", 0);
    g->sourceArray = (RedisModuleString **)array_new(RedisModuleString *, 1);
    g->tsContexts = RedisModule_CreateDict(NULL);
    return g;
}

static void streaming_group_free(StreamingGroup *g, AggregationClass *aggClass) {
    if (!g)
        return;
    free(g->labelValue);
    if (g->sourceLabel)
        RedisModule_FreeString(NULL, g->sourceLabel);
    if (g->sourceArray) {
        for (size_t i = 0; i < array_len(g->sourceArray); i++) {
            RedisModule_FreeString(NULL, g->sourceArray[i]);
        }
        array_free(g->sourceArray);
    }
    if (g->tsContexts) {
        // Defensive: free any TsCtxEntry still in the dict (should be empty
        // after FinalizeAndReply walked it, but Free can be called pre-finalize).
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(g->tsContexts, "^", NULL, 0);
        TsCtxEntry *entry;
        while (RedisModule_DictNextC(iter, NULL, (void **)&entry) != NULL) {
            if (entry) {
                if (entry->aggCtx)
                    aggClass->freeContext(entry->aggCtx);
                free(entry);
            }
        }
        RedisModule_DictIteratorStop(iter);
        RedisModule_FreeDict(NULL, g->tsContexts);
    }
    free(g);
}

void StreamingResultSet_Free(TS_StreamingResultSet *r) {
    if (!r)
        return;
    if (r->groups) {
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(r->groups, "^", NULL, 0);
        StreamingGroup *g;
        while (RedisModule_DictNextC(iter, NULL, (void **)&g) != NULL) {
            streaming_group_free(g, r->aggClass);
        }
        RedisModule_DictIteratorStop(iter);
        RedisModule_FreeDict(NULL, r->groups);
    }
    free(r->labelKey);
    free(r);
}

// Lookup or create the per-(group, timestamp) entry.
static TsCtxEntry *get_or_create_ts_entry(StreamingGroup *g,
                                          AggregationClass *aggClass,
                                          timestamp_t ts) {
    // Big-endian encoding so RedisModuleDict iterates in ascending ts order
    // on finalize. Same encoding used elsewhere in the codebase
    // (seriesEncodeTimestamp usage). Append samples to the
    // reduced Series in ascending order = single-chunk-grow path.
    uint64_t encoded;
    seriesEncodeTimestamp(&encoded, ts);
    int nokey;
    TsCtxEntry *entry = RedisModule_DictGetC(g->tsContexts, &encoded, sizeof encoded, &nokey);
    if (nokey) {
        entry = malloc(sizeof(*entry));
        entry->aggCtx = aggClass->createContext(/*reverse=*/false);
        entry->has_value = false;
        RedisModule_DictSetC(g->tsContexts, &encoded, sizeof encoded, entry);
    }
    return entry;
}

void StreamingResultSet_FeedSeries(TS_StreamingResultSet *r, Series *series) {
    if (!r || !series)
        return;

    // Find this series's groupby label value; series without the label are skipped.
    size_t labelLen;
    const char *labelValue = SeriesGetCStringLabelValue(series, r->labelKey, &labelLen);
    if (!labelValue)
        return;

    int nokey;
    StreamingGroup *group = RedisModule_DictGetC(r->groups, (void *)labelValue, labelLen, &nokey);
    if (nokey) {
        group = streaming_group_create(labelValue, labelLen);
        RedisModule_DictSetC(r->groups, (void *)labelValue, labelLen, group);
    }

    // Append this series's keyName to the __source__ accumulator.
    if (RedisModule_StringPtrLen(group->sourceLabel, NULL)[0] != '\0') {
        RedisModule_StringAppendBuffer(NULL, group->sourceLabel, ",", 1);
    }
    size_t keyLen;
    const char *keyStr = RedisModule_StringPtrLen(series->keyName, &keyLen);
    RedisModule_StringAppendBuffer(NULL, group->sourceLabel, keyStr, keyLen);
    array_append(group->sourceArray, RedisModule_HoldString(NULL, series->keyName));

    // Iterate the series's samples (applies AGGREGATION + filters + range bounds
    // per the rangeArgs; same iterator the buffered path eventually uses).
    AbstractSampleIterator *iter = SeriesCreateSampleIterator(series,
                                                              &r->rangeArgs,
                                                              /*reverse=*/false,
                                                              /*check_retention=*/true);
    Sample sample;
    while (iter->GetNext(iter, &sample) == CR_OK) {
        // Touch the entry so it exists at finalize time (matches the
        // iterator's behavior: NaN-only timestamps still produce a NaN
        // sample in the output). Only feed values this aggregation
        // considers valid (e.g. countnan/countall also want NaN inputs) —
        // mirrors MultiSeriesAggDupSampleIterator_GetNext's is_valid check.
        TsCtxEntry *entry = get_or_create_ts_entry(group, r->aggClass, sample.timestamp);
        if (!r->aggClass->isValueValid(sample.value))
            continue;
        r->aggClass->appendValue(entry->aggCtx, sample.value, sample.timestamp);
        entry->has_value = true;
    }
    iter->Close(iter);
}

static void emit_group_reply(RedisModuleCtx *ctx,
                             TS_StreamingResultSet *r,
                             StreamingGroup *group,
                             bool withLabels,
                             RedisModuleString *limitLabels[],
                             ushort limitLabelsSize,
                             bool reverse) {
    // Build a temp "reduced" Series, walk the per-ts contexts in order,
    // finalize each into a sample, append to the reduced Series.
    size_t serieNameLen = strlen(r->labelKey) + strlen(group->labelValue) + 2;
    char *serieName = malloc(serieNameLen);
    serieNameLen = sprintf(serieName, "%s=%s", r->labelKey, group->labelValue);

    CreateCtx cCtx = {
        .labels = NULL,
        .labelsCount = 0,
        .chunkSizeBytes = Chunk_SIZE_BYTES_SECS,
        .options = SERIES_OPT_UNCOMPRESSED,
    };
    Series *reduced = NewSeries(RedisModule_CreateString(NULL, serieName, serieNameLen), &cCtx);

    if (_ReplyMap(ctx)) {
        // Abuse srcKey to store the RESP3 source-keys array (same srcKey
        // overload trick used by the buffered ResultSet path in src/resultset.c).
        size_t nsrc = array_len(group->sourceArray);
        RedisModuleString **arr = (RedisModuleString **)array_new(RedisModuleString *, nsrc);
        for (size_t i = 0; i < nsrc; i++) {
            array_append(arr, group->sourceArray[i]);
        }
        reduced->srcKey = (RedisModuleString *)arr;
    }

    // Walk per-ts contexts in ascending order (BE-encoded keys → radix sort).
    // For reverse output, ReplySeriesArrayPos handles iteration direction at
    // emit time, so we always append samples to `reduced` in ascending order.
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(group->tsContexts, "^", NULL, 0);
    void *encodedKey;
    size_t encodedKeyLen;
    TsCtxEntry *entry;
    while ((encodedKey = RedisModule_DictNextC(iter, &encodedKeyLen, (void **)&entry)) != NULL) {
        // Decode timestamp from big-endian.
        uint64_t e;
        memcpy(&e, encodedKey, sizeof(e));
        timestamp_t ts = ntohu64(e);

        double val;
        if (entry->has_value) {
            int rc = r->aggClass->finalize(entry->aggCtx, &val);
            if (rc != TSDB_OK) {
                r->aggClass->finalizeEmpty(entry->aggCtx, &val);
            }
        } else {
            // No valid-for-this-aggregation samples were fed at this
            // timestamp. Count-type reducers report 0; other reducers
            // report NaN to signal "no data" (matches
            // MultiSeriesAggDupSampleIterator_GetNext behavior).
            TS_AGG_TYPES_T type = r->aggClass->type;
            if (type == TS_AGG_COUNT || type == TS_AGG_COUNT_NAN || type == TS_AGG_COUNT_ALL) {
                r->aggClass->finalizeEmpty(entry->aggCtx, &val);
            } else {
                val = NAN;
            }
        }
        SeriesAddSample(reduced, ts, val);
        r->aggClass->freeContext(entry->aggCtx);
        free(entry);
    }
    RedisModule_DictIteratorStop(iter);
    // Empty the dict so streaming_group_free doesn't double-free entries.
    RedisModule_FreeDict(NULL, group->tsContexts);
    group->tsContexts = NULL;

    // Replace the temp series's labels with the reducer labels.
    Label *labels = createReducedSeriesLabels(ctx, r->labelKey, group->labelValue, &r->reducer);
    // Stuff the accumulated __source__ string into labels[2].value.
    size_t srcLen;
    const char *srcStr = RedisModule_StringPtrLen(group->sourceLabel, &srcLen);
    RedisModule_StringAppendBuffer(NULL, labels[2].value, srcStr, srcLen);

    FreeLabels(reduced->labels, reduced->labelsCount);
    reduced->labels = labels;
    reduced->labelsCount = 3;

    // Emit (uses a minimized rangeArgs that doesn't re-aggregate — same
    // minimized-rangeArgs trick used by the buffered ResultSet emit path).
    RangeArgs minimizedArgs = r->rangeArgs;
    minimizedArgs.startTimestamp = 0;
    minimizedArgs.endTimestamp = UINT64_MAX;
    minimizedArgs.aggregationArgs.numClasses = 0;
    minimizedArgs.aggregationArgs.classes = NULL;
    minimizedArgs.aggregationArgs.timeDelta = 0;
    minimizedArgs.filterByTSArgs.hasValue = false;
    minimizedArgs.filterByValueArgs.hasValue = false;
    minimizedArgs.latest = false;

    ReplySeriesArrayPos(ctx,
                        reduced,
                        withLabels,
                        limitLabels,
                        limitLabelsSize,
                        &minimizedArgs,
                        reverse,
                        /*print_reduced=*/true,
                        NULL,
                        NULL);

    FreeTempSeries(reduced);
    free(serieName);
}

void StreamingResultSet_FinalizeAndReply(RedisModuleCtx *ctx,
                                         TS_StreamingResultSet *r,
                                         bool withLabels,
                                         RedisModuleString *limitLabels[],
                                         ushort limitLabelsSize,
                                         bool reverse) {
    if (!r) {
        ReplyWithMapOrArray(ctx, 0, false);
        return;
    }

    ReplyWithMapOrArray(ctx, RedisModule_DictSize(r->groups), false);

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(r->groups, "^", NULL, 0);
    StreamingGroup *group;
    while (RedisModule_DictNextC(iter, NULL, (void **)&group) != NULL) {
        emit_group_reply(ctx, r, group, withLabels, limitLabels, limitLabelsSize, reverse);
    }
    RedisModule_DictIteratorStop(iter);

    StreamingResultSet_Free(r);
}
