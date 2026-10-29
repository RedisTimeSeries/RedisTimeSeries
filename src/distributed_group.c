/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "distributed_group.h"
#include "module.h"
#include "reply.h"
#include "LibMR/src/utils/arr.h"
#include "rmutil/alloc.h"
#include <math.h>

double ShardReplyDouble(const redisReply *reply) {
    if (reply->type == REDIS_REPLY_DOUBLE)
        return reply->dval;
    RedisModule_Assert(reply->type == REDIS_REPLY_STRING || reply->type == REDIS_REPLY_STATUS);
    // Aggregation may overflow even though client samples cannot contain infinity.
    if ((reply->len == 3 && strncasecmp(reply->str, "inf", 3) == 0) ||
        (reply->len == 4 && strncasecmp(reply->str, "+inf", 4) == 0) ||
        (reply->len == 8 && strncasecmp(reply->str, "Infinity", 8) == 0) ||
        (reply->len == 9 && strncasecmp(reply->str, "+Infinity", 9) == 0))
        return INFINITY;
    if ((reply->len == 4 && strncasecmp(reply->str, "-inf", 4) == 0) ||
        (reply->len == 9 && strncasecmp(reply->str, "-Infinity", 9) == 0))
        return -INFINITY;
    double value;
    RedisModule_Assert(parse_double_cstr(reply->str, reply->len, &value));
    return value;
}

typedef struct GroupSample
{
    timestamp_t timestamp;
    uint64_t count;
    void *context;
} GroupSample;

typedef struct Group
{
    RedisModuleString *value;
    ARR(RedisModuleString *) sources;
    RedisModuleDict *samples;
} Group;

struct DistributedGroups
{
    TS_AGG_TYPES_T reducer;
    AggregationClass *aggregation;
    RedisModuleDict *groups;
};

static bool IsCount(TS_AGG_TYPES_T type) {
    return type == TS_AGG_COUNT || type == TS_AGG_COUNT_NAN || type == TS_AGG_COUNT_ALL;
}

bool DistributedGroupSupported(TS_AGG_TYPES_T reducer) {
    return IsCount(reducer) || reducer == TS_AGG_SUM || reducer == TS_AGG_MIN ||
           reducer == TS_AGG_MAX || reducer == TS_AGG_AVG;
}

DistributedGroups *DistributedGroups_New(TS_AGG_TYPES_T reducer) {
    RedisModule_Assert(DistributedGroupSupported(reducer));
    DistributedGroups *g = malloc(sizeof(*g));
    *g = (DistributedGroups){ reducer, GetAggClass(reducer), RedisModule_CreateDict(NULL) };
    return g;
}

static Group *GetGroup(DistributedGroups *groups, const char *value, size_t len) {
    Group *group = RedisModule_DictGetC(groups->groups, (void *)value, len, NULL);
    if (!group) {
        group = malloc(sizeof(*group));
        *group = (Group){ RedisModule_CreateString(NULL, value, len),
                          array_new(RedisModuleString *, 1),
                          RedisModule_CreateDict(NULL) };
        RedisModule_DictSetC(groups->groups, (void *)value, len, group);
    }
    return group;
}

static void AddSource(Group *group, const char *key, size_t len) {
    group->sources = array_append(group->sources, RedisModule_CreateString(NULL, key, len));
}

static GroupSample *GetSample(DistributedGroups *groups, Group *group, timestamp_t timestamp) {
    unsigned char key[sizeof(timestamp)];
    for (size_t i = 0; i < sizeof(key); ++i)
        key[sizeof(key) - 1 - i] = timestamp >> (8 * i);
    GroupSample *sample = RedisModule_DictGetC(group->samples, key, sizeof(key), NULL);
    if (!sample) {
        sample = malloc(sizeof(*sample));
        *sample = (GroupSample){ timestamp, 0, groups->aggregation->createContext(false) };
        RedisModule_DictSetC(group->samples, key, sizeof(key), sample);
    }
    return sample;
}

static double Finalize(const DistributedGroups *groups, const GroupSample *sample) {
    double value = NAN;
    if (sample->count)
        groups->aggregation->finalize(sample->context, &value);
    else if (IsCount(groups->reducer))
        groups->aggregation->finalizeEmpty(sample->context, &value);
    return value;
}

void DistributedGroups_AddSeries(DistributedGroups *groups,
                                 Series *series,
                                 const char *label,
                                 const RangeArgs *args,
                                 bool excludeEmpty) {
    size_t len;
    const char *value = SeriesGetCStringLabelValue(series, label, &len);
    if (!value)
        return;
    AbstractIterator *iter = SeriesQuery(series, args, false, true);
    Group *group = excludeEmpty ? NULL : GetGroup(groups, value, len);
    EnrichedChunk *chunk;
    while ((chunk = iter->GetNext(iter))) {
        if (chunk->samples.num_samples && !group)
            group = GetGroup(groups, value, len);
        for (size_t i = 0; i < chunk->samples.num_samples; ++i) {
            RedisModule_Assert(chunk->samples.values_per_sample <= 1);
            timestamp_t ts = chunk->samples.timestamps[i];
            double v = Samples_value_at(&chunk->samples, i, 0);
            GroupSample *sample = GetSample(groups, group, ts);
            if (groups->aggregation->isValueValid(v)) {
                groups->aggregation->appendValue(sample->context, v, ts);
                ++sample->count;
            }
        }
    }
    iter->Close(iter);
    if (group) {
        const char *name = RedisModule_StringPtrLen(series->keyName, &len);
        AddSource(group, name, len);
    }
}

static void AddPartial(DistributedGroups *groups,
                       Group *group,
                       timestamp_t timestamp,
                       double value,
                       uint64_t count) {
    GroupSample *sample = GetSample(groups, group, timestamp);
    if (groups->reducer == TS_AGG_AVG) {
        AvgMergePartial(sample->context, value, count);
    } else if (IsCount(groups->reducer)) {
        CountMergePartial(sample->context, value);
    } else if (count) {
        groups->aggregation->appendValue(sample->context, value, timestamp);
    }
    RedisModule_Assert(UINT64_MAX - sample->count >= count);
    sample->count += count;
}

void DistributedGroups_Merge(DistributedGroups *dest, const DistributedGroups *source) {
    RedisModule_Assert(dest->reducer == source->reducer);
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(source->groups, "^", NULL, 0);
    Group *src;
    while (RedisModule_DictNextC(iter, NULL, (void **)&src)) {
        size_t len;
        const char *value = RedisModule_StringPtrLen(src->value, &len);
        Group *group = GetGroup(dest, value, len);
        for (size_t i = 0; i < array_len(src->sources); ++i) {
            const char *name = RedisModule_StringPtrLen(src->sources[i], &len);
            AddSource(group, name, len);
        }
        RedisModuleDictIter *samples = RedisModule_DictIteratorStartC(src->samples, "^", NULL, 0);
        GroupSample *sample;
        while (RedisModule_DictNextC(samples, NULL, (void **)&sample))
            AddPartial(dest, group, sample->timestamp, Finalize(source, sample), sample->count);
        RedisModule_DictIteratorStop(samples);
    }
    RedisModule_DictIteratorStop(iter);
}

void DistributedGroups_ReplyPartial(RedisModuleCtx *ctx, const DistributedGroups *groups) {
    RedisModule_ReplyWithArray(ctx, 3);
    RedisModule_ReplyWithCString(ctx, TS_GROUP_REPLY_VERSION);
    RedisModule_ReplyWithLongLong(ctx, groups->reducer);
    RedisModule_ReplyWithArray(ctx, RedisModule_DictSize(groups->groups));
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(groups->groups, "^", NULL, 0);
    Group *group;
    while (RedisModule_DictNextC(iter, NULL, (void **)&group)) {
        RedisModule_ReplyWithArray(ctx, 3);
        RedisModule_ReplyWithString(ctx, group->value);
        RedisModule_ReplyWithArray(ctx, array_len(group->sources));
        for (size_t i = 0; i < array_len(group->sources); ++i)
            RedisModule_ReplyWithString(ctx, group->sources[i]);
        RedisModule_ReplyWithArray(ctx, RedisModule_DictSize(group->samples));
        RedisModuleDictIter *samples = RedisModule_DictIteratorStartC(group->samples, "^", NULL, 0);
        GroupSample *sample;
        while (RedisModule_DictNextC(samples, NULL, (void **)&sample)) {
            RedisModule_ReplyWithArray(ctx, 3);
            RedisModule_ReplyWithLongLong(ctx, sample->timestamp);
            RedisModule_ReplyWithDouble(ctx, Finalize(groups, sample));
            RedisModule_ReplyWithLongLong(ctx, sample->count);
        }
        RedisModule_DictIteratorStop(samples);
    }
    RedisModule_DictIteratorStop(iter);
}

DistributedGroups *DistributedGroups_Parse(const redisReply *reply) {
    RedisModule_Assert(reply->elements == 3 && reply->element[1]->type == REDIS_REPLY_INTEGER);
    DistributedGroups *groups = DistributedGroups_New(reply->element[1]->integer);
    const redisReply *entries = reply->element[2];
    RedisModule_Assert(entries->type == REDIS_REPLY_ARRAY);
    for (size_t i = 0; i < entries->elements; ++i) {
        const redisReply *entry = entries->element[i];
        RedisModule_Assert(entry->type == REDIS_REPLY_ARRAY && entry->elements == 3);
        const redisReply *value = entry->element[0], *sources = entry->element[1],
                         *samples = entry->element[2];
        RedisModule_Assert(value->type == REDIS_REPLY_STRING &&
                           sources->type == REDIS_REPLY_ARRAY &&
                           samples->type == REDIS_REPLY_ARRAY);
        Group *group = GetGroup(groups, value->str, value->len);
        for (size_t j = 0; j < sources->elements; ++j) {
            RedisModule_Assert(sources->element[j]->type == REDIS_REPLY_STRING);
            AddSource(group, sources->element[j]->str, sources->element[j]->len);
        }
        for (size_t j = 0; j < samples->elements; ++j) {
            const redisReply *sample = samples->element[j];
            RedisModule_Assert(sample->type == REDIS_REPLY_ARRAY && sample->elements == 3 &&
                               sample->element[0]->type == REDIS_REPLY_INTEGER &&
                               sample->element[2]->type == REDIS_REPLY_INTEGER &&
                               sample->element[2]->integer >= 0);
            double v = ShardReplyDouble(sample->element[1]);
            AddPartial(groups, group, sample->element[0]->integer, v, sample->element[2]->integer);
        }
    }
    return groups;
}

static int CompareSources(const void *a, const void *b) {
    return RedisModule_StringCompare(*(RedisModuleString **)a, *(RedisModuleString **)b);
}

void DistributedGroups_Reply(RedisModuleCtx *ctx,
                             DistributedGroups *groups,
                             const MRangeArgs *args) {
    ReplyWithMapOrArray(ctx, RedisModule_DictSize(groups->groups), false);
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(groups->groups, "^", NULL, 0);
    Group *group;
    while (RedisModule_DictNextC(iter, NULL, (void **)&group)) {
        qsort(group->sources, array_len(group->sources), sizeof(*group->sources), CompareSources);
        CreateCtx create = { .chunkSizeBytes = Chunk_SIZE_BYTES_SECS,
                             .options = SERIES_OPT_UNCOMPRESSED };
        size_t len;
        const char *value = RedisModule_StringPtrLen(group->value, &len);
        Series *result = NewSeries(
            RedisModule_CreateStringPrintf(NULL, "%s=%s", args->groupByLabel, value), &create);
        result->labelsCount = 3;
        result->labels = calloc(3, sizeof(*result->labels));
        result->labels[0] =
            (Label){ RedisModule_CreateString(NULL, args->groupByLabel, strlen(args->groupByLabel)),
                     RedisModule_CreateString(NULL, value, len) };
        const char *reducer = AggTypeEnumToStringLowerCase(groups->reducer);
        result->labels[1] = (Label){ RedisModule_CreateString(NULL, "__reducer__", 11),
                                     RedisModule_CreateString(NULL, reducer, strlen(reducer)) };
        result->labels[2] = (Label){ RedisModule_CreateString(NULL, "__source__", 10),
                                     RedisModule_CreateString(NULL, "", 0) };
        for (size_t i = 0; i < array_len(group->sources); ++i) {
            const char *name = RedisModule_StringPtrLen(group->sources[i], &len);
            if (i)
                RedisModule_StringAppendBuffer(NULL, result->labels[2].value, ",", 1);
            RedisModule_StringAppendBuffer(NULL, result->labels[2].value, name, len);
        }
        RedisModuleDictIter *samples = RedisModule_DictIteratorStartC(group->samples, "^", NULL, 0);
        GroupSample *sample;
        while (RedisModule_DictNextC(samples, NULL, (void **)&sample))
            SeriesAddSample(result, sample->timestamp, Finalize(groups, sample));
        RedisModule_DictIteratorStop(samples);
        RangeArgs range = RangeArgs_ZeroProcessing(&args->rangeArgs);
        result->srcKey = (RedisModuleString *)group->sources; // borrowed for RESP3 source metadata
        ReplySeriesArrayPos(ctx,
                            result,
                            args->withLabels,
                            (RedisModuleString **)args->limitLabels,
                            args->numLimitLabels,
                            &range,
                            args->reverse,
                            true,
                            NULL,
                            NULL);
        result->srcKey = NULL;
        FreeSeries(result);
    }
    RedisModule_DictIteratorStop(iter);
}

void DistributedGroups_Free(DistributedGroups *groups) {
    if (!groups)
        return;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(groups->groups, "^", NULL, 0);
    Group *group;
    while (RedisModule_DictNextC(iter, NULL, (void **)&group)) {
        RedisModuleDictIter *samples = RedisModule_DictIteratorStartC(group->samples, "^", NULL, 0);
        GroupSample *sample;
        while (RedisModule_DictNextC(samples, NULL, (void **)&sample)) {
            groups->aggregation->freeContext(sample->context);
            free(sample);
        }
        RedisModule_DictIteratorStop(samples);
        RedisModule_FreeDict(NULL, group->samples);
        RedisModule_FreeString(NULL, group->value);
        array_free_ex(group->sources, RedisModule_FreeString(NULL, *(RedisModuleString **)ptr));
        free(group);
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_FreeDict(NULL, groups->groups);
    free(groups);
}
