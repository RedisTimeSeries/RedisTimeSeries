/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "resultset.h"

#include "indexer.h"
#include "redismodule.h"
#include "reply.h"
#include "series_iterator.h"
#include "string.h"
#include "tsdb.h"

#include "rmutil/alloc.h"

typedef enum
{
    GroupType_Series,
    GroupType_ResultSet,
} GroupType;

struct TS_ResultSet
{
    RedisModuleDict *groups;
    GroupType groupsType;
    char *labelkey;
    char *labelvalue;
};

TS_ResultSet *ResultSet_Create() {
    TS_ResultSet *r = malloc(sizeof(TS_ResultSet));
    r->groups = RedisModule_CreateDict(NULL);
    r->groupsType = GroupType_Series;
    r->labelkey = NULL;
    r->labelvalue = NULL;
    return r;
}

int ResultSet_SetLabelValue(TS_ResultSet *r, const char *label) {
    r->labelvalue = strdup(label);
    return true;
}

int ResultSet_SetLabelKey(TS_ResultSet *r, const char *labelkey) {
    r->labelkey = strdup(labelkey);
    return true;
}

int ResultSet_GroupbyLabel(TS_ResultSet *r, const char *label) {
    r->groupsType = GroupType_ResultSet;
    r->labelkey = strdup(label);
    return true;
}

int parseMultiSeriesReduceOp(const char *reducerstr, MultiSeriesReduceOp *reducerOp) {
    if (strncasecmp(reducerstr, "sum", 3) == 0) {
        *reducerOp = MultiSeriesReduceOp_Sum;
        return TSDB_OK;

    } else if (strncasecmp(reducerstr, "max", 3) == 0) {
        *reducerOp = MultiSeriesReduceOp_Max;
        return TSDB_OK;

    } else if (strncasecmp(reducerstr, "min", 3) == 0) {
        *reducerOp = MultiSeriesReduceOp_Min;
        return TSDB_OK;
    }
    return TSDB_ERROR;
}

Label *createReducedSeriesLabels(TS_ResultSet *r, MultiSeriesReduceOp reducerOp) {
    // Labels:
    // <label>=<groupbyvalue>
    // __reducer__=<reducer>
    // __source__=key1,key2,key3
    char *reducer_str = NULL;
    switch (reducerOp) {
        case MultiSeriesReduceOp_Max:
            reducer_str = "max";
            break;
        case MultiSeriesReduceOp_Min:
            reducer_str = "min";
            break;
        case MultiSeriesReduceOp_Sum:
            reducer_str = "sum";
            break;
    }
    Label *labels = malloc(sizeof(Label) * 3);
    labels[0].key = RedisModule_CreateStringPrintf(NULL, "%s", r->labelkey);
    labels[0].value = RedisModule_CreateStringPrintf(NULL, "%s", r->labelvalue);
    labels[1].key = RedisModule_CreateStringPrintf(NULL, "__reducer__");
    labels[1].value = RedisModule_CreateString(NULL, reducer_str, strlen(reducer_str));
    labels[2].key = RedisModule_CreateStringPrintf(NULL, "__source__");
    labels[2].value = RedisModule_CreateStringPrintf(NULL, "");
    return labels;
}

void FreeTempSeries(Series *s) {
    RedisModule_FreeString(NULL, s->keyName);
    if (s->chunks) {
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(s->chunks, "^", NULL, 0);
        Chunk_t *currentChunk;
        while (RedisModule_DictNextC(iter, NULL, (void **)&currentChunk) != NULL) {
            s->funcs->FreeChunk(currentChunk);
        }
        RedisModule_DictIteratorStop(iter);
        RedisModule_FreeDict(NULL, s->chunks);
    }
    if (s->labels) {
        FreeLabels(s->labels, s->labelsCount);
    }
    free(s);
}

int ApplySerieRangeIntoNewSerie(Series **dest,
                                Series *source,
                                api_timestamp_t start_ts,
                                api_timestamp_t end_ts,
                                AggregationClass *aggObject,
                                int64_t time_delta,
                                long long maxResults,
                                bool rev) {
    Sample sample;
    CreateCtx cCtx = { 0 };

    Series *new = NewSeries(RedisModule_CreateStringFromString(NULL, source->keyName), &cCtx);
    void *context = NULL;
    long long arraylen = 0;
    timestamp_t last_agg_timestamp;

    // In case a retention is set shouldn't return chunks older than the retention
    // TODO: move to parseRangeArguments(?)
    if (source->retentionTime) {
        start_ts = source->lastTimestamp > source->retentionTime
                       ? max(start_ts, source->lastTimestamp - source->retentionTime)
                       : start_ts;
        // if new start_ts > end_ts, there are no results to return
        if (start_ts > end_ts) {
            *dest = new;
            return REDISMODULE_OK;
        }
    }

    SeriesIterator iterator;
    if (SeriesQuery(source, &iterator, start_ts, end_ts, rev, NULL, 0) != TSDB_OK) {
        // todo: is this the right thing here?
        *dest = new;
        return REDISMODULE_ERR;
    }

    if (aggObject == NULL) {
        // No aggregation
        while (SeriesIteratorGetNext(&iterator, &sample) == CR_OK &&
               (maxResults == -1 || arraylen < maxResults)) {
            SeriesAddSample(new, sample.timestamp, sample.value);
        }
    } else {
        bool firstSample = TRUE;
        context = aggObject->createContext();
        // setting the first timestamp of the aggregation
        timestamp_t init_ts = (rev == false)
                                  ? source->funcs->GetFirstTimestamp(iterator.currentChunk)
                                  : source->funcs->GetLastTimestamp(iterator.currentChunk);
        last_agg_timestamp = init_ts - (init_ts % time_delta);

        while (SeriesIteratorGetNext(&iterator, &sample) == CR_OK &&
               (maxResults == -1 || arraylen < maxResults)) {
            if ((iterator.reverse == false &&
                 sample.timestamp >= last_agg_timestamp + time_delta) ||
                (iterator.reverse == true && sample.timestamp < last_agg_timestamp)) {
                if (firstSample == FALSE) {
                    double value;
                    if (aggObject->finalize(context, &value) == TSDB_OK) {
                        SeriesAddSample(new, last_agg_timestamp, value);
                        aggObject->resetContext(context);
                    }
                }
                last_agg_timestamp = sample.timestamp - (sample.timestamp % time_delta);
            }
            firstSample = FALSE;
            aggObject->appendValue(context, sample.value);
        }
    }
    SeriesIteratorClose(&iterator);

    if (aggObject != NULL) {
        if (arraylen != maxResults) {
            // reply last bucket of data
            double value;
            if (aggObject->finalize(context, &value) == TSDB_OK) {
                SeriesAddSample(new, last_agg_timestamp, value);
                aggObject->resetContext(context);
            }
        }
        aggObject->freeContext(context);
    }
    *dest = new;
    return REDISMODULE_OK;
}

int ResultSet_ApplyReducer(TS_ResultSet *r, MultiSeriesReduceOp reducerOp) {
    size_t currentKeyLen;
    char *currentKey;
    if (r->groupsType == GroupType_Series) {
        Label *labels = createReducedSeriesLabels(r, reducerOp);
        const uint64_t total_series = RedisModule_DictSize(r->groups);
        uint64_t at_pos = 0;
        const size_t serie_name_len = strlen(r->labelkey) + strlen(r->labelvalue) + 2;
        char *serie_name = malloc(serie_name_len);
        sprintf(serie_name, "%s=%s", r->labelkey, r->labelvalue);
        // ^ seek the smallest element of the radix tree.
        // Use the first serie as the initial data
        Series *reduced = NULL;
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(r->groups, "^", NULL, 0);
        currentKey = RedisModule_DictNextC(iter, &currentKeyLen, (void **)&reduced);
        RedisModule_StringAppendBuffer(
            NULL, labels[2].value, (const char *)currentKey, currentKeyLen);
        RedisModule_DictDelC(r->groups, (void *)currentKey, currentKeyLen, NULL);
        RedisModule_DictIteratorReseekC(iter, ">", (void *)currentKey, currentKeyLen);

        at_pos++;
        Series *source;
        while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, (void **)&source)) !=
               NULL) {
            MultiSerieReduce(reduced, source, reducerOp);
            if (at_pos > 0 && at_pos < total_series) {
                RedisModule_StringAppendBuffer(NULL, labels[2].value, ",", 1);
            }
            RedisModule_StringAppendBuffer(
                NULL, labels[2].value, (const char *)currentKey, currentKeyLen);
            at_pos++;
            RedisModule_DictDelC(r->groups, (void *)currentKey, currentKeyLen, NULL);
            RedisModule_DictIteratorReseekC(iter, ">", (void *)currentKey, currentKeyLen);
            FreeTempSeries(source);
        }
        RedisModule_DictIteratorStop(iter);
        FreeLabels(reduced->labels, reduced->labelsCount);
        RedisModule_FreeString(NULL, reduced->keyName);
        reduced->keyName = RedisModule_CreateStringPrintf(NULL, "%s", serie_name);
        reduced->labels = labels;
        reduced->labelsCount = 3;
        RedisModule_DictSetC(r->groups, serie_name, serie_name_len, reduced);
        free(serie_name);
    } else {
        // ^ seek the smallest element of the radix tree.
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(r->groups, "^", NULL, 0);
        TS_ResultSet *innerResultSet;
        while (RedisModule_DictNextC(iter, NULL, (void **)&innerResultSet) != NULL) {
            ResultSet_ApplyReducer(innerResultSet, reducerOp);
        }
        RedisModule_DictIteratorStop(iter);
    }
    return TSDB_OK;
}

int ResultSet_ApplyRange(TS_ResultSet *r,
                         api_timestamp_t start_ts,
                         api_timestamp_t end_ts,
                         AggregationClass *aggObject,
                         int64_t time_delta,
                         long long maxResults,
                         bool rev) {
    size_t currentKeyLen;
    char *currentKey;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(r->groups, "^", NULL, 0);
    if (r->groupsType == GroupType_ResultSet) {
        TS_ResultSet *innerResultSet;
        while ((currentKey = RedisModule_DictNextC(
                    iter, &currentKeyLen, (void **)&innerResultSet)) != NULL) {
            ResultSet_ApplyRange(
                innerResultSet, start_ts, end_ts, aggObject, time_delta, maxResults, rev);
        }
    } else {
        Series *newRangeSerie;
        Series *originalCurrentSerie;
        while ((currentKey = RedisModule_DictNextC(
                    iter, &currentKeyLen, (void **)&originalCurrentSerie)) != NULL) {
            ApplySerieRangeIntoNewSerie(&newRangeSerie,
                                        originalCurrentSerie,
                                        start_ts,
                                        end_ts,
                                        aggObject,
                                        time_delta,
                                        maxResults,
                                        rev);
            // replace the serie with the range trimmed one
            RedisModule_DictReplaceC(r->groups, (void *)currentKey, currentKeyLen, newRangeSerie);
            RedisModule_DictIteratorReseekC(iter, ">", (void *)currentKey, currentKeyLen);
        }
    }
    RedisModule_DictIteratorStop(iter);
    return TSDB_OK;
}

int ResultSet_AddSerie(TS_ResultSet *r, Series *serie, const char *name) {
    const size_t namelen = strlen(name);
    int result = false;
    // If we're grouping by label then the rax associated values are TS_ResultSet
    // If we're grouping by name ( groupbylabel == NULL ) then the rax associated values are Series
    if (r->groupsType == GroupType_ResultSet) {
        char *labelValue = SeriesGetCStringLabelValue(serie, r->labelkey);
        if (labelValue != NULL) {
            const size_t labelLen = strlen(labelValue);
            int nokey;
            TS_ResultSet *labelGroup = (TS_ResultSet *)RedisModule_DictGetC(
                r->groups, (void *)labelValue, labelLen, &nokey);
            if (nokey) {
                labelGroup = ResultSet_Create();
                ResultSet_SetLabelKey(labelGroup, r->labelkey);
                ResultSet_SetLabelValue(labelGroup, labelValue);
                RedisModule_DictSetC(r->groups, (void *)labelValue, labelLen, labelGroup);
            }
            free(labelValue);
            result = ResultSet_AddSerie(labelGroup, serie, name);
        }
    } else {
        // If a serie with that name already exists we return
        int nokey;
        RedisModule_DictGetC(r->groups, (void *)name, namelen, &nokey);
        if (nokey) {
            RedisModule_DictSetC(r->groups, (void *)name, namelen, serie);
        }
        result = true;
    }
    return result;
}

void replyResultSet(RedisModuleCtx *ctx,
                    TS_ResultSet *r,
                    bool withlabels,
                    api_timestamp_t start_ts,
                    api_timestamp_t end_ts,
                    AggregationClass *aggObject,
                    int64_t time_delta,
                    long long maxResults,
                    bool rev) {
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(r->groups, "^", NULL, 0);
    if (r->groupsType == GroupType_ResultSet) {
        RedisModule_ReplyWithArray(ctx, RedisModule_DictSize(r->groups));
        TS_ResultSet *innerResultSet;
        while (RedisModule_DictNextC(iter, NULL, (void **)&innerResultSet) != NULL) {
            replyResultSet(ctx,
                           innerResultSet,
                           withlabels,
                           start_ts,
                           end_ts,
                           aggObject,
                           time_delta,
                           maxResults,
                           rev);
        }
    } else {
        Series *s;
        while (RedisModule_DictNextC(iter, NULL, (void **)&s) != NULL) {
            ReplySeriesArrayPos(
                ctx, s, withlabels, start_ts, end_ts, aggObject, time_delta, maxResults, rev);
        }
    }
    RedisModule_DictIteratorStop(iter);
}

void ResultSet_Free(TS_ResultSet *r) {
    if (r == NULL)
        return;
    if (r->groups) {
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(r->groups, "^", NULL, 0);
        if (r->groupsType == GroupType_ResultSet) {
            TS_ResultSet *innerResultSet;
            while (RedisModule_DictNextC(iter, NULL, (void **)&innerResultSet) != NULL) {
                ResultSet_Free(innerResultSet);
            }
        } else {
            Series *s;
            while (RedisModule_DictNextC(iter, NULL, (void **)&s) != NULL) {
                FreeTempSeries(s);
            }
        }
        RedisModule_DictIteratorStop(iter);
        RedisModule_FreeDict(NULL, r->groups);
    }
    if (r->labelkey)
        free(r->labelkey);
    if (r->labelvalue)
        free(r->labelvalue);
    free(r);
}
