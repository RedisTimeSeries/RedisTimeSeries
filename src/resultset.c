/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

// The buffered grouped-reducer (TS_ResultSet / GroupList / MultiSerieReduce)
// was replaced by the streaming reducer in src/streaming_resultset.c — it
// holds at most one series's chunks at a time instead of buffering every
// matched series, which is what keeps wide GROUPBY queries from OOMing a
// Flex shard (see bench-results/07-flex-1g.md). The two helpers below are the
// only pieces still shared: the streaming reducer reuses them to build the
// reduced-series labels and to free its temp output series.

#include "resultset.h"

#include "indexer.h"
#include "reply.h"
#include "tsdb.h"

#include "RedisModulesSDK/redismodule.h"
#include "utils/arr.h"
#include "rmutil/alloc.h"

#include <string.h>

void FreeTempSeries(Series *s) {
    if (!s)
        return;
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
    if (s->srcKey) {
        array_free((RedisModuleString **)s->srcKey);
    }
    free(s);
}

Label *createReducedSeriesLabels(RedisModuleCtx *ctx,
                                 char *labelKey,
                                 char *labelValue,
                                 const ReducerArgs *groupByReducerArgs) {
    // Labels:
    // <label>=<groupbyvalue>
    // __reducer__=<reducer>
    // __source__=key1,key2,key3
    const char *reducer_str = AggTypeEnumToStringLowerCase(groupByReducerArgs->agg_type);

    Label *labels = calloc(3, sizeof(Label));
    labels[0].key = RedisModule_CreateStringPrintf(NULL, "%s", labelKey);
    labels[0].value = RedisModule_CreateStringPrintf(NULL, "%s", labelValue);
    labels[1].key = RedisModule_CreateStringPrintf(NULL, "__reducer__");
    labels[1].value = RedisModule_CreateString(NULL, reducer_str, strlen(reducer_str));
    labels[2].key = RedisModule_CreateStringPrintf(NULL, "__source__");
    labels[2].value = RedisModule_CreateString(NULL, "", 0);
    return labels;
}
