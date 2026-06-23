/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef REDISTIMESERIES_STREAMING_RESULTSET_H
#define REDISTIMESERIES_STREAMING_RESULTSET_H

#include "consts.h"
#include "query_language.h"
#include "tsdb.h"

#include "RedisModulesSDK/redismodule.h"

#include <stdbool.h>

/* Streaming reducer for TS.MRANGE GROUPBY.
 *
 * Unlike the buffered ResultSet (src/resultset.c) which stores raw Series
 * pointers and reduces in one pass via MultiSerieReduce, this variant
 * maintains a per-group dict of AggregationClass contexts keyed by
 * timestamp. Each incoming Series is fed sample-by-sample into those
 * contexts and can be closed + released immediately after FeedSeries
 * returns.
 *
 * Memory benefit: at any moment we hold at most one Series's chunks plus
 * the accumulator state, instead of N Series's chunks. Critical under Flex
 * because the buffered ResultSet's chunk pointers are invisible to
 * bigredis-max-ram — see bench-results/07-flex-1g.md scenario T3 for the
 * crash this prevents.
 *
 * Not all reducers are streamable. TWA / FIRST / LAST / NONE are rejected
 * up front by parseMultiSeriesReduceArgs (src/query_language.c), so a
 * streaming resultset is only ever created for reducers it can handle —
 * there is no runtime fallback path.
 */

typedef struct TS_StreamingResultSet TS_StreamingResultSet;

/* Build a streaming resultset for the given groupby label + reducer. Stores
 * its own copy of `reducer` and `rangeArgs` so the caller can pass stack
 * pointers. Returns NULL on a NULL arg or an unknown/unsupported reducer; OOM
 * aborts via rmutil/alloc.h (it does not surface as a NULL return). */
TS_StreamingResultSet *StreamingResultSet_Create(const char *groupbyLabel,
                                                 const ReducerArgs *reducer,
                                                 const RangeArgs *rangeArgs);

/* Walks `series`'s samples and feeds them into the per-group, per-timestamp
 * accumulator. The series is NOT retained; after this returns the caller
 * may CloseKey + free the underlying redis key. If the series lacks the
 * groupby label it is silently skipped (matches ResultSet_AddSerie). */
void StreamingResultSet_FeedSeries(TS_StreamingResultSet *r, Series *series);

/* Finalize all groups, emit the reply on `ctx`, then free the resultset.
 * Caller must NOT call StreamingResultSet_Free afterwards. */
void StreamingResultSet_FinalizeAndReply(RedisModuleCtx *ctx,
                                         TS_StreamingResultSet *r,
                                         bool withLabels,
                                         RedisModuleString *limitLabels[],
                                         ushort limitLabelsSize,
                                         bool reverse);

/* Free a resultset that was created but never finalized (e.g. error path).
 * Safe to call on NULL. */
void StreamingResultSet_Free(TS_StreamingResultSet *r);

#endif // REDISTIMESERIES_STREAMING_RESULTSET_H
