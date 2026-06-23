/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "consts.h"
#include "indexer.h"
#include "query_language.h"
#include "tsdb.h"

#ifndef REDISTIMESERIES_RESULTSET_H
#define REDISTIMESERIES_RESULTSET_H

// Parse a GROUPBY ... REDUCE <type> argument. Rejects TWA/FIRST/LAST/NONE
// ("Invalid reducer type") — only those rejections are why the streaming
// reducer never has to handle non-streamable types.
int parseMultiSeriesReduceArgs(RedisModuleCtx *ctx,
                               RedisModuleString *reducerstr,
                               ReducerArgs *reducerArgs);

// Shared with the streaming reducer (src/streaming_resultset.c): build the
// reduced-series label set (<label>=<value>, __reducer__, __source__) and
// free a temp output series.
Label *createReducedSeriesLabels(RedisModuleCtx *ctx,
                                 char *labelKey,
                                 char *labelValue,
                                 const ReducerArgs *gropuByReducerArgs);
void FreeTempSeries(Series *s);

#endif // REDISTIMESERIES_RESULTSET_H
