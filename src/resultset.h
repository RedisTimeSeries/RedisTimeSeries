/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "consts.h"
#include "query_language.h"
#include "tsdb.h"

#ifndef REDISTIMESERIES_RESULTSET_H
#define REDISTIMESERIES_RESULTSET_H

/* Incomplete structures for compiler checks but opaque access. */
typedef struct TS_ResultSet TS_ResultSet;

TS_ResultSet *ResultSet_Create();

void ResultSet_GroupbyLabel(TS_ResultSet *r, const char *label);

void ResultSet_ApplyReducer(RedisModuleCtx *ctx,
                            TS_ResultSet *r,
                            const RangeArgs *args,
                            const ReducerArgs *gropuByReducerArgs);

int parseMultiSeriesReduceArgs(RedisModuleCtx *ctx,
                               RedisModuleString *reducerstr,
                               ReducerArgs *reducerArgs);

bool ResultSet_AddSerie(TS_ResultSet *r, Series *serie, const char *name);

void replyResultSet(RedisModuleCtx *ctx,
                    TS_ResultSet *r,
                    bool withlabels,
                    RedisModuleString *limitLabels[],
                    ushort limitLabelsSize,
                    RangeArgs *args,
                    bool rev);

void ResultSet_Free(TS_ResultSet *r);

void MultiSerieReduce(Series *dest,
                      Series **series,
                      size_t n_series,
                      const ReducerArgs *gropuByReducerArgs,
                      const RangeArgs *args);

#endif // REDISTIMESERIES_RESULTSET_H
