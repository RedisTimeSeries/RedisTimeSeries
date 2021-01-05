/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "consts.h"
#include "tsdb.h"

#ifndef REDISTIMESERIES_RESULTSET_H
#define REDISTIMESERIES_RESULTSET_H

/* Incomplete structures for compiler checks but opaque access. */
typedef struct TS_ResultSet TS_ResultSet;

TS_ResultSet *createResultSet();

int groupbyLabel(TS_ResultSet *r, const char *label);

int setLabelKey(TS_ResultSet *r, const char *labelKey);

int setLabelValue(TS_ResultSet *r, const char *label);

int applyReducerToResultSet(TS_ResultSet *r, MultiSeriesReduceOp reducerOp);

int parseMultiSeriesReduceOp(const char *reducerstr, MultiSeriesReduceOp *reducerOp);

Label *createReducedSeriesLabels(TS_ResultSet *r, MultiSeriesReduceOp reducerOp);

int ApplySerieRangeIntoNewSerie(Series **dest,
                                Series *source,
                                api_timestamp_t start_ts,
                                api_timestamp_t end_ts,
                                AggregationClass *aggObject,
                                int64_t time_delta,
                                long long maxResults,
                                bool rev);

int applyRangeToResultSet(TS_ResultSet *r,
                          api_timestamp_t start_ts,
                          api_timestamp_t end_ts,
                          AggregationClass *aggObject,
                          int64_t time_delta,
                          long long maxResults,
                          bool rev);

int addSerieToResultSet(TS_ResultSet *r, Series *serie, const char *name);

void replyResultSet(RedisModuleCtx *ctx,
                    TS_ResultSet *r,
                    bool withlabels,
                    api_timestamp_t start_ts,
                    api_timestamp_t end_ts,
                    AggregationClass *aggObject,
                    int64_t time_delta,
                    long long maxResults,
                    bool rev);

void freeResultSet(TS_ResultSet *r);

#endif // REDISTIMESERIES_RESULTSET_H
