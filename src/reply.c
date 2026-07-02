/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "reply.h"

#include "enriched_chunk.h"
#include "query_language.h"
#include "sample_iterator.h"
#include "series_iterator.h"
#include "tsdb.h"

#include "dragonbox/dragonbox.h"
#include "RedisModulesSDK/redismodule.h"
#include "utils/arr.h"
#include "rmutil/alloc.h"

#include <math.h> // NAN

// MRANGE reply structure element counts
#define MRANGE_RESP2_ENTRY_ELEMENTS 3   // [name, labels, samples]
#define MRANGE_RESP3_VALUE_ELEMENTS 3   // [labels, aggregators, samples]
#define MRANGE_RESP3_REDUCED_ELEMENTS 4 // [labels, reducers, sources, samples]

// double string presentation requires 15 digit integers +
// '.' + "e+" or "e-" + 3 digits of exponent
#define MAX_VAL_LEN 24
int ReplyWithDoubleOrString(RedisModuleCtx *ctx, double d) {
    if (_is_resp3(ctx)) {
        return RedisModule_ReplyWithDouble(ctx, d);
    } else {
        char buf[MAX_VAL_LEN + 1];
        dragonbox_double_to_chars(d, buf);
        return RedisModule_ReplyWithSimpleString(ctx, buf);
    }
}

void ReplySetMapOrArrayLength(RedisModuleCtx *ctx, long len, bool divide_by_two) {
    if (_ReplyMap(ctx)) {
        RedisModule_ReplySetMapLength(ctx, divide_by_two ? len / 2 : len);
    } else {
        RedisModule_ReplySetArrayLength(ctx, len);
    }
}

void ReplyWithMapOrArray(RedisModuleCtx *ctx, long len, bool divide_by_two) {
    if (_ReplyMap(ctx)) {
        RedisModule_ReplyWithMap(ctx, divide_by_two ? len / 2 : len);
    } else {
        RedisModule_ReplyWithArray(ctx, len);
    }
}

void ReplySetSetOrArrayLength(RedisModuleCtx *ctx, long len) {
    if (_ReplySet(ctx)) {
        RedisModule_ReplySetSetLength(ctx, len);
    } else {
        RedisModule_ReplySetArrayLength(ctx, len);
    }
}

void ReplyWithSetOrArray(RedisModuleCtx *ctx, long len) {
    if (_ReplySet(ctx)) {
        RedisModule_ReplyWithSet(ctx, len);
    } else {
        RedisModule_ReplyWithArray(ctx, len);
    }
}

int ReplySeriesArrayPos(RedisModuleCtx *ctx,
                        Series *s,
                        bool withlabels,
                        RedisModuleString *limitLabels[],
                        uint16_t limitLabelsSize,
                        const RangeArgs *args,
                        bool rev,
                        bool print_reduced) {
    if (!_ReplyMap(ctx)) {
        RedisModule_ReplyWithArray(ctx, MRANGE_RESP2_ENTRY_ELEMENTS);
    }
    RedisModule_ReplyWithString(ctx, s->keyName);
    if (_ReplyMap(ctx)) {
        RedisModule_ReplyWithArray(
            ctx, print_reduced ? MRANGE_RESP3_REDUCED_ELEMENTS : MRANGE_RESP3_VALUE_ELEMENTS);
    }
    if (withlabels) {
        if (_ReplyMap(ctx) && print_reduced) {
            s->labelsCount -= 2; // Inorder to not print the __reducer__ etc
        }
        ReplyWithSeriesLabels(ctx, s);
        if (_ReplyMap(ctx) && print_reduced) {
            s->labelsCount += 2;
        }
    } else if (limitLabelsSize > 0) {
        ReplyWithSeriesLabelsWithLimit(ctx, s, limitLabels, limitLabelsSize);
    } else {
        ReplyWithMapOrArray(ctx, 0, false);
    }

    if (_ReplyMap(ctx)) {
        if (print_reduced) {
            // reply reducers
            RedisModule_ReplyWithMap(ctx, 1);
            RedisModule_ReplyWithCString(ctx, "reducers");
            RedisModule_ReplyWithArray(ctx, 1);
            RedisModule_ReplyWithString(ctx, s->labels[s->labelsCount - 2].value);

            // reply sources
            RedisModule_ReplyWithMap(ctx, 1);
            RedisModule_ReplyWithCString(ctx, "sources");
            RedisModule_ReplyWithArray(ctx, array_len((RedisModuleString **)s->srcKey));
            for (uint32_t i = 0; i < array_len((RedisModuleString **)s->srcKey); i++) {
                RedisModule_ReplyWithString(ctx, ((RedisModuleString **)s->srcKey)[i]);
            }
        } else {
            // reply aggregators
            RedisModule_ReplyWithMap(ctx, 1);
            RedisModule_ReplyWithCString(ctx, "aggregators");
            if (args->aggregationArgs.numClasses == 0) {
                RedisModule_ReplyWithArray(ctx, 0);
            } else {
                RedisModule_ReplyWithArray(ctx, args->aggregationArgs.numClasses);
                for (size_t i = 0; i < args->aggregationArgs.numClasses; i++) {
                    RedisModule_ReplyWithCString(
                        ctx, AggTypeEnumToStringLowerCase(args->aggregationArgs.classes[i]->type));
                }
            }
        }
    }
    ReplySeriesRange(ctx, s, args, rev);
    return REDISMODULE_OK;
}

int ReplySeriesRange(RedisModuleCtx *ctx, Series *series, const RangeArgs *args, bool reverse) {
    long long arraylen = 0;
    long long _count = LLONG_MAX;
    unsigned int n;
    if (args->count != -1) {
        _count = args->count;
    }

    AbstractIterator *iter = SeriesQuery(series, args, reverse, true);
    EnrichedChunk *enrichedChunk;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    while ((arraylen < _count) && (enrichedChunk = iter->GetNext(iter))) {
        n = (unsigned int)min(_count - arraylen, enrichedChunk->samples.num_samples);
        size_t vps = enrichedChunk->samples.values_per_sample;
        for (size_t i = 0; i < n; ++i) {
            if (vps > 1) {
                ReplyWithMultiAggSample(ctx,
                                        enrichedChunk->samples.timestamps[i],
                                        Samples_values_row_ptr(&enrichedChunk->samples, i),
                                        vps);
            } else {
                ReplyWithSample(ctx,
                                enrichedChunk->samples.timestamps[i],
                                Samples_value_at(&enrichedChunk->samples, i, 0));
            }
        }
        arraylen += n;
    }
    iter->Close(iter);

    RedisModule_ReplySetArrayLength(ctx, arraylen);
    return REDISMODULE_OK;
}

void ReplyWithSeriesLabelsWithLimit(RedisModuleCtx *ctx,
                                    const Series *series,
                                    RedisModuleString **limitLabels,
                                    uint16_t limitLabelsSize) {
    const char **limitLabelsStr = malloc(sizeof(char *) * limitLabelsSize);
    for (int i = 0; i < limitLabelsSize; i++) {
        limitLabelsStr[i] = RedisModule_StringPtrLen(limitLabels[i], NULL);
    }
    ReplyWithSeriesLabelsWithLimitC(ctx, series, limitLabelsStr, limitLabelsSize);
    free(limitLabelsStr);
}

void ReplyWithSeriesLabelsWithLimitC(RedisModuleCtx *ctx,
                                     const Series *series,
                                     const char **limitLabels,
                                     uint16_t limitLabelsSize) {
    ReplyWithMapOrArray(ctx, limitLabelsSize, false);
    for (int i = 0; i < limitLabelsSize; i++) {
        bool found = false;
        for (int j = 0; j < series->labelsCount; ++j) {
            const char *key = RedisModule_StringPtrLen(series->labels[j].key, NULL);
            if (strcasecmp(key, limitLabels[i]) == 0) {
                if (!_ReplyMap(ctx)) {
                    RedisModule_ReplyWithArray(ctx, 2);
                }
                RedisModule_ReplyWithString(ctx, series->labels[j].key);
                RedisModule_ReplyWithString(ctx, series->labels[j].value);
                found = true;
                break;
            }
        }
        if (!found) {
            if (!_ReplyMap(ctx)) {
                RedisModule_ReplyWithArray(ctx, 2);
            }
            RedisModule_ReplyWithCString(ctx, limitLabels[i]);
            RedisModule_ReplyWithNull(ctx);
        }
    }
}

void ReplyWithSeriesLabels(RedisModuleCtx *ctx, const Series *series) {
    ReplyWithMapOrArray(ctx, series->labelsCount, false);
    for (int i = 0; i < series->labelsCount; i++) {
        if (!_ReplyMap(ctx)) {
            RedisModule_ReplyWithArray(ctx, 2);
        }
        RedisModule_ReplyWithString(ctx, series->labels[i].key);
        if (likely(series->labels[i].value != NULL))
            RedisModule_ReplyWithString(ctx, series->labels[i].value);
        else
            RedisModule_ReplyWithNull(ctx);
    }
}

void ReplyWithSample(RedisModuleCtx *ctx, uint64_t timestamp, double value) {
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithLongLong(ctx, timestamp);
    ReplyWithDoubleOrString(ctx, value);
}

void ReplyWithMultiAggSample(RedisModuleCtx *ctx,
                             uint64_t timestamp,
                             double *values,
                             size_t num_values) {
    RedisModule_ReplyWithArray(ctx, 1 + num_values);
    RedisModule_ReplyWithLongLong(ctx, timestamp);
    for (size_t i = 0; i < num_values; i++) {
        ReplyWithDoubleOrString(ctx, values[i]);
    }
}

int ReplyMultiAggSeriesGroup(RedisModuleCtx *ctx,
                             Series **group,
                             size_t numAggTypes,
                             bool withLabels,
                             RedisModuleString *limitLabels[],
                             uint16_t limitLabelsSize,
                             const RangeArgs *args,
                             bool rev) {
    if (!_ReplyMap(ctx))
        RedisModule_ReplyWithArray(ctx, MRANGE_RESP2_ENTRY_ELEMENTS);
    RedisModule_ReplyWithString(ctx, group[0]->keyName);
    if (_ReplyMap(ctx))
        RedisModule_ReplyWithArray(ctx, MRANGE_RESP3_VALUE_ELEMENTS);

    if (withLabels)
        ReplyWithSeriesLabels(ctx, group[0]);
    else if (limitLabelsSize > 0)
        ReplyWithSeriesLabelsWithLimit(ctx, group[0], limitLabels, limitLabelsSize);
    else
        ReplyWithMapOrArray(ctx, 0, false);

    if (_ReplyMap(ctx)) {
        RedisModule_ReplyWithMap(ctx, 1);
        RedisModule_ReplyWithCString(ctx, "aggregators");
        if (args->aggregationArgs.numClasses == 0) {
            RedisModule_ReplyWithArray(ctx, 0);
        } else {
            RedisModule_ReplyWithArray(ctx, args->aggregationArgs.numClasses);
            for (size_t i = 0; i < args->aggregationArgs.numClasses; i++) {
                RedisModule_ReplyWithCString(
                    ctx, AggTypeEnumToStringLowerCase(args->aggregationArgs.classes[i]->type));
            }
        }
    }

    RangeArgs rawArgs = *args;
    rawArgs.filterByValueArgs.hasValue = false;
    rawArgs.filterByTSArgs.hasValue = false;

    // All series share the same timestamps — advance N sample iterators in lockstep and emit
    // directly.
    AbstractSampleIterator *iters[TS_AGG_TYPES_MAX];
    for (size_t aggIdx = 0; aggIdx < numAggTypes; aggIdx++)
        iters[aggIdx] = (AbstractSampleIterator *)SeriesSampleIterator_New(
            SeriesQuery(group[aggIdx], &rawArgs, rev, true));

    long long limit = (args->count != -1) ? args->count : LLONG_MAX;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    long long emitted = 0;
    Sample lead;
    double row[TS_AGG_TYPES_MAX];
    while (emitted < limit && iters[0]->GetNext(iters[0], &lead) == CR_OK) {
        row[0] = lead.value;
        for (size_t aggIdx = 1; aggIdx < numAggTypes; aggIdx++) {
            Sample s;
            iters[aggIdx]->GetNext(iters[aggIdx], &s);
            row[aggIdx] = s.value;
        }
        ReplyWithMultiAggSample(ctx, lead.timestamp, row, numAggTypes);
        emitted++;
    }
    RedisModule_ReplySetArrayLength(ctx, emitted);

    for (size_t aggIdx = 0; aggIdx < numAggTypes; aggIdx++)
        iters[aggIdx]->Close(iters[aggIdx]);

    return REDISMODULE_OK;
}

void ReplyWithPivotSample(RedisModuleCtx *ctx,
                          uint64_t timestamp,
                          const double *values,
                          size_t num_values) {
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithLongLong(ctx, timestamp);
    RedisModule_ReplyWithArray(ctx, num_values);
    for (size_t i = 0; i < num_values; i++) {
        ReplyWithDoubleOrString(ctx, values[i]);
    }
}

int ReplySeriesNRange(RedisModuleCtx *ctx,
                      AbstractIterator **iters,
                      size_t num_keys,
                      const size_t *aggs_per_key,
                      long long count,
                      bool reverse) {
    const long long limit = (count < 0) ? LLONG_MAX : count;

    // Per-key chunk state for the k-way merge.
    EnrichedChunk **key_chunks =
        malloc(num_keys * sizeof(*key_chunks)); // current data batch per key
    size_t *chunk_pos =
        malloc(num_keys * sizeof(*chunk_pos)); // position inside current batch per key
    bool *key_active = malloc(num_keys * sizeof(*key_active)); // whether key still has data

    // Pre-compute where each key's values start in the flat row buffer, and the total row width.
    size_t *key_row_offset = malloc(num_keys * sizeof(*key_row_offset));
    size_t row_width = 0;
    for (size_t i = 0; i < num_keys; i++) {
        key_row_offset[i] = row_width;
        row_width += aggs_per_key[i];
    }
    double *row_buf = malloc(row_width * sizeof(*row_buf)); // scratch buffer for one output row

    size_t active_count = 0;
    for (size_t i = 0; i < num_keys; i++) {
        key_chunks[i] = iters[i]->GetNext(iters[i]);
        chunk_pos[i] = 0;
        key_active[i] = (key_chunks[i] != NULL && key_chunks[i]->samples.num_samples > 0);
        if (key_active[i])
            active_count++;
    }

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    long long emitted = 0;
    while (active_count > 0 && emitted < limit) {
        // Next output timestamp: smallest front ts (largest when reverse).
        // Linear over the fronts; a heap could replace it if num_keys grows large.
        bool found = false;
        timestamp_t target = 0;
        for (size_t i = 0; i < num_keys; i++) {
            if (!key_active[i])
                continue;
            timestamp_t ts = key_chunks[i]->samples.timestamps[chunk_pos[i]];
            if (!found || (reverse ? ts > target : ts < target)) {
                target = ts;
                found = true;
            }
        }

        // Build the pivoted row. A missing sample for a key at this timestamp is reported as
        // NaN -- indistinguishable from a key that has a real NaN sample. This conflation is
        // intended (see TS.NRANGE/TS.NREVRANGE docs); disambiguating needs a separate null cell.
        for (size_t i = 0; i < num_keys; i++) {
            double *slot = row_buf + key_row_offset[i];
            if (key_active[i] && key_chunks[i]->samples.timestamps[chunk_pos[i]] == target) {
                for (size_t j = 0; j < aggs_per_key[i]; j++) {
                    slot[j] = Samples_value_at(&key_chunks[i]->samples, chunk_pos[i], j);
                }
                chunk_pos[i]++;
                if (chunk_pos[i] >= key_chunks[i]->samples.num_samples) {
                    key_chunks[i] = iters[i]->GetNext(iters[i]);
                    if (!key_chunks[i] || key_chunks[i]->samples.num_samples == 0) {
                        key_active[i] = false;
                        active_count--;
                    } else {
                        chunk_pos[i] = 0;
                    }
                }
            } else {
                for (size_t j = 0; j < aggs_per_key[i]; j++) {
                    slot[j] = NAN;
                }
            }
        }

        ReplyWithPivotSample(ctx, target, row_buf, row_width);
        emitted++;
    }

    RedisModule_ReplySetArrayLength(ctx, emitted);

    for (size_t i = 0; i < num_keys; i++) {
        iters[i]->Close(iters[i]);
    }
    free(key_chunks);
    free(chunk_pos);
    free(key_active);
    free(key_row_offset);
    free(row_buf);
    return REDISMODULE_OK;
}

void ReplyWithSeriesLastDatapoint(RedisModuleCtx *ctx, const Series *series) {
    if (SeriesGetNumSamples(series) == 0) {
        RedisModule_ReplyWithArray(ctx, 0);
    } else {
        ReplyWithSample(ctx, series->lastTimestamp, series->lastValue);
    }
}
