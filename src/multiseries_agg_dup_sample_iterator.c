/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "abstract_iterator.h"
#include "multiseries_agg_dup_sample_iterator.h"
#include "generic_chunk.h"
#include "query_language.h"
#include <math.h>
#include <assert.h>

ChunkResult MultiSeriesAggDupSampleIterator_GetNext(
    struct AbstractMultiSeriesAggDupSampleIterator *iterator,
    Sample *sample) {
    MultiSeriesAggDupSampleIterator *iter = (MultiSeriesAggDupSampleIterator *)iterator;
    void *aggContext = iter->aggregationContext;

    if (!iter->has_next_sample) {
        return CR_END;
    }

    bool is_valid = iter->aggregation->isValueValid(iter->next_sample.value);
    if (is_valid) {
        iter->aggregation->appendValue(
            aggContext, iter->next_sample.value, iter->next_sample.timestamp);
    }

    bool any_valid = is_valid;
    Sample _sample;
    ChunkResult ret;
    while ((ret = iter->base.input->GetNext(iter->base.input, &_sample)) == CR_OK &&
           _sample.timestamp == iter->next_sample.timestamp) {
        bool _is_valid = iter->aggregation->isValueValid(_sample.value);
        if (_is_valid) {
            iter->aggregation->appendValue(aggContext, _sample.value, _sample.timestamp);
        }
        any_valid = any_valid || _is_valid;
    }

    sample->timestamp = iter->next_sample.timestamp;
    if (likely(any_valid)) {
        iter->aggregation->finalize(aggContext, &sample->value);
    } else {
        iter->aggregation->finalizeEmpty(aggContext, &sample->value);
    }
    iter->aggregation->resetContext(aggContext);
    iter->next_sample = _sample;
    if (ret == CR_END) {
        iter->has_next_sample = false;
    }
    return CR_OK;
}

void MultiSeriesAggDupSampleIterator_Close(
    struct AbstractMultiSeriesAggDupSampleIterator *iterator) {
    MultiSeriesAggDupSampleIterator *self = (MultiSeriesAggDupSampleIterator *)iterator;
    iterator->input->Close(iterator->input);
    self->aggregation->freeContext(self->aggregationContext);
    free(iterator);
}

MultiSeriesAggDupSampleIterator *MultiSeriesAggDupSampleIterator_New(
    AbstractMultiSeriesSampleIterator *input,
    const ReducerArgs *reducerArgs) {
    MultiSeriesAggDupSampleIterator *newIter = malloc(sizeof(MultiSeriesAggDupSampleIterator));
    newIter->base.input = input;
    newIter->base.GetNext = MultiSeriesAggDupSampleIterator_GetNext;
    newIter->base.Close = MultiSeriesAggDupSampleIterator_Close;
    newIter->aggregation = reducerArgs->aggregationClass;
    newIter->aggregationContext = newIter->aggregation->createContext(DC);
    ChunkResult res = newIter->base.input->GetNext(newIter->base.input, &newIter->next_sample);
    newIter->has_next_sample = true;
    if (res != CR_OK) {
        assert(res != CR_ERR); // we don't handle errors in this function currently
        newIter->has_next_sample = false;
    }
    return newIter;
}
