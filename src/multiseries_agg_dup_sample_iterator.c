/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "abstract_iterator.h"
#include "multiseries_agg_dup_sample_iterator.h"
#include "generic_chunk.h"
#include <assert.h>

ChunkResult MultiSeriesAggDupSampleIterator_GetNext(
    struct AbstractMultiSeriesAggDupSampleIterator *iterator,
    Sample *sample) {
    MultiSeriesAggDupSampleIterator *iter = (MultiSeriesAggDupSampleIterator *)iterator;
    if (!iter->has_next_sample) {
        return CR_END;
    }

    *sample = iter->next_sample;
    Sample _sample;
    ChunkResult ret;
    while ((ret = iter->base.input->GetNext(iter->base.input, &_sample)) == CR_OK &&
           _sample.timestamp == iter->next_sample.timestamp) {
        assert(handleDuplicateSample(*iter->dp, _sample, sample) == CR_OK);
    }

    iter->next_sample = _sample;
    if (ret == CR_END) {
        iter->has_next_sample = false;
    }
    return CR_OK;
}

void MultiSeriesAggDupSampleIterator_Close(
    struct AbstractMultiSeriesAggDupSampleIterator *iterator) {
    iterator->input->Close(iterator->input);
    free(iterator);
}

MultiSeriesAggDupSampleIterator *MultiSeriesAggDupSampleIterator_New(
    AbstractMultiSeriesSampleIterator *input,
    DuplicatePolicy *dp) {
    MultiSeriesAggDupSampleIterator *newIter = malloc(sizeof(MultiSeriesAggDupSampleIterator));
    newIter->base.input = input;
    newIter->base.GetNext = MultiSeriesAggDupSampleIterator_GetNext;
    newIter->base.Close = MultiSeriesAggDupSampleIterator_Close;
    newIter->dp = dp;
    ChunkResult res = newIter->base.input->GetNext(newIter->base.input, &newIter->next_sample);
    if (res != CR_OK) {
        assert(res != CR_ERR); // we don't handle errors in this function currently
        newIter->has_next_sample = false;
    }
    newIter->has_next_sample = true;
    return newIter;
}
