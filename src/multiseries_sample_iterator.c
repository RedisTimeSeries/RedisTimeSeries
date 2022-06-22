/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "abstract_iterator.h"
#include "multiseries_sample_iterator.h"
#include "consts.h"
#include "assert.h"

typedef struct
{
    Sample sample;
    AbstractSampleIterator *iter; // The iterator this sample belongs to
} MSSample;

// implements min heap
static int heap_cmp_func(const void *sample1, const void *sample2, __unused const void *udata) {
    return (((MSSample *)sample1)->sample.timestamp < ((MSSample *)sample2)->sample.timestamp) ? 1
                                                                                               : -1;
}

// implements max heap
static int heap_cmp_func_reverse(const void *sample1,
                                 const void *sample2,
                                 __unused const void *udata) {
    return (((MSSample *)sample1)->sample.timestamp > ((MSSample *)sample2)->sample.timestamp) ? 1
                                                                                               : -1;
}

void MultiSeriesSampleIterator_Close(struct AbstractMultiSeriesSampleIterator *iterator) {
    MultiSeriesSampleIterator *iter = (MultiSeriesSampleIterator *)iterator;
    for (size_t i = 0; i < iter->n_series; ++i) {
        iter->base.input[i]->Close(iter->base.input[i]);
    }
    free(iter->base.input);
    heap_clear_free_items(iter->samples_heap, free);
    heap_free(iter->samples_heap);
    free(iterator);
}

// Return the smallest timestamp sample and insert new sample from the same iterator if exist
ChunkResult MultiSeriesSampleIterator_GetNext(struct AbstractMultiSeriesSampleIterator *base,
                                              Sample *sample) {
    MultiSeriesSampleIterator *iter = (MultiSeriesSampleIterator *)base;
    MSSample *hsample = heap_poll(iter->samples_heap);

    if (!hsample) {
        return CR_END;
    }

    *sample = hsample->sample;
    if (hsample->iter->GetNext(hsample->iter, &hsample->sample) == CR_OK) {
        heap_offer(&iter->samples_heap, hsample);
    } else { // the series is exhausted free it's sample
        free(hsample);
    }

    return CR_OK;
}

MultiSeriesSampleIterator *MultiSeriesSampleIterator_New(AbstractSampleIterator **iters,
                                                         size_t n_series,
                                                         bool reverse) {
    MultiSeriesSampleIterator *newIter = malloc(sizeof(MultiSeriesSampleIterator));
    newIter->base.input = malloc(sizeof(AbstractSampleIterator *) * n_series);
    memcpy(newIter->base.input, iters, sizeof(AbstractSampleIterator *) * n_series);
    newIter->base.GetNext = MultiSeriesSampleIterator_GetNext;
    newIter->base.Close = MultiSeriesSampleIterator_Close;
    newIter->n_series = n_series;
    newIter->samples_heap = heap_new(reverse ? heap_cmp_func_reverse : heap_cmp_func, NULL);
    for (size_t i = 0; i < newIter->n_series; ++i) {
        AbstractSampleIterator *sample_iter = newIter->base.input[i];
        MSSample *sample = malloc(sizeof(MSSample));
        if (sample_iter->GetNext(sample_iter, &sample->sample) == CR_OK) {
            sample->iter = sample_iter;
            assert(heap_offer(&newIter->samples_heap, sample) == 0);
        } else {
            free(sample);
        }
    }
    return newIter;
}
