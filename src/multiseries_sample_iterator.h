/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#ifndef REDISTIMESERIES_MULTISERIES_SAMPLE_ITERATOR_H
#define REDISTIMESERIES_MULTISERIES_SAMPLE_ITERATOR_H

#include "abstract_iterator.h"
#include "utils/heap.h"

typedef struct MultiSeriesSampleIterator
{
    AbstractMultiSeriesSampleIterator base;
    size_t n_series;
    heap_t *samples_heap;
} MultiSeriesSampleIterator;

MultiSeriesSampleIterator *MultiSeriesSampleIterator_New(AbstractSampleIterator **iters,
                                                         size_t n_series,
                                                         bool reverse);

#endif // REDISTIMESERIES_MULTISERIES_SAMPLE_ITERATOR_H
