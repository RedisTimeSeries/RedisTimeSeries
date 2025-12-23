/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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
