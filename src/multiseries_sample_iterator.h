/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
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
