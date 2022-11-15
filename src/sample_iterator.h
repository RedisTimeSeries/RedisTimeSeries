/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */
#ifndef REDISTIMESERIES_SAMPLE_ITERATOR_H
#define REDISTIMESERIES_SAMPLE_ITERATOR_H

#include "abstract_iterator.h"

typedef struct SeriesSampleIterator
{
    AbstractSampleIterator base;
    EnrichedChunk *chunk;
    size_t cur_index;
} SeriesSampleIterator;

SeriesSampleIterator *SeriesSampleIterator_New(AbstractIterator *input);

#endif // REDISTIMESERIES_SAMPLE_ITERATOR_H
