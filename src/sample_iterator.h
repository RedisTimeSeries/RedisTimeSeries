/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#ifndef REDISTIMESERIES_SAMPLE_ITERATOR_H
#define REDISTIMESERIES_SAMPLE_ITERATOR_H

#include "abstract_iterator.h"

typedef struct SeriesSampleIterator
{
    AbstractSampleIterator base;
    DomainChunk *chunk;
    size_t cur_index;
} SeriesSampleIterator;

SeriesSampleIterator *SeriesSampleIterator_New(AbstractIterator *input);

#endif // REDISTIMESERIES_SAMPLE_ITERATOR_H
