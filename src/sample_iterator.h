/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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
