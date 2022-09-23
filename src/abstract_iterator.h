/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "consts.h"
#include "generic_chunk.h"

#ifndef ABSTRACT_ITERATOR_H
#define ABSTRACT_ITERATOR_H

typedef struct AbstractIterator
{
    EnrichedChunk *(*GetNext)(struct AbstractIterator *iter);
    void (*Close)(struct AbstractIterator *iter);

    struct AbstractIterator *input;
} AbstractIterator;

typedef struct AbstractSampleIterator
{
    ChunkResult (*GetNext)(struct AbstractSampleIterator *iter, Sample *sample);
    void (*Close)(struct AbstractSampleIterator *iter);

    struct AbstractIterator *input;
} AbstractSampleIterator;

typedef struct AbstractMultiSeriesSampleIterator
{
    ChunkResult (*GetNext)(struct AbstractMultiSeriesSampleIterator *iter, Sample *sample);
    void (*Close)(struct AbstractMultiSeriesSampleIterator *iter);

    struct AbstractSampleIterator **input; // array of iterators
} AbstractMultiSeriesSampleIterator;

typedef struct AbstractMultiSeriesAggDupSampleIterator
{
    ChunkResult (*GetNext)(struct AbstractMultiSeriesAggDupSampleIterator *iter, Sample *sample);
    void (*Close)(struct AbstractMultiSeriesAggDupSampleIterator *iter);

    struct AbstractMultiSeriesSampleIterator *input;
} AbstractMultiSeriesAggDupSampleIterator;

#endif //ABSTRACT_ITERATOR_H
