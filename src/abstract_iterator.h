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
    ChunkResult (*GetNext)(struct AbstractIterator *iter, Sample *currentSample);
    bool (*GetNextBoundaryAggValue)(struct AbstractIterator *iter,
                                    const timestamp_t timestampBoundaryStart,
                                    const timestamp_t timestampBoundaryEnd,
                                    int aggType,
                                    Sample *sample);
    void (*Close)(struct AbstractIterator *iter);

    struct AbstractIterator *input;
} AbstractIterator;

#endif // ABSTRACT_ITERATOR_H
