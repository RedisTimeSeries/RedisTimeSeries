/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */

#ifndef GORILLA_H
#define GORILLA_H

#include "consts.h"
#include "generic_chunk.h"

#include <stdbool.h> // bool
#include <stdint.h>

typedef uint64_t timestamp_t;
typedef uint64_t binary_t;
typedef uint64_t globalbit_t;
typedef uint8_t localbit_t;

typedef union
{
    double d;
    int64_t i;
    uint64_t u;
} union64bits;

typedef struct CompressedChunk
{
    uint64_t size;
    uint64_t count;
    uint64_t idx;

    union64bits baseValue;
    uint64_t baseTimestamp;

    uint64_t *data;

    uint64_t prevTimestamp;
    int64_t prevTimestampDelta;

    union64bits prevValue;
    uint8_t prevLeading;
    uint8_t prevTrailing;
} CompressedChunk;

typedef struct Compressed_Iterator
{
    CompressedChunk *chunk;
    uint64_t idx;
    uint64_t count;

    // timestamp vars
    uint64_t prevTS;
    int64_t prevDelta;

    // value vars
    union64bits prevValue;
    uint8_t leading;
    uint8_t trailing;
    uint8_t blocksize;
} Compressed_Iterator;

ChunkResult Compressed_Append(CompressedChunk *chunk, uint64_t timestamp, double value);
ChunkResult Compressed_ChunkIteratorGetNext(ChunkIter_t *iter, Sample *sample);

#endif
