/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */

#ifndef GORILLA_H
#define GORILLA_H

#include "consts.h"
#include "generic_chunk.h"

#include <stdbool.h>   // bool
#include <sys/types.h> // u_int_t

typedef u_int64_t timestamp_t;
typedef u_int64_t binary_t;
typedef u_int64_t globalbit_t;
typedef u_int8_t localbit_t;

typedef union
{
    double d;
    int64_t i;
    u_int64_t u;
} union64bits;

typedef struct CompressedChunk
{
    u_int64_t size;
    u_int64_t count;
    u_int64_t idx;

    union64bits baseValue;
    u_int64_t baseTimestamp;

    u_int64_t *data;

    u_int64_t prevTimestamp;
    int64_t prevTimestampDelta;

    union64bits prevValue;
    u_int8_t prevLeading;
    u_int8_t prevTrailing;
} CompressedChunk;

typedef struct Compressed_Iterator
{
    CompressedChunk *chunk;
    u_int64_t idx;
    u_int64_t count;

    // timestamp vars
    u_int64_t prevTS;
    int64_t prevDelta;

    // value vars
    union64bits prevValue;
    u_int8_t leading;
    u_int8_t trailing;
    u_int8_t blocksize;
} Compressed_Iterator;

ChunkResult Compressed_Append(CompressedChunk *chunk, u_int64_t timestamp, double value);
ChunkResult Compressed_ChunkIteratorGetNext(ChunkIter_t *iter, Sample *sample);

#endif
