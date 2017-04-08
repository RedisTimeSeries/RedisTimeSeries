#ifndef CHUNK_H
#define CHUNK_H

#include "chunk.h"
#include "consts.h"
#include <sys/types.h>

typedef struct Sample {
    timestamp_t timestamp;
    double data;
} Sample;

typedef struct Chunk
{
    timestamp_t base_timestamp;
    Sample * samples;
    short num_samples;
    short max_samples;
    struct Chunk *nextChunk;
    // struct Chunk *prevChunk;
} Chunk;

typedef struct ChunkIterator
{
    Chunk *chunk;
    int currentIndex;
} ChunkIterator;

Chunk * NewChunk(size_t sampleCount);
void FreeChunk(Chunk *chunk);

void ChunkAddSample(Chunk *chunk, Sample sample);
int IsChunkFull(Chunk *chunk);
int ChunkNumOfSample(Chunk *chunk);
Sample *ChunkGetLastSample(Chunk *chunk);
Sample *ChunkGetFirstSample(Chunk *chunk);

ChunkIterator NewChunkIterator(Chunk *chunk);
Sample * ChunkItertorGetNext(ChunkIterator *iterator);
#endif