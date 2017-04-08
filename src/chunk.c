#include "chunk.h"
#include <stdlib.h>

Chunk * NewChunk(size_t sampleCount)
{
    Chunk *newChunk = (Chunk *)malloc(sizeof(Chunk));
    newChunk->num_samples = 0;
    newChunk->max_samples = sampleCount;
    newChunk->nextChunk = NULL;
    newChunk->samples = malloc(sizeof(Sample)*sampleCount);

    return newChunk;
}

void FreeChunk(Chunk *chunk) {
    free(chunk->samples);
    free(chunk);
}

int IsChunkFull(Chunk *chunk) {
    return chunk->num_samples == chunk->max_samples;
}

int ChunkNumOfSample(Chunk *chunk) {
    return chunk->num_samples;
}

Sample *ChunkGetLastSample(Chunk *chunk) {
    if (chunk->num_samples == 0) {
        return NULL;
    }
    return &chunk->samples[chunk->num_samples - 1];
}
Sample *ChunkGetFirstSample(Chunk *chunk) {
    if (chunk->num_samples == 0) {
        return NULL;
    }
    return &chunk->samples[0];
}

void ChunkAddSample(Chunk *chunk, Sample sample) {
    if (ChunkNumOfSample(chunk) == 0) {
        chunk->base_timestamp = sample.timestamp;
    }
    chunk->samples[chunk->num_samples] = sample;
    chunk->num_samples++;
}

ChunkIterator NewChunkIterator(Chunk* chunk) {
    return (ChunkIterator){.chunk = chunk, .currentIndex = 0};
}

Sample * ChunkItertorGetNext(ChunkIterator *iter) {
    if (iter->currentIndex < iter->chunk->num_samples) {
        iter->currentIndex++;
        return &iter->chunk->samples[iter->currentIndex-1];
    } else {
        return NULL;
    }
}