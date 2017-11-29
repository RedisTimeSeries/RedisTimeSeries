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

Sample *ChunkGetSampleArray(Chunk *chunk) {
    return (Sample *)chunk->samples;
}

Sample *ChunkGetSample(Chunk *chunk, int index) {
    return &ChunkGetSampleArray(chunk)[index];
}

timestamp_t ChunkGetLastTimestamp(Chunk *chunk) {
    if (chunk->num_samples == 0) {
        return -1;
    }
    return ChunkGetSample(chunk, chunk->num_samples - 1)->timestamp;
}
timestamp_t ChunkGetFirstTimestamp(Chunk *chunk) {
    if (chunk->num_samples == 0) {
        return -1;
    }
    return ChunkGetSample(chunk, 0)->timestamp;
}

int ChunkAddSample(Chunk *chunk, Sample sample) {
    if (IsChunkFull(chunk)){
        return 0;
    }

    if (ChunkNumOfSample(chunk) == 0) {
        //initilize base_timestamp
        chunk->base_timestamp = sample.timestamp;
    }

    ChunkGetSampleArray(chunk)[chunk->num_samples] = sample;
    chunk->num_samples++;

    return 1;
}

ChunkIterator NewChunkIterator(Chunk* chunk) {
    return (ChunkIterator){.chunk = chunk, .currentIndex = 0};
}

int ChunkItertorGetNext(ChunkIterator *iter, Sample* sample) {
    if (iter->currentIndex < iter->chunk->num_samples) {
        iter->currentIndex++;
        Sample *internalSample = ChunkGetSample(iter->chunk, iter->currentIndex-1);
        memcpy(sample, internalSample, sizeof(Sample));
        return 1;
    } else {
        return 0;
    }
}