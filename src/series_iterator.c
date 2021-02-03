/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "series_iterator.h"

#include "tsdb.h"

static int SeriesChunkIteratorOptions(SeriesIterator *iter) {
    int options = 0;
    if (iter->reverse) {
        options |= CHUNK_ITER_OP_REVERSE;
    }
    return options;
}

// Initiates SeriesIterator, find the correct chunk and initiate a ChunkIterator
int SeriesQuery(Series *series,
                SeriesIterator *iter,
                timestamp_t start_ts,
                timestamp_t end_ts,
                bool rev) {
    iter->series = series;
    iter->minTimestamp = start_ts;
    iter->maxTimestamp = end_ts;
    iter->reverse = rev;

    timestamp_t rax_key;
    ChunkFuncs *funcs = series->funcs;

    if (iter->reverse == false) {
        iter->DictGetNext = RedisModule_DictNextC;
        seriesEncodeTimestamp(&rax_key, iter->minTimestamp);
    } else {
        iter->DictGetNext = RedisModule_DictPrevC;
        seriesEncodeTimestamp(&rax_key, iter->maxTimestamp);
    }

    // get first chunk within query range
    iter->dictIter =
        RedisModule_DictIteratorStartC(series->chunks, "<=", &rax_key, sizeof(rax_key));
    if (!iter->DictGetNext(iter->dictIter, NULL, (void *)&iter->currentChunk)) {
        RedisModule_DictIteratorReseekC(iter->dictIter, "^", NULL, 0);
        iter->DictGetNext(iter->dictIter, NULL, (void *)&iter->currentChunk);
    }

    iter->chunkIterator = funcs->NewChunkIterator(
        iter->currentChunk, SeriesChunkIteratorOptions(iter), &iter->chunkIteratorFuncs);
    return TSDB_OK;
}

// this is an internal function that routes the next call to the appropriate chunk iterator function
static ChunkResult SeriesGetNext(SeriesIterator *iter, Sample *sample) {
    if (iter->reverse == false) {
        return iter->chunkIteratorFuncs.GetNext(iter->chunkIterator, sample);
    } else {
        if (iter->chunkIteratorFuncs.GetPrev == NULL) {
            return CR_ERR;
        }
        return iter->chunkIteratorFuncs.GetPrev(iter->chunkIterator, sample);
    }
}

void SeriesIteratorClose(SeriesIterator *iterator) {
    iterator->chunkIteratorFuncs.Free(iterator->chunkIterator);
    RedisModule_DictIteratorStop(iterator->dictIter);
}

// Fills sample from chunk. If all samples were extracted from the chunk, we
// move to the next chunk.
ChunkResult _seriesIteratorGetNext(SeriesIterator *iterator, Sample *currentSample) {
    ChunkResult res;
    ChunkFuncs *funcs = iterator->series->funcs;
    Chunk_t *currentChunk = iterator->currentChunk;

    while (true) {
        res = SeriesGetNext(iterator, currentSample);
        if (res == CR_END) { // Reached the end of the chunk
            if (!iterator->DictGetNext(iterator->dictIter, NULL, (void *)&currentChunk) ||
                funcs->GetFirstTimestamp(currentChunk) > iterator->maxTimestamp ||
                funcs->GetLastTimestamp(currentChunk) < iterator->minTimestamp) {
                return CR_END; // No more chunks or they out of range
            }
            iterator->chunkIteratorFuncs.Free(iterator->chunkIterator);
            iterator->chunkIterator = funcs->NewChunkIterator(
                currentChunk, SeriesChunkIteratorOptions(iterator), &iterator->chunkIteratorFuncs);
            if (SeriesGetNext(iterator, currentSample) != CR_OK) {
                return CR_END;
            }
        } else if (res == CR_ERR) {
            return CR_ERR;
        }

        // check timestamp is within range
        if (!iterator->reverse) {
            // forward range handling
            if (currentSample->timestamp < iterator->minTimestamp) {
                // didn't reach the starting point of the requested range
                continue;
            }
            if (currentSample->timestamp > iterator->maxTimestamp) {
                // reached the end of the requested range
                return CR_END;
            }
        } else {
            // reverse range handling
            if (currentSample->timestamp > iterator->maxTimestamp) {
                // didn't reach our starting range
                continue;
            }
            if (currentSample->timestamp < iterator->minTimestamp) {
                // didn't reach the starting point of the requested range
                return CR_END;
            }
        }
        return CR_OK;
    }
    return CR_OK;
}

ChunkResult SeriesIteratorGetNext(SeriesIterator *iterator, Sample *currentSample) {
    return _seriesIteratorGetNext(iterator, currentSample);
}
