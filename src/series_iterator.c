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
                bool rev,
                AggregationClass *aggregation,
                int64_t time_delta) {
    iter->series = series;
    iter->minTimestamp = start_ts;
    iter->maxTimestamp = end_ts;
    iter->reverse = rev;
    iter->aggregation = NULL;
    iter->aggregationContext = NULL;
    iter->aggregationTimeDelta = time_delta;
    iter->aggregationIsFirstSample = TRUE;
    iter->aggregationIsFinalized = FALSE;

    timestamp_t rax_key;
    ChunkFuncs *funcs = series->funcs;

    if (aggregation) {
        iter->aggregation = aggregation;
        iter->aggregationContext = iter->aggregation->createContext();
    }

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

    if (aggregation) {
        timestamp_t init_ts = (rev == false) ? series->funcs->GetFirstTimestamp(iter->currentChunk)
                                             : series->funcs->GetLastTimestamp(iter->currentChunk);
        iter->aggregationLastTimestamp = init_ts - (init_ts % time_delta);
    }
    return TSDB_OK;
}

// this is an internal function that routes the next call to the appropriate chunk iterator function
static inline ChunkResult SeriesGetNext(SeriesIterator *iter, Sample *sample) {
    return iter->chunkIteratorFuncs.GetNext(iter->chunkIterator, sample);
}

// this is an internal function that routes the next call to the appropriate chunk iterator function
static inline ChunkResult SeriesGetPrevious(SeriesIterator *iter, Sample *sample) {
    return iter->chunkIteratorFuncs.GetPrev(iter->chunkIterator, sample);
}

void SeriesIteratorClose(SeriesIterator *iterator) {
    iterator->chunkIteratorFuncs.Free(iterator->chunkIterator);

    if (iterator->aggregationContext != NULL) {
        iterator->aggregation->freeContext(iterator->aggregationContext);
    }

    RedisModule_DictIteratorStop(iterator->dictIter);
}

static inline void resetChunkIterator(SeriesIterator *iterator,
                                      const ChunkFuncs *funcs,
                                      void *currentChunk) {
    iterator->chunkIteratorFuncs.Free(iterator->chunkIterator);
    iterator->chunkIterator = funcs->NewChunkIterator(
        currentChunk, SeriesChunkIteratorOptions(iterator), &iterator->chunkIteratorFuncs);
}

// Fills sample from chunk. If all samples were extracted from the chunk, we
// move to the next chunk.
ChunkResult _seriesIteratorGetNext(SeriesIterator *iterator, Sample *currentSample) {
    ChunkResult res;
    ChunkFuncs *funcs = iterator->series->funcs;
    Chunk_t *currentChunk = iterator->currentChunk;
    const uint64_t itt_max_ts = iterator->maxTimestamp;
    const uint64_t itt_min_ts = iterator->minTimestamp;
    const int not_reverse = !iterator->reverse;
    if (not_reverse) {
        while (TRUE) {
            res = SeriesGetNext(iterator, currentSample);
            if (res == CR_END) { // Reached the end of the chunk
                if (!iterator->DictGetNext(iterator->dictIter, NULL, (void *)&currentChunk) ||
                    funcs->GetFirstTimestamp(currentChunk) > itt_max_ts ||
                    funcs->GetLastTimestamp(currentChunk) < itt_min_ts) {
                    return CR_END; // No more chunks or they out of range
                }
                resetChunkIterator(iterator, funcs, currentChunk);
                if (SeriesGetNext(iterator, currentSample) != CR_OK) {
                    return CR_END;
                }
            } else if (res == CR_ERR) {
                return CR_ERR;
            }
            // check timestamp is within range
            // forward range handling
            if (currentSample->timestamp < itt_min_ts) {
                // didn't reach the starting point of the requested range
                continue;
            }
            if (currentSample->timestamp > itt_max_ts) {
                // reached the end of the requested range
                return CR_END;
            }
            return CR_OK;
        }
    } else {
        while (TRUE) {
            res = SeriesGetPrevious(iterator, currentSample);
            if (res == CR_END) { // Reached the end of the chunk
                if (!iterator->DictGetNext(iterator->dictIter, NULL, (void *)&currentChunk) ||
                    funcs->GetFirstTimestamp(currentChunk) > itt_max_ts ||
                    funcs->GetLastTimestamp(currentChunk) < itt_min_ts) {
                    return CR_END; // No more chunks or they out of range
                }
                resetChunkIterator(iterator, funcs, currentChunk);
                if (SeriesGetPrevious(iterator, currentSample) != CR_OK) {
                    return CR_END;
                }
            } else if (res == CR_ERR) {
                return CR_ERR;
            }
            // reverse range handling
            if (currentSample->timestamp > itt_max_ts) {
                // didn't reach our starting range
                continue;
            }
            if (currentSample->timestamp < itt_min_ts) {
                // didn't reach the starting point of the requested range
                return CR_END;
            }
            return CR_OK;
        }
    }
    return CR_OK;
}

ChunkResult SeriesIteratorGetNextAggregated(SeriesIterator *iterator, Sample *currentSample) {
    Sample internalSample = { 0 };
    ChunkResult result = _seriesIteratorGetNext(iterator, &internalSample);
    bool hasSample = FALSE;
    while (result == CR_OK) {
        if ((iterator->reverse == FALSE &&
             internalSample.timestamp >=
                 iterator->aggregationLastTimestamp + iterator->aggregationTimeDelta) ||
            (iterator->reverse == TRUE &&
             internalSample.timestamp < iterator->aggregationLastTimestamp)) {
            // update the last timestamp before because its relevant for first sample and others
            if (iterator->aggregationIsFirstSample == FALSE) {
                double value;
                if (iterator->aggregation->finalize(iterator->aggregationContext, &value) ==
                    TSDB_OK) {
                    currentSample->timestamp = iterator->aggregationLastTimestamp;
                    currentSample->value = value;
                    hasSample = TRUE;
                    iterator->aggregation->resetContext(iterator->aggregationContext);
                }
            }
            iterator->aggregationLastTimestamp =
                internalSample.timestamp -
                (internalSample.timestamp % iterator->aggregationTimeDelta);
        }
        iterator->aggregationIsFirstSample = FALSE;
        iterator->aggregation->appendValue(iterator->aggregationContext, internalSample.value);
        if (hasSample) {
            return CR_OK;
        }
        result = _seriesIteratorGetNext(iterator, &internalSample);
    }

    if (result == CR_END) {
        if (iterator->aggregationIsFinalized || iterator->aggregationIsFirstSample) {
            return CR_END;
        } else {
            double value;
            if (iterator->aggregation->finalize(iterator->aggregationContext, &value) == TSDB_OK) {
                currentSample->timestamp = iterator->aggregationLastTimestamp;
                currentSample->value = value;
            }
            iterator->aggregationIsFinalized = TRUE;
            return CR_OK;
        }
    } else {
        return CR_ERR;
    }
}

ChunkResult SeriesIteratorGetNext(SeriesIterator *iterator, Sample *currentSample) {
    if (iterator->aggregation == NULL) {
        return _seriesIteratorGetNext(iterator, currentSample);
    } else {
        return SeriesIteratorGetNextAggregated(iterator, currentSample);
    }
}
