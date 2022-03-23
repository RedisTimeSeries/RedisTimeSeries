/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "series_iterator.h"

#include "abstract_iterator.h"
#include "filter_iterator.h"
#include "tsdb.h"
#include "domain_chunk.h"

DomainChunk *SeriesIteratorGetNextChunk(AbstractIterator *iterator);

void SeriesIteratorClose(AbstractIterator *iterator);

// Initiates SeriesIterator, find the correct chunk and initiate a ChunkIterator
AbstractIterator *SeriesIterator_New(Series *series,
                                     timestamp_t start_ts,
                                     timestamp_t end_ts,
                                     bool rev,
                                     bool rev_chunk) {
    SeriesIterator *iter = malloc(sizeof(SeriesIterator));
    iter->base.Close = SeriesIteratorClose;
    iter->base.GetNext = SeriesIteratorGetNextChunk;
    iter->base.input = NULL;
    iter->currentChunk = NULL;
    iter->domainChunk = allocateDomainChunk();
    iter->domainChunkAux = allocateDomainChunk();
    iter->series = series;
    iter->minTimestamp = start_ts;
    iter->maxTimestamp = end_ts;
    iter->reverse = rev;
    iter->reverse_chunk = rev_chunk;
    iter->isFirstIteration = true;

    timestamp_t rax_key;

    if (!rev) {
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

    return (AbstractIterator *)iter;
}

void SeriesIteratorClose(AbstractIterator *iterator) {
    SeriesIterator *self = (SeriesIterator *)iterator;
    RedisModule_DictIteratorStop(self->dictIter);
    FreeDomainChunk(self->domainChunk, true);
    FreeDomainChunk(self->domainChunkAux, false);
    free(iterator);
}

// Fills sample from chunk. If all samples were extracted from the chunk, we
// move to the next chunk.
DomainChunk *SeriesIteratorGetNextChunk(AbstractIterator *abstractIterator) {
    SeriesIterator *iter = (SeriesIterator *)abstractIterator;
    if (!iter->currentChunk) {
        return NULL;
    }
    u_int64_t n_samples = iter->series->funcs->GetNumOfSample(iter->currentChunk);
    if (n_samples > iter->domainChunk->size) {
        ReallocDomainChunk(iter->domainChunk, n_samples);
    }
    DomainChunk *ret = iter->series->funcs->ProcessChunk(iter->currentChunk,
                                                         iter->minTimestamp,
                                                         iter->maxTimestamp,
                                                         iter->domainChunk,
                                                         iter->domainChunkAux,
                                                         iter->reverse_chunk);
    if (!iter->DictGetNext(iter->dictIter, NULL, (void *)&iter->currentChunk)) {
        iter->currentChunk = NULL;
    }

    if (unlikely(iter->isFirstIteration && !ret && !iter->reverse)) {
        // It is possible that start is larger than the first chunk - need to check the next one
        iter->isFirstIteration = false;
        return SeriesIteratorGetNextChunk(abstractIterator);
    }

    return ret;
}
