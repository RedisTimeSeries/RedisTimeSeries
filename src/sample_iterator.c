/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "sample_iterator.h"

ChunkResult SeriesSampleIterator_GetNext(struct AbstractSampleIterator *base, Sample *sample) {
    SeriesSampleIterator *iter = (SeriesSampleIterator *)base;
    if (unlikely(!iter->chunk || iter->cur_index >= iter->chunk->num_samples)) {
        iter->chunk = iter->base.input->GetNext(iter->base.input);
        if (!iter->chunk || iter->chunk->num_samples == 0) {
            return CR_END;
        }
        iter->cur_index = 0;
    }

    sample->timestamp = iter->chunk->samples.timestamps[iter->cur_index];
    sample->value = iter->chunk->samples.values[iter->cur_index];
    iter->cur_index++;
    return CR_OK;
}

void SeriesSampleIterator_Close(struct AbstractSampleIterator *iterator) {
    iterator->input->Close(iterator->input);
    free(iterator);
}

SeriesSampleIterator *SeriesSampleIterator_New(AbstractIterator *input) {
    SeriesSampleIterator *newIter = malloc(sizeof(SeriesSampleIterator));
    newIter->base.input = input;
    newIter->base.GetNext = SeriesSampleIterator_GetNext;
    newIter->base.Close = SeriesSampleIterator_Close;
    newIter->cur_index = 0;
    newIter->chunk = NULL;
    return newIter;
}
