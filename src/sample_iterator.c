/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */

#include "sample_iterator.h"

ChunkResult SeriesSampleIterator_GetNext(struct AbstractSampleIterator *base, Sample *sample) {
    SeriesSampleIterator *iter = (SeriesSampleIterator *)base;
    if (unlikely(!iter->chunk || iter->cur_index >= iter->chunk->samples.num_samples)) {
        iter->chunk = iter->base.input->GetNext(iter->base.input);
        if (!iter->chunk || iter->chunk->samples.num_samples == 0) {
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
