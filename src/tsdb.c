#include <time.h>
#include "tsdb.h"
#include "rmutil/alloc.h"

Series * NewSeries(int32_t retentionSecs, short maxSamplesPerChunk)
{
    Series *newSeries = (Series *)malloc(sizeof(Series));
    newSeries->maxSamplesPerChunk = maxSamplesPerChunk;
    newSeries->firstChunk = NewChunk(newSeries->maxSamplesPerChunk);
    newSeries->lastChunk = newSeries->firstChunk;
    newSeries->chunkCount = 1;
    newSeries->lastTimestamp = 0;
    newSeries->retentionSecs = retentionSecs;
    newSeries->rules = NULL;

    return newSeries;
}

void SeriesTrim(Series * series) {
    if (series->retentionSecs == 0) {
        return;
    }
    Chunk *currentChunk = series->firstChunk;
    timestamp_t minTimestamp = time(NULL) - series->retentionSecs;
    while (currentChunk != NULL)
    {
        if (ChunkGetLastTimestamp(currentChunk) < minTimestamp)
        {
            Chunk *nextChunk = currentChunk->nextChunk;
            if (nextChunk != NULL) {
                series->firstChunk = nextChunk;    
            } else {
                series->firstChunk = NewChunk(series->maxSamplesPerChunk);
            }
            
            series->chunkCount--;
            FreeChunk(currentChunk);
            currentChunk = nextChunk;
        } else {
            break;
        }
    }
}

void FreeSeries(void *value) {
    Series *currentSeries = (Series *) value;
    Chunk *currentChunk = currentSeries->firstChunk;
    while (currentChunk != NULL)
    {
        Chunk *nextChunk = currentChunk->nextChunk;
        FreeChunk(currentChunk);
        currentChunk = nextChunk;
    }
}

size_t SeriesMemUsage(const void *value) {
    Series *series = (Series *)value;
    return sizeof(series) + sizeof(Chunk) * series->chunkCount;
}

int SeriesAddSample(Series *series, api_timestamp_t timestamp, double value) {
    if (timestamp < series->lastTimestamp) {
        return TSDB_ERR_TIMESTAMP_TOO_OLD;
    } else if (timestamp == series->lastTimestamp) {
        // this is a hack, we want to override the last sample, so lets ignore it first
        series->lastChunk->num_samples--;
    }
    
    Chunk *currentChunk = series->lastChunk;
    Sample sample = {.timestamp = timestamp, .data = value};
    int ret = ChunkAddSample(currentChunk, sample);
    if (ret == 0 ) {
        // When a new chunk is created trim the series
        SeriesTrim(series);

        Chunk *newChunk = NewChunk(series->maxSamplesPerChunk);
        series->lastChunk->nextChunk = newChunk;
        series->lastChunk = newChunk;
        series->chunkCount++;        
        currentChunk = newChunk;
        // re-add the sample
        ChunkAddSample(currentChunk, sample);
    } 
    series->lastTimestamp = timestamp;

    return TSDB_OK;
}

SeriesItertor SeriesQuery(Series *series, api_timestamp_t minTimestamp, api_timestamp_t maxTimestamp) {
    SeriesItertor iter;
    iter.series = series;
    iter.currentChunk = series->firstChunk;
    iter.chunkIteratorInitilized = FALSE;
    iter.currentSampleIndex = 0;
    iter.minTimestamp = minTimestamp;
    iter.maxTimestamp = maxTimestamp;
    return iter;
}

int SeriesItertorGetNext(SeriesItertor *iterator, Sample *currentSample) {
    Sample internalSample;
    while (iterator->currentChunk != NULL)
    {
        Chunk *currentChunk = iterator->currentChunk;
        if (ChunkGetLastTimestamp(currentChunk) < iterator->minTimestamp)
        {
            iterator->currentChunk = currentChunk->nextChunk;
            iterator->chunkIteratorInitilized = FALSE;
            continue;
        }
        else if (ChunkGetFirstTimestamp(currentChunk) > iterator->maxTimestamp)
        {
            break;
        }
        
        if (!iterator->chunkIteratorInitilized) 
        {
            iterator->chunkIterator = NewChunkIterator(iterator->currentChunk);
            iterator->chunkIteratorInitilized = TRUE;
        }

        if (ChunkItertorGetNext(&iterator->chunkIterator, &internalSample) == 0) { // reached the end of the chunk
            iterator->currentChunk = currentChunk->nextChunk;
            iterator->chunkIteratorInitilized = FALSE;
            continue;
        }

        if (internalSample.timestamp < iterator->minTimestamp) {
            continue;
        } else if (internalSample.timestamp > iterator->maxTimestamp) {
            break;
        } else {
            memcpy(currentSample, &internalSample, sizeof(Sample));
            return 1;
        }
    }
    return 0;
}

CompactionRule *NewRule(RedisModuleString *destKey, int aggType, int bucketSizeSec) {
    if (bucketSizeSec <= 0) {
        return NULL;
    }

    CompactionRule *rule = (CompactionRule *)malloc(sizeof(CompactionRule));
    rule->aggClass = GetAggClass(aggType);;
    rule->aggType = aggType;
    rule->aggContext = rule->aggClass->createContext();
    rule->bucketSizeSec = bucketSizeSec;
    rule->destKey = destKey;

    rule->nextRule = NULL;

    return rule;
}