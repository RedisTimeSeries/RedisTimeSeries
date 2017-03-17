#include <time.h>
#include "tsdb.h"
#include "rmutil/alloc.h"

Chunk * NewChunk(size_t sampleCount)
{
    Chunk *newChunk = (Chunk *)malloc(sizeof(Chunk));
    newChunk->num_samples = 0;
    newChunk->nextChunk = NULL;
    newChunk->samples = malloc(sizeof(Sample)*sampleCount);

    return newChunk;
}

void freeChunk(Chunk *chunk) {
    free(chunk->samples);
    free(chunk);
}

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
        if (currentChunk->samples[currentChunk->num_samples - 1].timestamp < minTimestamp)
        {
            Chunk *nextChunk = currentChunk->nextChunk;
            if (nextChunk != NULL) {
                series->firstChunk = nextChunk;    
            } else {
                series->firstChunk = NewChunk(series->maxSamplesPerChunk);
            }
            
            series->chunkCount--;
            freeChunk(currentChunk);
            currentChunk = nextChunk;
        } else {
            break;
        }
    }
}

void SeriesFree(void *value) {
    Series *currentSeries = (Series *) value;
    Chunk *currentChunk = currentSeries->firstChunk;
    while (currentChunk != NULL)
    {
        Chunk *nextChunk = currentChunk->nextChunk;
        freeChunk(currentChunk);
        currentChunk = nextChunk;
    }
}

size_t SeriesMemUsage(void *value) {
    Series *series = (Series *)value;
    return sizeof(series) + sizeof(Chunk) * series->chunkCount;
}

int SeriesAddSample(Series *series, api_timestamp_t timestamp, double value) {
    if (timestamp < series->lastTimestamp) {
        return TSDB_ERR_TIMESTAMP_TOO_OLD;
    } else if (timestamp == series->lastTimestamp) {
        // we want to override the last sample, so lets ignore it first
        series->lastChunk->num_samples--;
    }
    
    Chunk *currentChunk;
    if (series->lastChunk->num_samples < series->maxSamplesPerChunk) {
         currentChunk = series->lastChunk;
         if (currentChunk->num_samples == 0) {
             currentChunk->base_timestamp = timestamp;
         }
    }
    else {
        // When a new chunk is created trim the series
        SeriesTrim(series);

        Chunk *newChunk = NewChunk(series->maxSamplesPerChunk);
        newChunk->base_timestamp = timestamp;
        series->lastChunk->nextChunk = newChunk;
        series->lastChunk = newChunk;
        series->chunkCount++;        
        currentChunk = newChunk;
    }
    currentChunk->samples[currentChunk->num_samples].timestamp = timestamp;
    currentChunk->samples[currentChunk->num_samples].data = value;
    currentChunk->num_samples++;
    series->lastTimestamp = timestamp;

    return TSDB_OK;
}

SeriesItertor SeriesQuery(Series *series, api_timestamp_t minTimestamp, api_timestamp_t maxTimestamp) {
    SeriesItertor iter;
    iter.series = series;
    iter.currentChunk = series->firstChunk;
    iter.currentSampleIndex = 0;
    iter.minTimestamp = minTimestamp;
    iter.maxTimestamp = maxTimestamp;
    return iter;
}

Sample *SeriesItertorGetNext(SeriesItertor *iterator) {
    while (iterator->currentChunk != NULL)
    {
        Chunk *currentChunk = iterator->currentChunk;
        if (currentChunk->samples[currentChunk->num_samples - 1].timestamp < iterator->minTimestamp)
        {
            iterator->currentChunk = currentChunk->nextChunk;
            continue;
        }
        else if (currentChunk->base_timestamp > iterator->maxTimestamp)
        {
            break;
        }

        if (iterator->currentSampleIndex >= currentChunk->num_samples) {
            iterator->currentSampleIndex = 0;
            iterator->currentChunk = currentChunk->nextChunk;
            continue;
        } else {
            int currentSampleIndex = iterator->currentSampleIndex;
            iterator->currentSampleIndex++;
            if (currentChunk->samples[currentSampleIndex].timestamp < iterator->minTimestamp) {
                continue;
            }
            else if (currentChunk->samples[currentSampleIndex].timestamp > iterator->maxTimestamp) {
                break;
            }
            return &currentChunk->samples[currentSampleIndex];
        }
    }
    return NULL;
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