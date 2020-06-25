/*
* Copyright 2018-2020 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#include <stdio.h>
#include <stdlib.h>
#include "parse_policies.h"
#include "minunit.h"
#include "compaction.h"
#include "rmutil/alloc.h"
#include "compressed_chunk.h"
#include "gorilla.h"
#include "tsdb.h"


MU_TEST(test_valid_policy) {
    SimpleCompactionRule* parsedRules;
    size_t rulesCount;
    int result = ParseCompactionPolicy("max:1m:1h;min:10s:10d;last:5M:10m;avg:2h:10d;avg:3d:100d", &parsedRules, &rulesCount);
	mu_check(result == TRUE);
	mu_check(rulesCount == 5);

    mu_check(parsedRules[0].aggType == StringAggTypeToEnum("max"));
    mu_assert_int_eq(parsedRules[0].timeBucket, 1);

    mu_check(parsedRules[1].aggType == StringAggTypeToEnum("min"));
    mu_check(parsedRules[1].timeBucket == 10*1000);

    mu_check(parsedRules[2].aggType == StringAggTypeToEnum("last"));
    mu_check(parsedRules[2].timeBucket == 5*60*1000);

    mu_check(parsedRules[3].aggType == StringAggTypeToEnum("avg"));
    mu_check(parsedRules[3].timeBucket == 2*60*60*1000);

    mu_check(parsedRules[4].aggType == StringAggTypeToEnum("avg"));
    mu_check(parsedRules[4].timeBucket == 3*60*60*1000*24);
    free(parsedRules);
}


MU_TEST(test_compressed_upsert) {
    srand((unsigned int)time(NULL));
    int total_data_points = 500;
    size_t max_chunk_size = 8192;
    int total_upserts = 0;
    int size = 0;
    float minV = 0.0;
    float maxV = 100.0;
    for (size_t chunk_size = 2; chunk_size < max_chunk_size; chunk_size+=64 ){
        CompressedChunk *chunk = Compressed_NewChunk(chunk_size);
        mu_assert(chunk != NULL, "create compressed chunk");
        for (size_t i = 1; i <= total_data_points; i++){
            float value = minV + (float)rand()/(float)(RAND_MAX/maxV);
            Sample sample = { .timestamp = i, .value = value };
            total_upserts++;
            UpsertCtx uCtx = {
            .inChunk = chunk,
            .sample = sample,
            };
            Compressed_UpsertSample(&uCtx, &size);
        }
        uint64_t total_samples = Compressed_ChunkNumOfSample(chunk);
        mu_assert_int_eq(total_data_points,total_samples);
        Compressed_FreeChunk(chunk);
    }
}

MU_TEST(test_compressed_fail_appendInteger) {
    // either Compressed_UpsertSample or Compressed_SplitChunk
    // ensureAddSample -> Compressed_AddSample -> Compressed_Append -> appendInteger
    srand((unsigned int)time(NULL));
    const size_t chunk_size = 4096; // 4096 bytes (data) chunck
    CompressedChunk *chunk = Compressed_NewChunk(chunk_size);
    mu_assert(chunk != NULL, "create compressed chunk");
    Sample s1 = { .timestamp = 10, .value = 5.0 };
    Sample s2 = { .timestamp = 6, .value = 10.0 };
    Compressed_AddSample(chunk,&s1);
    mu_assert_int_eq(1,Compressed_ChunkNumOfSample(chunk));
    int size = 0;
    float minV = 0.0;
    float maxV = 100.0;
    UpsertCtx uCtx = {
    .inChunk = chunk,
    .sample = s2,
    };
    Compressed_UpsertSample(&uCtx, &size);
    mu_assert_int_eq(2,Compressed_ChunkNumOfSample(chunk));
    mu_assert_int_eq(1,size);
    Compressed_UpsertSample(&uCtx, &size);
    mu_assert_int_eq(2,Compressed_ChunkNumOfSample(chunk));
    mu_assert_int_eq(0,size);
    mu_assert_int_eq(6,Compressed_GetFirstTimestamp(chunk));
    mu_assert_int_eq(10,Compressed_GetLastTimestamp(chunk));
    for (size_t i = 0; i < 10; i++)
    {
        s2.value = minV + (float)rand()/(float)(RAND_MAX/maxV);
        Compressed_UpsertSample(&uCtx, &size);
        // ensure we're not adding more datapoints and only overwritting previous ones
        mu_assert_int_eq(2,Compressed_ChunkNumOfSample(chunk));
        mu_assert_int_eq(0,size);
    }
    CompressedChunk *chunk2 = Compressed_SplitChunk(chunk);
    mu_assert(chunk2 != NULL, "splitted compressed chunk");
    mu_assert_int_eq(1,Compressed_ChunkNumOfSample(chunk));
    mu_assert_int_eq(1,Compressed_ChunkNumOfSample(chunk2));

    mu_assert_int_eq(6,Compressed_GetFirstTimestamp(chunk));
    mu_assert_int_eq(6,Compressed_GetLastTimestamp(chunk));

    mu_assert_int_eq(10,Compressed_GetFirstTimestamp(chunk2));
    mu_assert_int_eq(10,Compressed_GetLastTimestamp(chunk2));

    for (size_t i = 1; i < 6; i++)
    {   
        Sample s3 = { .timestamp = i, .value =minV + (float)rand()/(float)(RAND_MAX/maxV) };
        UpsertCtx uCtxS3 = {
        .inChunk = chunk,
        .sample = s3,
    };
        ChunkResult rv = Compressed_UpsertSample(&uCtxS3, &size);
        mu_assert(rv == CR_OK, "upsert");
    }
    mu_assert_int_eq(6,Compressed_ChunkNumOfSample(chunk));
    mu_assert_int_eq(1,Compressed_GetFirstTimestamp(chunk));
    mu_assert_int_eq(6,Compressed_GetLastTimestamp(chunk));
    Compressed_FreeChunk(chunk);
}

MU_TEST(test_invalid_policy) {
    SimpleCompactionRule* parsedRules;
    size_t rulesCount;
    int result;
    result = ParseCompactionPolicy("max:1M;mins:10s;avg:2h;avg:1d", &parsedRules, &rulesCount);
	mu_check(result == FALSE);
	mu_check(rulesCount == 0);

    result = ParseCompactionPolicy("max:12hd;", &parsedRules, &rulesCount);
	mu_check(result == FALSE);
	mu_check(rulesCount == 0);
    free(parsedRules);
}

MU_TEST(test_StringLenAggTypeToEnum) {
    mu_check(StringAggTypeToEnum("min") == TS_AGG_MIN);
    mu_check(StringAggTypeToEnum("max") == TS_AGG_MAX);
    mu_check(StringAggTypeToEnum("sum") == TS_AGG_SUM);
    mu_check(StringAggTypeToEnum("avg") == TS_AGG_AVG);
    mu_check(StringAggTypeToEnum("count") == TS_AGG_COUNT);
    mu_check(StringAggTypeToEnum("first") == TS_AGG_FIRST);
    mu_check(StringAggTypeToEnum("last") == TS_AGG_LAST);
    mu_check(StringAggTypeToEnum("range") == TS_AGG_RANGE);
}

MU_TEST_SUITE(test_suite) {
	MU_RUN_TEST(test_valid_policy);
	MU_RUN_TEST(test_invalid_policy);
	MU_RUN_TEST(test_StringLenAggTypeToEnum);
    MU_RUN_TEST(test_compressed_upsert);
    MU_RUN_TEST(test_compressed_fail_appendInteger);
}

int main(int argc, char *argv[]) {
    RMUTil_InitAlloc();
    MU_RUN_SUITE(test_suite);
	MU_REPORT();
	return 0;
}