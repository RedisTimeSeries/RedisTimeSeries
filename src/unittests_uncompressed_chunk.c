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
#include "chunk.h"
#include "tsdb.h"


MU_TEST(test_Uncompressed_NewChunk) {
    srand((unsigned int)time(NULL));
    int total_data_points = 500;
    size_t max_chunk_size = 8192;
    int total_upserts = 0;
    int size = 0;
    float minV = 0.0;
    float maxV = 100.0;
    for (size_t chunk_size = 2; chunk_size < max_chunk_size; chunk_size+=64 ){
        Chunk *chunk = Uncompressed_NewChunk(chunk_size);
        mu_assert(chunk != NULL, "create compressed chunk");
        mu_assert_short_eq(0,chunk->num_samples);
        Uncompressed_FreeChunk(chunk);
    }
}

MU_TEST_SUITE(uncompressed_chunk_test_suite) {
    MU_RUN_TEST(test_Uncompressed_NewChunk);
}