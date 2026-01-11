/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "compaction.h"
#include "compressed_chunk.h"
#include "gorilla.h"
#include "minunit.h"
#include "parse_policies.h"
#include "tsdb.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include "rmutil/alloc.h"

MU_TEST(test_compressed_upsert) {
    srand((unsigned int)time(NULL));
    int total_data_points = 500;
    size_t max_chunk_size = 8192;
    int total_upserts = 0;
    int size = 0;
    float minV = 0.0;
    float maxV = 100.0;
    for (size_t chunk_size = 8; chunk_size < max_chunk_size; chunk_size += 64) {
        CompressedChunk *chunk = Compressed_NewChunk(chunk_size);
        mu_assert(chunk != NULL, "create compressed chunk");
        for (size_t i = 1; i <= total_data_points; i++) {
            float value = minV + (float)rand() / ((float)RAND_MAX / maxV);
            Sample sample = { .timestamp = i, .value = value };
            total_upserts++;
            UpsertCtx uCtx = {
                .inChunk = chunk,
                .sample = sample,
            };
            Compressed_UpsertSample(&uCtx, &size, DP_LAST);
        }
        uint64_t total_samples = Compressed_ChunkNumOfSample(chunk);
        mu_assert_int_eq(total_data_points, total_samples);
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
    Compressed_AddSample(chunk, &s1);
    mu_assert_int_eq(1, Compressed_ChunkNumOfSample(chunk));
    int size = 0;
    float minV = 0.0;
    float maxV = 100.0;
    UpsertCtx uCtx = {
        .inChunk = chunk,
        .sample = s2,
    };
    Compressed_UpsertSample(&uCtx, &size, DP_LAST);
    mu_assert_int_eq(2, Compressed_ChunkNumOfSample(chunk));
    mu_assert_int_eq(1, size);
    Compressed_UpsertSample(&uCtx, &size, DP_LAST);
    mu_assert_int_eq(2, Compressed_ChunkNumOfSample(chunk));
    mu_assert_int_eq(0, size);
    mu_assert_int_eq(6, Compressed_GetFirstTimestamp(chunk));
    mu_assert_int_eq(10, Compressed_GetLastTimestamp(chunk));
    for (size_t i = 0; i < 10; i++) {
        s2.value = minV + (float)rand() / ((float)RAND_MAX / maxV);
        Compressed_UpsertSample(&uCtx, &size, DP_LAST);
        // ensure we're not adding more datapoints and only overwritting previous ones
        mu_assert_int_eq(2, Compressed_ChunkNumOfSample(chunk));
        mu_assert_int_eq(0, size);
    }
    CompressedChunk *chunk2 = Compressed_SplitChunk(chunk);
    mu_assert(chunk2 != NULL, "splitted compressed chunk");
    mu_assert_int_eq(1, Compressed_ChunkNumOfSample(chunk));
    mu_assert_int_eq(1, Compressed_ChunkNumOfSample(chunk2));

    mu_assert_int_eq(6, Compressed_GetFirstTimestamp(chunk));
    mu_assert_int_eq(6, Compressed_GetLastTimestamp(chunk));

    mu_assert_int_eq(10, Compressed_GetFirstTimestamp(chunk2));
    mu_assert_int_eq(10, Compressed_GetLastTimestamp(chunk2));

    for (size_t i = 1; i < 6; i++) {
        Sample s3 = { .timestamp = i, .value = minV + (float)rand() / ((float)RAND_MAX / maxV) };
        UpsertCtx uCtxS3 = {
            .inChunk = chunk,
            .sample = s3,
        };
        ChunkResult rv = Compressed_UpsertSample(&uCtxS3, &size, DP_LAST);
        mu_assert(rv == CR_OK, "upsert");
    }
    mu_assert_int_eq(6, Compressed_ChunkNumOfSample(chunk));
    mu_assert_int_eq(1, Compressed_GetFirstTimestamp(chunk));
    mu_assert_int_eq(6, Compressed_GetLastTimestamp(chunk));
    Compressed_FreeChunk(chunk);
    Compressed_FreeChunk(chunk2);
}

MU_TEST(test_Compressed_SplitChunk_empty) {
    srand((unsigned int)time(NULL));
    const size_t chunk_size = 4096; // 4096 bytes (data) chunck
    CompressedChunk *chunk = Compressed_NewChunk(chunk_size);
    mu_assert(chunk != NULL, "create compressed chunk");

    CompressedChunk *chunk2 = Compressed_SplitChunk(chunk);
    mu_assert(chunk2 != NULL, "splitted compressed chunk");
    mu_assert_int_eq(0, Compressed_ChunkNumOfSample(chunk));
    mu_assert_int_eq(0, Compressed_ChunkNumOfSample(chunk2));

    Compressed_FreeChunk(chunk);
    Compressed_FreeChunk(chunk2);
}

MU_TEST(test_Compressed_SplitChunk_odd) {
    srand((unsigned int)time(NULL));
    const size_t chunk_size = 4096; // 4096 bytes (data) chunck
    CompressedChunk *chunk = Compressed_NewChunk(chunk_size);
    mu_assert(chunk != NULL, "create compressed chunk");

    Sample s1 = { .timestamp = 4, .value = 5.0 };
    Sample s2 = { .timestamp = 50, .value = 10.0 };
    Sample s3 = { .timestamp = 100, .value = 10.0 };
    ChunkResult rv = Compressed_AddSample(chunk, &s1);
    mu_assert(rv == CR_OK, "add sample s1");

    rv = Compressed_AddSample(chunk, &s2);
    mu_assert(rv == CR_OK, "add sample s2");

    rv = Compressed_AddSample(chunk, &s3);
    mu_assert(rv == CR_OK, "add sample s3");

    mu_assert_int_eq(3, Compressed_ChunkNumOfSample(chunk));

    CompressedChunk *chunk2 = Compressed_SplitChunk(chunk);
    mu_assert(chunk2 != NULL, "splitted compressed chunk");
    mu_assert_int_eq(2, Compressed_ChunkNumOfSample(chunk));
    mu_assert_int_eq(1, Compressed_ChunkNumOfSample(chunk2));

    Compressed_FreeChunk(chunk);
    Compressed_FreeChunk(chunk2);
}

MU_TEST(test_Compressed_SplitChunk_force_realloc) {
    srand((unsigned int)time(NULL));
    const size_t chunk_size = 4096; // 4096 bytes (data) chunck
    CompressedChunk *chunk = Compressed_NewChunk(chunk_size);
    mu_assert(chunk != NULL, "create compressed chunk");
    ChunkResult rv = CR_OK;
    int64_t ts = 1;
    int64_t total_added_samples = 0;

    // adding 1,3,5....
    while (rv != CR_END) {
        double tsv = ts * 1.0;
        Sample s1 = { .timestamp = ts, .value = tsv };
        rv = Compressed_AddSample(chunk, &s1);
        mu_assert(rv == CR_OK || rv == CR_END, "add sample");
        if (rv != CR_END) {
            ts++;
            total_added_samples++;
            mu_assert_int_eq(total_added_samples, chunk->count);
        }
    }
    const size_t chunk_current_size = Compressed_GetChunkSize(chunk, false);

    mu_assert_int_eq(chunk_size, chunk_current_size);
    mu_assert_int_eq(chunk_size, chunk->size);

    // Now we're at the max of the chunck's capacity
    Sample s3 = { .timestamp = 2, .value = 10.0 };
    UpsertCtx uCtxS3 = {
        .inChunk = chunk,
        .sample = s3,
    };

    int size = 0;

    // We're forcing the chunk to grow
    rv = Compressed_UpsertSample(&uCtxS3, &size, DP_BLOCK);
    mu_assert(rv == CR_ERR, "Upsert should fail");
    rv = Compressed_UpsertSample(&uCtxS3, &size, DP_LAST);
    mu_assert(rv == CR_OK, "upserted existing sample");

    uCtxS3.sample.timestamp = ts + 1;
    rv = Compressed_UpsertSample(&uCtxS3, &size, DP_LAST);
    mu_assert(rv == CR_OK, "upsert non existing sample");
    total_added_samples++;
    mu_assert_int_eq(total_added_samples, chunk->count);
    mu_assert_int_eq(chunk_size + 32, chunk->size);

    Compressed_FreeChunk(chunk);
}

MU_TEST(test_nan_mixed_compression) {
    const size_t chunk_size = 4096;
    const int total_samples = 1000;
    const int nan_frequency = 100; // 1 in 100 samples is NaN (1%)

    // Different NaN bit patterns
    union
    {
        double d;
        uint64_t u;
    } nan1, nan2, nan3;

    nan1.u = CANONICAL_NAN_BITS;
    nan2.u = 0x7ff8000000000001ULL; // Different payload
    nan3.u = 0x7ffFFFFFFFFFFFFFULL; // Max payload

    // Chunk 1: Sparse NaN with canonical pattern
    CompressedChunk *chunk_canonical = Compressed_NewChunk(chunk_size);
    mu_assert(chunk_canonical != NULL, "create chunk_canonical");

    for (int i = 0; i < total_samples; i++) {
        double val = (i % nan_frequency == 0) ? nan1.d : (double)(i * 1.5);
        Sample s = { .timestamp = i + 1, .value = val };
        Compressed_AddSample(chunk_canonical, &s);
    }
    uint64_t bits_canonical = chunk_canonical->idx;

    // Chunk 2: Sparse NaN with varying patterns
    CompressedChunk *chunk_varying = Compressed_NewChunk(chunk_size);
    mu_assert(chunk_varying != NULL, "create chunk_varying");

    int nan_count = 0;
    for (int i = 0; i < total_samples; i++) {
        double val;
        if (i % nan_frequency == 0) {
            switch (nan_count++ % 3) {
                case 0:
                    val = nan1.d;
                    break;
                case 1:
                    val = nan2.d;
                    break;
                default:
                    val = nan3.d;
                    break;
            }
        } else {
            val = (double)(i * 1.5);
        }
        Sample s = { .timestamp = i + 1, .value = val };
        Compressed_AddSample(chunk_varying, &s);
    }
    uint64_t bits_varying = chunk_varying->idx;

    mu_assert(bits_varying == bits_canonical,
              "Sparse NaN data with varying patterns should compress same as canonical");

    Compressed_FreeChunk(chunk_canonical);
    Compressed_FreeChunk(chunk_varying);
}

MU_TEST_SUITE(compressed_chunk_test_suite) {
    MU_RUN_TEST(test_compressed_upsert);
    MU_RUN_TEST(test_compressed_fail_appendInteger);
    MU_RUN_TEST(test_Compressed_SplitChunk_empty);
    MU_RUN_TEST(test_Compressed_SplitChunk_odd);
    MU_RUN_TEST(test_Compressed_SplitChunk_force_realloc);
    MU_RUN_TEST(test_nan_mixed_compression);
}
