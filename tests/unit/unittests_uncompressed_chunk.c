/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "chunk.h"
#include "../deps/LibMR/src/utils/buffer.h"
#include "compaction.h"
#include "enriched_chunk.h"
#include "minunit.h"
#include "parse_policies.h"
#include "tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include "rmutil/alloc.h"

MU_TEST(test_Uncompressed_NewChunk) {
    srand((unsigned int)time(NULL));
    size_t max_chunk_size = 8192;
    for (size_t chunk_size = 8; chunk_size < max_chunk_size; chunk_size += 64) {
        Chunk *chunk = Uncompressed_NewChunk(chunk_size);
        mu_assert(chunk != NULL, "create uncompressed chunk");
        mu_assert_short_eq(0, chunk->num_samples);
        Uncompressed_FreeChunk(chunk);
    }
}

MU_TEST(test_Uncompressed_Uncompressed_AddSample) {
    srand((unsigned int)time(NULL));
    const size_t chunk_size = 4096; // 4096 bytes (data) chunck
    Chunk *chunk = Uncompressed_NewChunk(chunk_size);
    mu_assert(chunk != NULL, "create uncompressed chunk");
    mu_assert_short_eq(0, chunk->num_samples);
    ChunkResult rv = CR_OK;
    int64_t ts = 1;
    int64_t total_added_samples = 0;

    // adding 1,3,5....
    while (rv != CR_END) {
        double tsv = ts * 1.0;
        Sample s1 = { .timestamp = ts, .value = tsv };
        rv = Uncompressed_AddSample(chunk, &s1);
        mu_assert(rv == CR_OK || rv == CR_END, "add sample");
        if (rv != CR_END) {
            total_added_samples++;
            mu_assert_int_eq(total_added_samples, chunk->num_samples);
        }
    }
    const size_t chunk_current_size = Uncompressed_GetChunkSize(chunk, false);
    mu_assert_int_eq(chunk_size, chunk_current_size);
    Uncompressed_FreeChunk(chunk);
}

MU_TEST(test_Uncompressed_Uncompressed_UpsertSample) {
    srand((unsigned int)time(NULL));
    const size_t chunk_size = 4096; // 4096 bytes (data) chunck
    Chunk *chunk = Uncompressed_NewChunk(chunk_size);
    mu_assert(chunk != NULL, "create uncompressed chunk");
    mu_assert_short_eq(0, chunk->num_samples);
    ChunkResult rv = CR_OK;
    int64_t ts = 1;
    int64_t total_added_samples = 0;

    // adding 1,3,5....
    while (rv != CR_END) {
        double tsv = ts * 1.0;
        Sample s1 = { .timestamp = ts, .value = tsv };
        rv = Uncompressed_AddSample(chunk, &s1);
        mu_assert(rv == CR_OK || rv == CR_END, "add sample");
        if (rv != CR_END) {
            total_added_samples++;
            mu_assert_int_eq(total_added_samples, chunk->num_samples);
        }
    }
    const size_t chunk_current_size = Uncompressed_GetChunkSize(chunk, false);
    mu_assert_int_eq(chunk_size, chunk_current_size);

    // Now we're at the max of the chunck's capacity
    Sample s3 = { .timestamp = 2, .value = 10.0 };
    UpsertCtx uCtxS3 = {
        .inChunk = chunk,
        .sample = s3,
    };

    int size = 0;
    // We're forcing the chunk to grow
    rv = Uncompressed_UpsertSample(&uCtxS3, &size, DP_LAST);
    total_added_samples++;
    mu_assert(rv == CR_OK, "upsert");
    mu_assert_int_eq(total_added_samples, chunk->num_samples);
    Uncompressed_FreeChunk(chunk);
}

MU_TEST(test_Uncompressed_UpsertSample_large_sample_count) {
    const unsigned int sample_counts[] = { 32768, 65536 };

    for (size_t count_index = 0; count_index < 2; ++count_index) {
        const unsigned int sample_count = sample_counts[count_index];
        Chunk *chunk = Uncompressed_NewChunk((size_t)sample_count * SAMPLE_SIZE);
        mu_assert(chunk != NULL, "create large uncompressed chunk");

        for (unsigned int i = 0; i < sample_count; ++i) {
            Sample sample = { .timestamp = i + 1, .value = 1.0 };
            mu_assert(Uncompressed_AddSample(chunk, &sample) == CR_OK, "fill large chunk");
        }

        UpsertCtx upsert = {
            .inChunk = chunk,
            .sample = { .timestamp = sample_count + 1, .value = 2.0 },
        };
        int size = 0;

        mu_assert(Uncompressed_UpsertSample(&upsert, &size, DP_LAST) == CR_OK,
                  "upsert into large chunk");
        mu_assert_int_eq(1, size);
        mu_assert_int_eq(sample_count + 1, chunk->num_samples);
        mu_assert_int_eq(sample_count + 1, chunk->samples[sample_count].timestamp);
        Uncompressed_FreeChunk(chunk);
    }
}

MU_TEST(test_Uncompressed_Uncompressed_UpsertSample_DuplicatePolicy) {
    srand((unsigned int)time(NULL));
    const size_t chunk_size = 4096; // 4096 bytes (data) chunck
    Chunk *chunk = Uncompressed_NewChunk(chunk_size);
    mu_assert(chunk != NULL, "create uncompressed chunk");
    mu_assert_short_eq(0, chunk->num_samples);
    ChunkResult rv = CR_OK;
    Sample s1 = { .timestamp = 1, .value = -0.5 };
    Sample s2 = { .timestamp = 1, .value = -0.6 };
    rv = Uncompressed_AddSample(chunk, &s1);
    mu_assert(rv == CR_OK, "add sample");
    UpsertCtx uCtx = {
        .inChunk = chunk,
        .sample = s2,
    };

    int size = 0;
    // We're forcing the chunk to insert a duplicate and test different policies
    // DP_BLOCK should not change old sample
    rv = Uncompressed_UpsertSample(&uCtx, &size, DP_BLOCK);
    mu_assert(rv == CR_ERR, "duplicate block");
    mu_assert_int_eq(1, chunk->num_samples);
    const uint64_t firstTs = Uncompressed_GetFirstTimestamp(chunk);
    mu_assert_int_eq(1, firstTs);
    mu_assert_double_eq(-0.5, chunk->samples[0].value);
    // DP_MAX should keep -0.5 given that -0.4 is smaller
    uCtx.sample.value = -0.4;
    rv = Uncompressed_UpsertSample(&uCtx, &size, DP_MIN);
    mu_assert(rv == CR_OK, "duplicate min not changing old value");
    mu_assert_int_eq(1, chunk->num_samples);
    mu_assert_double_eq(-0.5, chunk->samples[0].value);
    // DP_MIN should replace -0.5 by -0.6
    uCtx.sample.value = -0.6;
    rv = Uncompressed_UpsertSample(&uCtx, &size, DP_MIN);
    mu_assert(rv == CR_OK, "duplicate min changing old value");
    mu_assert_int_eq(1, chunk->num_samples);
    mu_assert_double_eq(-0.6, chunk->samples[0].value);
    // DP_MAX should keep -0.6 given that -1 is smaller
    uCtx.sample.value = -1.0;
    rv = Uncompressed_UpsertSample(&uCtx, &size, DP_MAX);
    mu_assert(rv == CR_OK, "duplicate max not changing old value");
    mu_assert_double_eq(-0.6, chunk->samples[0].value);
    // DP_MAX should replace -0.6 by -0.2
    uCtx.sample.value = -0.2;
    rv = Uncompressed_UpsertSample(&uCtx, &size, DP_MAX);
    mu_assert(rv == CR_OK, "duplicate max changing old value");
    mu_assert_double_eq(-0.2, chunk->samples[0].value);
    Uncompressed_FreeChunk(chunk);
}

MU_TEST(test_reverseEnrichedChunk_multi_values_per_sample) {
    EnrichedChunk *ec = NewEnrichedChunk();
    ec->samples.values_per_sample = 2;
    ReallocSamplesArray(&ec->samples, 4);
    ec->samples.num_samples = 3;
    ec->samples.timestamps[0] = 10;
    ec->samples.timestamps[1] = 20;
    ec->samples.timestamps[2] = 30;
    Samples_value_at(&ec->samples, 0, 0) = 1.0;
    Samples_value_at(&ec->samples, 0, 1) = 2.0;
    Samples_value_at(&ec->samples, 1, 0) = 3.0;
    Samples_value_at(&ec->samples, 1, 1) = 4.0;
    Samples_value_at(&ec->samples, 2, 0) = 5.0;
    Samples_value_at(&ec->samples, 2, 1) = 6.0;

    reverseEnrichedChunk(ec);

    mu_assert_int_eq(30, (int)ec->samples.timestamps[0]);
    mu_assert_int_eq(20, (int)ec->samples.timestamps[1]);
    mu_assert_int_eq(10, (int)ec->samples.timestamps[2]);
    mu_assert_double_eq(5.0, Samples_value_at(&ec->samples, 0, 0));
    mu_assert_double_eq(6.0, Samples_value_at(&ec->samples, 0, 1));
    mu_assert_double_eq(3.0, Samples_value_at(&ec->samples, 1, 0));
    mu_assert_double_eq(4.0, Samples_value_at(&ec->samples, 1, 1));
    mu_assert_double_eq(1.0, Samples_value_at(&ec->samples, 2, 0));
    mu_assert_double_eq(2.0, Samples_value_at(&ec->samples, 2, 1));
    mu_assert(ec->rev == true, "rev flag set");

    FreeEnrichedChunk(ec);
}

MU_TEST(test_reverseEnrichedChunk_single_value_per_sample) {
    EnrichedChunk *ec = NewEnrichedChunk();
    ec->samples.values_per_sample = 1;
    ReallocSamplesArray(&ec->samples, 4);
    ec->samples.num_samples = 3;
    ec->samples.timestamps[0] = 10;
    ec->samples.timestamps[1] = 20;
    ec->samples.timestamps[2] = 30;
    Samples_value_at(&ec->samples, 0, 0) = 1.0;
    Samples_value_at(&ec->samples, 1, 0) = 2.0;
    Samples_value_at(&ec->samples, 2, 0) = 3.0;

    reverseEnrichedChunk(ec);

    mu_assert_int_eq(30, (int)ec->samples.timestamps[0]);
    mu_assert_int_eq(20, (int)ec->samples.timestamps[1]);
    mu_assert_int_eq(10, (int)ec->samples.timestamps[2]);
    mu_assert_double_eq(3.0, Samples_value_at(&ec->samples, 0, 0));
    mu_assert_double_eq(2.0, Samples_value_at(&ec->samples, 1, 0));
    mu_assert_double_eq(1.0, Samples_value_at(&ec->samples, 2, 0));

    FreeEnrichedChunk(ec);
}

MU_TEST(test_Uncompressed_MRDeserialize_validation) {
    Sample sample = { .timestamp = 1, .value = 2 };
    const size_t counts[] = { 1, 2, 1, SIZE_MAX };
    const size_t sizes[] = { SAMPLE_SIZE, SAMPLE_SIZE, 2 * SAMPLE_SIZE, SAMPLE_SIZE };
    for (size_t i = 0; i < 4; ++i) {
        mr_Buffer *buffer = mr_BufferNew(64);
        mr_BufferWriter writer;
        mr_BufferWriterInit(&writer, buffer);
        mr_BufferWriterWriteLongLong(&writer, sample.timestamp);
        mr_BufferWriterWriteLongLong(&writer, counts[i]);
        mr_BufferWriterWriteLongLong(&writer, sizes[i]);
        mr_BufferWriterWriteBuff(&writer, (const char *)&sample, sizeof(sample));
        mr_BufferReader reader;
        mr_BufferReaderInit(&reader, buffer);
        Chunk_t *chunk = NULL;
        mu_assert_int_eq(i == 0 ? TSDB_OK : TSDB_ERROR,
                         Uncompressed_MRDeserialize(&chunk, &reader));
        if (i == 0) {
            mu_assert(chunk != NULL, "valid chunk accepted");
            Uncompressed_FreeChunk(chunk);
        } else {
            mu_assert(chunk == NULL, "invalid chunk rejected");
        }
        mr_BufferFree(buffer);
    }
}

MU_TEST_SUITE(uncompressed_chunk_test_suite) {
    MU_RUN_TEST(test_Uncompressed_MRDeserialize_validation);
    MU_RUN_TEST(test_Uncompressed_NewChunk);
    MU_RUN_TEST(test_Uncompressed_Uncompressed_AddSample);
    MU_RUN_TEST(test_Uncompressed_Uncompressed_UpsertSample);
    MU_RUN_TEST(test_Uncompressed_UpsertSample_large_sample_count);
    MU_RUN_TEST(test_Uncompressed_Uncompressed_UpsertSample_DuplicatePolicy);
    MU_RUN_TEST(test_reverseEnrichedChunk_multi_values_per_sample);
    MU_RUN_TEST(test_reverseEnrichedChunk_single_value_per_sample);
}
