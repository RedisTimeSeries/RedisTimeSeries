/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "chunk.h"
#include "compaction.h"
#include "consts.h"
#include "enriched_chunk.h"
#include "minunit.h"
#include "parse_policies.h"
#include "tsdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

MU_TEST(test_Uncompressed_DelRange_DeleteAll) {
    // Test for CVE fix: uninitialized heap read when deleting all samples
    const size_t chunk_size = 4096;
    Chunk *chunk = Uncompressed_NewChunk(chunk_size);
    mu_assert(chunk != NULL, "create uncompressed chunk");

    // Add some samples
    Sample s1 = { .timestamp = 100, .value = 1.0 };
    Sample s2 = { .timestamp = 200, .value = 2.0 };
    Sample s3 = { .timestamp = 300, .value = 3.0 };
    Uncompressed_AddSample(chunk, &s1);
    Uncompressed_AddSample(chunk, &s2);
    Uncompressed_AddSample(chunk, &s3);
    mu_assert_int_eq(3, chunk->num_samples);
    mu_assert_long_eq(100, chunk->base_timestamp);

    // Delete all samples in range [100, 300]
    size_t deleted = Uncompressed_DelRange(chunk, 100, 300);
    mu_assert_int_eq(3, deleted);
    mu_assert_int_eq(0, chunk->num_samples);
    // After fix: base_timestamp should be 0 (safe), not uninitialized
    mu_assert_long_eq(0, chunk->base_timestamp);
    Uncompressed_FreeChunk(chunk);
}

MU_TEST(test_Uncompressed_DelRange_DeletePartial) {
    const size_t chunk_size = 4096;
    Chunk *chunk = Uncompressed_NewChunk(chunk_size);
    mu_assert(chunk != NULL, "create uncompressed chunk");

    // Add some samples
    Sample s1 = { .timestamp = 100, .value = 1.0 };
    Sample s2 = { .timestamp = 200, .value = 2.0 };
    Sample s3 = { .timestamp = 300, .value = 3.0 };
    Uncompressed_AddSample(chunk, &s1);
    Uncompressed_AddSample(chunk, &s2);
    Uncompressed_AddSample(chunk, &s3);

    // Delete only middle sample [200, 200]
    size_t deleted = Uncompressed_DelRange(chunk, 200, 200);
    mu_assert_int_eq(1, deleted);
    mu_assert_int_eq(2, chunk->num_samples);
    // base_timestamp should be from first remaining sample (100)
    mu_assert_long_eq(100, chunk->base_timestamp);
    mu_assert_long_eq(100, chunk->samples[0].timestamp);
    mu_assert_long_eq(300, chunk->samples[1].timestamp);
    Uncompressed_FreeChunk(chunk);
}

// Regression test for the info-leak primitive (HackerOne #3713815 / VDP-4667).
//
// Uncompressed_DelRange replaces chunk->samples with a freshly allocated
// `regChunk->size`-byte buffer but only copies new_count samples into it.
// Uncompressed_GenericSerialize later writes the *entire* `regChunk->size`
// buffer to the wire (DUMP / RDB), so any uninitialized trailing bytes leak
// heap memory to the client.
//
// To force the bug to manifest deterministically, we groom the same-size
// free list with a sentinel byte pattern. With the buggy malloc, the
// allocator inside DelRange returns one of those grooming buffers — still
// holding the sentinel — on Linux glibc/jemalloc (the deployment target).
// macOS libmalloc zeroes blocks on free as a hardening feature, so the
// fail-before-fix property only holds on Linux; on macOS the test passes
// either way and just documents the required invariant. With calloc the
// buffer is zeroed regardless of what the free list contained.
static int trailing_bytes_all_zero(const unsigned char *buf, size_t off, size_t end) {
    for (size_t i = off; i < end; i++) {
        if (buf[i] != 0) {
            return 0;
        }
    }
    return 1;
}

static void groom_size_class(size_t size, unsigned char sentinel, size_t fanout) {
    void **grooms = (void **)malloc(fanout * sizeof(void *));
    for (size_t i = 0; i < fanout; i++) {
        grooms[i] = malloc(size);
        memset(grooms[i], sentinel, size);
    }
    for (size_t i = 0; i < fanout; i++) {
        free(grooms[i]);
    }
    free(grooms);
}

MU_TEST(test_Uncompressed_DelRange_NoUninitLeak_DeleteAll) {
    const size_t chunk_size = 256;
    Chunk *chunk = Uncompressed_NewChunk(chunk_size);
    mu_assert(chunk != NULL, "create uncompressed chunk");

    Sample s = { .timestamp = 1, .value = 1.0 };
    Uncompressed_AddSample(chunk, &s);

    // Plant a non-zero sentinel pattern in the matching size-class free
    // list. Subsequent malloc(chunk_size) inside DelRange very likely
    // returns one of these freshly-freed buffers (LIFO free list on
    // libc malloc / jemalloc / tcmalloc).
    groom_size_class(chunk_size, 0xAB, 32);

    size_t deleted = Uncompressed_DelRange(chunk, 0, UINT64_MAX);
    mu_assert_int_eq(1, deleted);
    mu_assert_int_eq(0, chunk->num_samples);

    // After the fix: entire `size`-byte buffer is zero (calloc).
    // Without the fix: at least one byte is the sentinel 0xAB.
    const unsigned char *buf = (const unsigned char *)chunk->samples;
    mu_assert(trailing_bytes_all_zero(buf, 0, chunk_size),
              "samples buffer must be fully zeroed after DelRange "
              "(otherwise uninitialized heap leaks via RDB/DUMP)");
    Uncompressed_FreeChunk(chunk);
}

MU_TEST(test_Uncompressed_DelRange_NoUninitLeak_PartialDelete) {
    // Even when samples remain, the bytes between
    // num_samples*SAMPLE_SIZE and chunk->size are serialized verbatim
    // by Uncompressed_GenericSerialize, so they must also be zero.
    const size_t chunk_size = 256;
    Chunk *chunk = Uncompressed_NewChunk(chunk_size);
    mu_assert(chunk != NULL, "create uncompressed chunk");

    Sample s1 = { .timestamp = 100, .value = 1.5 };
    Sample s2 = { .timestamp = 200, .value = 2.5 };
    Sample s3 = { .timestamp = 300, .value = 3.5 };
    Uncompressed_AddSample(chunk, &s1);
    Uncompressed_AddSample(chunk, &s2);
    Uncompressed_AddSample(chunk, &s3);

    groom_size_class(chunk_size, 0xCD, 32);

    size_t deleted = Uncompressed_DelRange(chunk, 200, 200);
    mu_assert_int_eq(1, deleted);
    mu_assert_int_eq(2, chunk->num_samples);

    const unsigned char *buf = (const unsigned char *)chunk->samples;
    mu_assert(trailing_bytes_all_zero(buf, 2 * SAMPLE_SIZE, chunk_size),
              "trailing bytes after remaining samples must be zeroed "
              "(otherwise uninitialized heap leaks via RDB/DUMP)");
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

MU_TEST_SUITE(uncompressed_chunk_test_suite) {
    MU_RUN_TEST(test_Uncompressed_NewChunk);
    MU_RUN_TEST(test_Uncompressed_Uncompressed_AddSample);
    MU_RUN_TEST(test_Uncompressed_Uncompressed_UpsertSample);
    MU_RUN_TEST(test_Uncompressed_Uncompressed_UpsertSample_DuplicatePolicy);
    MU_RUN_TEST(test_Uncompressed_DelRange_DeleteAll);
    MU_RUN_TEST(test_Uncompressed_DelRange_DeletePartial);
    MU_RUN_TEST(test_Uncompressed_DelRange_NoUninitLeak_DeleteAll);
    MU_RUN_TEST(test_Uncompressed_DelRange_NoUninitLeak_PartialDelete);
    MU_RUN_TEST(test_reverseEnrichedChunk_multi_values_per_sample);
    MU_RUN_TEST(test_reverseEnrichedChunk_single_value_per_sample);
}
