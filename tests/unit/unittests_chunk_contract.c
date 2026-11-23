/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "chunk.h"
#include "compressed_chunk.h"
#include "enriched_chunk.h"
#include "generic_chunk.h"
#include "minunit.h"

/* Bind directly to the unit binary's chunk functions, as the existing chunk
 * suites do. Its Redis allocator hooks are initialized by RMUTil_InitAlloc. */
#define CHUNK_CONTRACT_FUNCS(prefix, count)                                                        \
    { .NewChunk = prefix##_NewChunk,                                                               \
      .FreeChunk = prefix##_FreeChunk,                                                             \
      .CloneChunk = prefix##_CloneChunk,                                                           \
      .SplitChunk = prefix##_SplitChunk,                                                           \
      .DelRange = prefix##_DelRange,                                                               \
      .AddSample = prefix##_AddSample,                                                             \
      .ProcessChunk = prefix##_ProcessChunk,                                                       \
      .GetNumOfSample = count,                                                                     \
      .GetLastTimestamp = prefix##_GetLastTimestamp,                                               \
      .GetLastValue = prefix##_GetLastValue,                                                       \
      .GetFirstTimestamp = prefix##_GetFirstTimestamp }
static const ChunkFuncs contract_funcs[] = {
    CHUNK_CONTRACT_FUNCS(Uncompressed, Uncompressed_NumOfSample),
    CHUNK_CONTRACT_FUNCS(Compressed, Compressed_ChunkNumOfSample)
};
#undef CHUNK_CONTRACT_FUNCS

/* Test the same observable contract for both storage formats. Use public chunk
 * operations so the expected samples do not depend on either representation. */
MU_TEST(test_chunk_clone_delete_and_append) {
    for (int type = CHUNK_REGULAR; type <= CHUNK_COMPRESSED; type++) {
        const ChunkFuncs *funcs = &contract_funcs[type];
        Chunk_t *original = funcs->NewChunk(4096);
        for (int i = 1; i <= 9; i++) {
            Sample sample = { .timestamp = i * 10, .value = i * -0.5 };
            mu_assert_int_eq(CR_OK, funcs->AddSample(original, &sample));
        }
        Chunk_t *copy = funcs->CloneChunk(original);
        mu_assert(copy != original, "clone owns a separate chunk");
        mu_assert_int_eq(0, funcs->DelRange(copy, 31, 39));
        mu_assert_int_eq(3, funcs->DelRange(copy, 30, 50));
        mu_assert_int_eq(6, funcs->GetNumOfSample(copy));
        mu_assert_int_eq(9, funcs->GetNumOfSample(original));
        mu_assert_double_eq(-4.5, funcs->GetLastValue(copy));

        /* Destroy the source before reading the clone: a shallow copy fails here. */
        funcs->FreeChunk(original);
        EnrichedChunk *result = NewEnrichedChunk();
        ReallocSamplesArray(&result->samples, 16);
        funcs->ProcessChunk(copy, 0, 100, result, false);
        const timestamp_t expected[] = { 10, 20, 60, 70, 80, 90 };
        mu_assert_int_eq(6, result->samples.num_samples);
        for (size_t i = 0; i < 6; i++) {
            mu_assert_int_eq(expected[i], result->samples.timestamps[i]);
            mu_assert_double_eq(-0.05 * expected[i], Samples_value_at(&result->samples, i, 0));
        }
        Sample appended = { .timestamp = 100, .value = 42.0 };
        mu_assert_int_eq(CR_OK, funcs->AddSample(copy, &appended));
        mu_assert_int_eq(100, funcs->GetLastTimestamp(copy));
        mu_assert_double_eq(42.0, funcs->GetLastValue(copy));
        /* Remove the prefix and verify the first timestamp is updated. */
        mu_assert_int_eq(2, funcs->DelRange(copy, 0, 20));
        mu_assert_int_eq(60, funcs->GetFirstTimestamp(copy));
        mu_assert_int_eq(2, funcs->DelRange(copy, 90, 100));
        mu_assert_int_eq(80, funcs->GetLastTimestamp(copy));
        mu_assert_int_eq(3, funcs->DelRange(copy, 0, UINT64_MAX));
        mu_assert_int_eq(0, funcs->GetNumOfSample(copy));
        mu_assert_int_eq(0, funcs->GetFirstTimestamp(copy));
        funcs->ProcessChunk(copy, 0, 100, result, false);
        mu_assert_int_eq(0, result->samples.num_samples);
        mu_assert_int_eq(CR_OK, funcs->AddSample(copy, &appended));
        mu_assert_int_eq(100, funcs->GetFirstTimestamp(copy));
        mu_assert_double_eq(42.0, funcs->GetLastValue(copy));
        FreeEnrichedChunk(result);
        funcs->FreeChunk(copy);
    }
}

MU_TEST(test_chunk_split_and_bounded_reads) {
    for (int type = CHUNK_REGULAR; type <= CHUNK_COMPRESSED; type++) {
        const ChunkFuncs *funcs = &contract_funcs[type];
        for (int count = 8; count <= 9; count++) {
            Chunk_t *left = funcs->NewChunk(4096);
            for (int i = 1; i <= count; i++) {
                Sample sample = { .timestamp = i * 10, .value = i * 1.25 };
                mu_assert_int_eq(CR_OK, funcs->AddSample(left, &sample));
            }
            Chunk_t *right = funcs->SplitChunk(left);
            const int leftCount = count - count / 2;
            mu_assert_int_eq(leftCount, funcs->GetNumOfSample(left));
            mu_assert_int_eq(count / 2, funcs->GetNumOfSample(right));
            mu_assert_int_eq(leftCount * 10, funcs->GetLastTimestamp(left));
            mu_assert_int_eq((leftCount + 1) * 10, funcs->GetFirstTimestamp(right));
            EnrichedChunk *result = NewEnrichedChunk();
            ReallocSamplesArray(&result->samples, 16);
            for (int reverse = 0; reverse <= 1; reverse++) {
                funcs->ProcessChunk(left, 20, 40, result, reverse);
                mu_assert_int_eq(3, result->samples.num_samples);
                for (int i = 0; i < 3; i++) {
                    const int sampleIndex = reverse ? 4 - i : 2 + i;
                    mu_assert_int_eq(sampleIndex * 10, result->samples.timestamps[i]);
                    mu_assert_double_eq(sampleIndex * 1.25,
                                        Samples_value_at(&result->samples, i, 0));
                }
                /* Reusing an output buffer must clear stale samples on no match. */
                funcs->ProcessChunk(right, 0, 1, result, reverse);
                mu_assert_int_eq(0, result->samples.num_samples);
                funcs->ProcessChunk(right, 1000, 2000, result, reverse);
                mu_assert_int_eq(0, result->samples.num_samples);
                funcs->ProcessChunk(right, 80, 70, result, reverse);
                mu_assert_int_eq(0, result->samples.num_samples);
                funcs->ProcessChunk(right, 0, 100, result, reverse);
                mu_assert_int_eq(count / 2, result->samples.num_samples);
                for (int i = 0; i < count / 2; i++) {
                    const int sampleIndex = reverse ? count - i : leftCount + 1 + i;
                    mu_assert_int_eq(sampleIndex * 10, result->samples.timestamps[i]);
                    mu_assert_double_eq(sampleIndex * 1.25,
                                        Samples_value_at(&result->samples, i, 0));
                }
            }
            FreeEnrichedChunk(result);
            funcs->FreeChunk(left);
            funcs->FreeChunk(right);
        }
    }
}

MU_TEST_SUITE(chunk_contract_test_suite) {
    MU_RUN_TEST(test_chunk_clone_delete_and_append);
    MU_RUN_TEST(test_chunk_split_and_bounded_reads);
}
