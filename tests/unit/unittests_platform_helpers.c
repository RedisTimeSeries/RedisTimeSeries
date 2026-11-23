/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/*
 * Platform helpers that production code runs only on some targets:
 * - src/endianconv.c: memrev16/32 and intrev16/32 run only on big-endian builds
 *   (memrev*ifbe / intrev*ifbe), so they are checked directly here.
 * - src/compactions/compaction_avx2.c: the AVX2 MAX aggregation must agree with the
 *   scalar MaxAppendValuesVec for every range and start alignment (x86-64 with AVX2 only).
 */

#include "endianconv.h"
#include "minunit.h"

#include <stdint.h>
#include <string.h>

MU_TEST(test_memrev) {
    unsigned char b16[] = { 0x01, 0x02 };
    const unsigned char r16[] = { 0x02, 0x01 };
    memrev16(b16);
    mu_check(memcmp(b16, r16, sizeof(b16)) == 0);

    unsigned char b32[] = { 0x01, 0x02, 0x03, 0x04 };
    const unsigned char r32[] = { 0x04, 0x03, 0x02, 0x01 };
    memrev32(b32);
    mu_check(memcmp(b32, r32, sizeof(b32)) == 0);

    unsigned char b64[] = { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
    const unsigned char r64[] = { 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01 };
    memrev64(b64);
    mu_check(memcmp(b64, r64, sizeof(b64)) == 0);

    // Reversing twice restores the original bytes.
    memrev16(b16);
    memrev16(b16);
    memrev32(b32);
    memrev32(b32);
    memrev64(b64);
    memrev64(b64);
    mu_check(memcmp(b16, r16, sizeof(b16)) == 0);
    mu_check(memcmp(b32, r32, sizeof(b32)) == 0);
    mu_check(memcmp(b64, r64, sizeof(b64)) == 0);
}

MU_TEST(test_intrev) {
    mu_check(intrev16(0x0102) == 0x0201);
    mu_check(intrev32(0x01020304) == 0x04030201);
    mu_check(intrev64(0x0102030405060708ULL) == 0x0807060504030201ULL);

    mu_check(intrev16(intrev16(0xABCD)) == 0xABCD);
    mu_check(intrev32(intrev32(0xDEADBEEF)) == 0xDEADBEEF);
    mu_check(intrev64(intrev64(0x0123456789ABCDEFULL)) == 0x0123456789ABCDEFULL);
}

#if defined(__x86_64__)
#include "compactions/compaction_common.h"
#include "compactions/compaction_avx2.h"

MU_TEST(test_max_avx2_matches_scalar) {
    if (!__builtin_cpu_supports("avx2")) {
        return;
    }
    // 37 and 64 are coprime, so the values are a permutation of -20..43 and the maximum of
    // each range sits at a different offset.
    double values[64] __attribute__((aligned(ALIGN_SIZE_AVX2)));
    for (int i = 0; i < 64; i++) {
        values[i] = (double)((i * 37) % 64) - 20;
    }
    for (size_t si = 0; si < 64; si++) {
        for (size_t ei = si; ei < 64; ei++) {
            MaxMinContext scalar = { .minValue = DBL_MAX, .maxValue = _DOUBLE_MIN };
            MaxMinContext avx2 = scalar;
            MaxAppendValuesVec(&scalar, values, si, ei);
            MaxAppendValuesAVX2(&avx2, values, si, ei);
            mu_check(avx2.maxValue == scalar.maxValue);
        }
    }
}
#endif // __x86_64__

MU_TEST_SUITE(platform_helpers_test_suite) {
    MU_RUN_TEST(test_memrev);
    MU_RUN_TEST(test_intrev);
#if defined(__x86_64__)
    MU_RUN_TEST(test_max_avx2_matches_scalar);
#endif
}
