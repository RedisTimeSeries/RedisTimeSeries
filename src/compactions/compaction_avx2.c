#include "compaction_common.h"
#include <immintrin.h>

void MaxAppendValuesAVX2(void *__restrict__ context,
                         double *__restrict__ values,
                         size_t si,
                         size_t ei) {
    if ((ei - si + 1) < VECTOR_SIZE_AVX2 * 2) {
        MaxAppendValuesVec(context, values, si, ei);
        return;
    }

    double *res = &((MaxMinContext *)context)->maxValue;

    // search in beginning
    while (si <= ei && !is_aligned(&values[si], ALIGN_SIZE_AVX2)) {
        _AssignIfGreater(res, &values[si]);
        ++si;
    }

    if (si > ei) {
        return;
    }

    if (ei - si + 1 >= VECTOR_SIZE_AVX2 * 2) {
        double vec[VECTOR_SIZE_AVX2] __attribute__((aligned(ALIGN_SIZE_AVX2)));
        __m256d values_avx = _mm256_load_pd(&values[si]);
        _mm256_store_pd(vec, values_avx);
        __m256d res_avx = _mm256_load_pd(vec);

        size_t vec_ei = (ei - ((ei - si) % VECTOR_SIZE_AVX2));
        si += VECTOR_SIZE_AVX2;
        for (; si < vec_ei; si += VECTOR_SIZE_AVX2) {
            values_avx = _mm256_load_pd(&values[si]);
            res_avx = _mm256_max_pd(res_avx, values_avx);
        }

        // find max in the vector
        _mm256_store_pd(vec, res_avx);
        for (int i = 0; i < VECTOR_SIZE_AVX2; ++i) {
            _AssignIfGreater(res, &vec[i]);
        }
    }

    // search in the remainder
    for (; si <= ei; ++si) {
        _AssignIfGreater(res, &values[si]);
    }

    return;
}
