#include "compaction_common.h"
#include <immintrin.h>

void MaxAppendValuesAVX512F(void *__restrict__ context,
                            double *__restrict__ values,
                            size_t si,
                            size_t ei) {
    if ((ei - si + 1) < VECTOR_SIZE * 2) {
        MaxAppendValuesVec(context, values, si, ei);
        return;
    }

    double *res = &((MaxMinContext *)context)->maxValue;

    // search in beginning
    while (si <= ei && !is_aligned(&values[si], CACHE_LINE_SIZE)) {
        _AssignIfGreater(res, &values[si]);
        ++si;
    }

    if (si > ei) {
        return;
    }

    if (ei - si + 1 >= VECTOR_SIZE * 2) {
        double vec[VECTOR_SIZE] __attribute__((aligned(CACHE_LINE_SIZE)));
        __m512d values_avx = _mm512_load_pd(&values[si]);
        _mm512_store_pd(vec, values_avx);
        __m512d res_avx = _mm512_load_pd(vec);

        size_t vec_ei = (ei - ((ei - si) % VECTOR_SIZE));
        si += VECTOR_SIZE;
        for (; si < vec_ei; si += VECTOR_SIZE) {
            values_avx = _mm512_load_pd(&values[si]);
            res_avx = _mm512_max_pd(res_avx, values_avx);
        }

        // find max in the vector
        _mm512_store_pd(vec, res_avx);
        for (int i = 0; i < VECTOR_SIZE; ++i) {
            _AssignIfGreater(res, &vec[i]);
        }
    }

    // search in the remainder
    for (; si <= ei; ++si) {
        _AssignIfGreater(res, &values[si]);
    }

    return;
}
