#include "compaction_common.h"
#include <immintrin.h>

void MaxAppendValuesAVX512F(void *__restrict__ context,
                            double *__restrict__ values,
                            size_t si,
                            size_t ei) {
    double *res = &((MaxMinContext *)context)->maxValue;
    double vec[VECTOR_SIZE]
        __attribute__((aligned(8))) = { _DOUBLE_MOST_NEG, _DOUBLE_MOST_NEG, _DOUBLE_MOST_NEG,
                                        _DOUBLE_MOST_NEG, _DOUBLE_MOST_NEG, _DOUBLE_MOST_NEG,
                                        _DOUBLE_MOST_NEG, _DOUBLE_MOST_NEG };

    // search in beginning
    while (si <= ei && !is_aligned(&values[si], CACHE_LINE_SIZE)) {
        _AssignIfGreater(res, &values[si]);
        ++si;
    }

    if (si > ei) {
        return;
    }

    __m512d res_avx = _mm512_load_pd(vec);
    __m512d values_avx;
    size_t vec_ei = (ei - si < VECTOR_SIZE - 1) ? 0 : (ei - ((ei + 1) % VECTOR_SIZE));
    for (; si < vec_ei; si += VECTOR_SIZE) {
        values_avx = _mm512_load_pd(&values[si]);
        res_avx = _mm512_max_pd(res_avx, values_avx);
    }

    // find max in the vector
    _mm512_store_pd(vec, res_avx);
    for (int i = 0; i < VECTOR_SIZE; ++i) {
        _AssignIfGreater(res, &vec[i]);
    }

    // search in end
    for (; si <= ei; ++si) {
        _AssignIfGreater(res, &values[si]);
    }

    return;
}
