/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#ifndef COMPACTION_AVX512F_H
#define COMPACTION_AVX512F_H

void MaxAppendValuesAVX512F(void *__restrict__ context,
                            double *__restrict__ values,
                            size_t si,
                            size_t ei);

#endif // COMPACTION_AVX512F_H
