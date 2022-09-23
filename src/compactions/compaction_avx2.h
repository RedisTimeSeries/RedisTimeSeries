/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#ifndef COMPACTION_AVX2_H
#define COMPACTION_AVX2_H

void MaxAppendValuesAVX2(void *__restrict__ context,
                         double *__restrict__ values,
                         size_t si,
                         size_t ei);

#endif // COMPACTION_AVX2_H
