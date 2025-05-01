/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef COMPACTION_AVX512F_H
#define COMPACTION_AVX512F_H

void MaxAppendValuesAVX512F(void *__restrict__ context,
                            double *__restrict__ values,
                            size_t si,
                            size_t ei);

#endif // COMPACTION_AVX512F_H
