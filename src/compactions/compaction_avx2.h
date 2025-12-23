/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef COMPACTION_AVX2_H
#define COMPACTION_AVX2_H

void MaxAppendValuesAVX2(void *__restrict__ context,
                         double *__restrict__ values,
                         size_t si,
                         size_t ei);

#endif // COMPACTION_AVX2_H
