/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */
#ifndef COMPACTION_AVX2_H
#define COMPACTION_AVX2_H

void MaxAppendValuesAVX2(void *__restrict__ context,
                         double *__restrict__ values,
                         size_t si,
                         size_t ei);

#endif // COMPACTION_AVX2_H
