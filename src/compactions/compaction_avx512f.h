/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */
#ifndef COMPACTION_AVX512F_H
#define COMPACTION_AVX512F_H

void MaxAppendValuesAVX512F(void *__restrict__ context,
                            double *__restrict__ values,
                            size_t si,
                            size_t ei);

#endif // COMPACTION_AVX512F_H
