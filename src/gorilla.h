/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#ifndef GORILLA_H
#define GORILLA_H

#include <sys/types.h>      // u_int_t
#include <stdbool.h>        // bool
#include "consts.h"

typedef union {
    double d;
    int64_t i;
    u_int64_t u;
} union64bits;

typedef struct CompressedChunk {
    u_int64_t size;
    u_int64_t count;
    u_int64_t idx;

    union64bits baseValue;
    u_int64_t baseTimestamp;

    u_int64_t *data;
    
    u_int64_t prevTimestamp;
    int64_t prevTimestampDelta;

    union64bits prevValue;
    u_int8_t prevLeading;
    u_int8_t prevTrailing;
} CompressedChunk;

typedef struct CChunk_Iterator {
  CompressedChunk *chunk;
  u_int64_t idx;
  u_int64_t count;

  // timestamp vars
  u_int64_t prevTS;
  int64_t prevDelta;

  // value vars
  union64bits prevValue;  
  u_int8_t prevLeading;
  u_int8_t prevTrailing;
} CChunk_Iterator;

enum chunkResults {
  CC_OK = 0,    // RM_OK
  CC_ERR = 1,   // RM_ERR
  CC_END = 2  
};

int CChunk_Append(CompressedChunk *chunk, u_int64_t timestamp, double value);
int CChunk_ReadNext(CChunk_Iterator *iter, u_int64_t *timestamp, double *value);

#endif