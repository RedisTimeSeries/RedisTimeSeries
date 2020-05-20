/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#ifndef CONSTS_H
#define CONSTS_H

#include <sys/types.h>
#include <stdbool.h>
#include <assert.h>

#define TRUE 1
#define FALSE 0

#define timestamp_t u_int64_t
#define api_timestamp_t u_int64_t
#define TSDB_ERR_TIMESTAMP_TOO_OLD -1
#define TSDB_OK 0
#define TSDB_ERROR -1
#define TSDB_NOTEXISTS 2
#define TSDB_ERR_TIMESTAMP_OCCUPIED -2

/* TS.CREATE Defaults */
#define RETENTION_TIME_DEFAULT          0LL
#define SAMPLES_PER_CHUNK_DEFAULT_SECS  256LL   // fills one page 4096
#define SPLIT_FACTOR                    1.2

/* TS.Range Aggregation types */
typedef enum {
    TS_AGG_INVALID = -1,
    TS_AGG_NONE = 0,
    TS_AGG_MIN,
    TS_AGG_MAX,
    TS_AGG_SUM,
    TS_AGG_AVG,
    TS_AGG_COUNT,
    TS_AGG_FIRST,
    TS_AGG_LAST,
    TS_AGG_RANGE,
    TS_AGG_STD_P,
    TS_AGG_STD_S,
    TS_AGG_VAR_P,
    TS_AGG_VAR_S,
    TS_AGG_TYPES_MAX // 13
} TS_AGG_TYPES_T;

/* Series struct options */
#define SERIES_OPT_UNCOMPRESSED (1 << 0)
#define SERIES_OPT_OUT_OF_ORDER (1 << 1)

/* Chunk enum */
typedef enum {
  CR_OK = 0,    // RM_OK
  CR_ERR = 1,   // RM_ERR
  CR_END = 2,   // END_OF_CHUNK
  CR_OCCUPIED = 3,
  CR_DEL_FAIL = 4,
} ChunkResult;

typedef enum {
  UPSERT_NOT_ADD = 0,
  UPSERT_ADD = 1,
  UPSERT_DEL = 2
} UpsertType;

#define SAMPLES_TO_BYTES(size) (size * sizeof(Sample))

#endif
