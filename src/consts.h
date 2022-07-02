/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#ifndef CONSTS_H
#define CONSTS_H

#include "redismodule.h"


#include <sys/types.h>
#include <stdbool.h>
#include <string.h>
#include <ctype.h>

  #if defined(__GNUC__)
#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)
  #elif _MSC_VER
#define likely(x)       (x)
#define unlikely(x)     (x)
  #endif

#define TRUE 1
#define FALSE 0

#ifndef really_inline
#define really_inline __attribute__((always_inline)) inline
#endif // really_inline

#ifndef __unused
#define __unused __attribute__((unused))
#endif

#define SAMPLE_SIZE sizeof(Sample)

#define timestamp_t u_int64_t
#define api_timestamp_t u_int64_t
#define TSDB_ERR_TIMESTAMP_TOO_OLD -1
#define TSDB_OK 0
#define TSDB_ERROR -1
#define TSDB_NOTEXISTS 2
#define TSDB_ERR_TIMESTAMP_OCCUPIED -2

/* TS.CREATE Defaults */
#define RETENTION_TIME_DEFAULT          0LL
#define Chunk_SIZE_BYTES_SECS           4096LL   // fills one page 4096
#define SPLIT_FACTOR                    1.2
#define DEFAULT_DUPLICATE_POLICY        DP_BLOCK

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
    TS_AGG_TWA,
    TS_AGG_TYPES_MAX // 13
} TS_AGG_TYPES_T;


typedef enum DuplicatePolicy {
    DP_INVALID = -1,
    DP_NONE = 0,
    DP_BLOCK = 1,
    DP_LAST = 2,
    DP_FIRST = 3,
    DP_MIN = 4,
    DP_MAX = 5,
    DP_SUM = 6,
} DuplicatePolicy;

/* Series struct options */
#define SERIES_OPT_UNCOMPRESSED 0x1

#define SERIES_OPT_COMPRESSED_GORILLA 0x2

#define SERIES_OPT_DEFAULT_COMPRESSION SERIES_OPT_COMPRESSED_GORILLA

/* Chunk enum */
typedef enum {
  CR_OK = 0,          // RM_OK
  CR_ERR_TS = 1,      // Error inserting time stamps
  CR_ERR_VALUES = 2,  // Error inserting values
  CR_ERR = 3,         // RM_ERR
  CR_END = 4,         // END_OF_CHUNK
} ChunkResult;

/* parsing */

#define DUPLICATE_POLICY_ARG "DUPLICATE_POLICY"
#define TS_ADD_DUPLICATE_POLICY_ARG "ON_DUPLICATE"
#define UNCOMPRESSED_ARG_STR "uncompressed"
#define COMPRESSED_GORILLA_ARG_STR "compressed"

// DC - Don't Care (Arbitrary value) 
#define DC 0

#define SAMPLES_TO_BYTES(size) (size * sizeof(Sample))

#define min(a,b) (((a)<(b))?(a):(b))

#define max(a,b) (((a)>(b))?(a):(b))

#define __SWAP(x,y) do {  \
  typeof(x) _x = x;      \
  typeof(y) _y = y;      \
  x = _y;                \
  y = _x;                \
} while(0)

static inline int RMStringStrCmpUpper(RedisModuleString *rm_str, const char *str) {
    size_t str_len;
    const char *rm_str_cstr = RedisModule_StringPtrLen(rm_str, &str_len);
    char input_upper[str_len + 1];
    for (int i = 0; i < str_len; i++) {
        input_upper[i] = toupper(rm_str_cstr[i]);
    }
    input_upper[str_len] = '\0';
    return strcmp(input_upper, str);
}

#endif
