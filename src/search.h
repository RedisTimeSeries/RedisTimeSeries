/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
* This file is available under the Redis Labs Source Available License Agreement
*/

#ifndef __RS_LITE_H__
#define __RS_LITE_H__

#include "indexer.h"
#include "field_spec.h"
#include "redisearch_api.h"

typedef uint32_t count_t;
typedef struct GeoFilter GeoFilter;

typedef struct {
    char **fields;              // Indexed fields.
    uint32_t fields_count;          // Number of fields.
    RSIndex *idx;               // RediSearch full-text index.
} RSLiteIndex;

typedef struct {
  //Field string
  char *field;
  uint32_t fieldlen;
  
  //Full Text or Tag string
  char *str;
  uint32_t strlen;
  
  //Numeric value
  double dbl;

  //GEO values
  GeoFilter *geo;

  FieldType RSFieldType;
} RSLabels;

static RSLiteIndex *globalRSIndex;

RSLiteIndex *RSLiteCreate(const char *name);

int AddDoc(RSLiteIndex *,
           char *item, uint32_t itemlen,
           RSLabels *labels, count_t count);

int DeleteKey(RSLiteIndex *,
              char *item, uint32_t itemlen,
              RSLabels *labels, count_t count);

/*
 * Returns an iterator with results.
 * Function RediSearch_ResultsIteratorNext should be used to iterate over
 * all results until INDEXREAD_EOF is reached.
 */
RSResultsIterator *QueryString(RSLiteIndex *, 
                   const char *s, uint64_t n, 
                   char **err);

#endif // __RS_LITE_H__