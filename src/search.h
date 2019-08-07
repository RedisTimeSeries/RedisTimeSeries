/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
* This file is available under the Redis Labs Source Available License Agreement
*/

#ifndef __RS_LITE_H__
#define __RS_LITE_H__

#include <stdbool.h>

#include "indexer.h"
#include "redisearch_api.h"

typedef uint32_t count_t;
typedef uint8_t FieldType;
typedef struct GeoFilter GeoFilter;

typedef struct {
    char **fields;              // Indexed fields.
    uint32_t fields_count;          // Number of fields.
    RSIndex *idx;               // RediSearch full-text index.
} RSLiteIndex;

typedef struct {
  //Field string
  char *fieldStr;
  uint32_t fieldLen;
  
  //Full Text or Tag string
  char *valueStr;
  uint32_t valueLen;
  
  //Numeric value
  double dbl;

  //GEO values
  //GeoFilter *geo;

  FieldType RSFieldType;
  Label RTS_Label;
} RSLabel;

RSLiteIndex *RSLiteCreate(const char *name);

int RSL_Index(RSLiteIndex *,
           const char *item, uint32_t itemlen,
           RSLabel *labels, count_t count);

int RSL_Remove(RSLiteIndex *, const char *item, uint32_t itemlen);

/*
 * Returns an iterator with results.
 * Function RediSearch_ResultsIteratorNext should be used to iterate over
 * all results until INDEXREAD_EOF is reached.
 */
RSResultsIterator *RSL_GetQueryIter(RSLiteIndex *, 
                   const char *s, uint64_t n, 
                   char **err);

const char *RSL_IterateResults(RSResultsIterator *iter, size_t *len);

void FreeRSLabels(RSLabel *labels, size_t count, bool freeRMString);
Label *RSLabelToLabels(RSLabel *labels, size_t count);

#endif // __RS_LITE_H__