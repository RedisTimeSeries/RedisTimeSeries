/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
* This file is available under the Redis Labs Source Available License Agreement
*/

#ifndef __RS_LITE_H__
#define __RS_LITE_H__

#include <stdbool.h>

#include "indexer.h"
#include "redisearch_api.h"

#define DEFAULT_SIZE 1024

typedef uint32_t count_t;
typedef uint8_t FieldType;
typedef struct GeoFilter GeoFilter;

typedef struct {
    char **fields;              // Indexed fields.
    uint32_t fields_count;      // Number of fields.
    RSIndex *idx;               // RediSearch full-text index.
} RSLiteIndex;

typedef struct {
  FieldType RSFieldType;
   // Numeric and Geo values
  union
  {
    double dbl;
    GeoFilter *geo;
  } value;
  
  Label RTS_Label;     // two RedisModuleString for back compatibility
} RSLabel;

RSLiteIndex *RSLiteCreate(const char *name);

int RSL_Index(RSLiteIndex *fti, RedisModuleString *keyName,
              RSLabel *labels, count_t count);

int RSL_Remove(RSLiteIndex *, const char *item, uint32_t itemlen);

/*
 * Returns an iterator with results.
 * Function RediSearch_ResultsIteratorNext should be used to iterate over
 * all results until INDEXREAD_EOF is reached.
 */ /*
RSResultsIterator *RSL_GetQueryIter(RedisModuleCtx *ctx, RSLiteIndex *fti,
                                    RedisModuleString **argv, int start,
                                    int query_count);
RSResultsIterator *RSL_GetQueryFromNode(RSLiteIndex *fti, RSQNode *queryNode);
RSResultsIterator *RSL_GetQueryFromString(RSLiteIndex *fti, const char *s, 
                                          size_t n, char **err);
*/
const char *RSL_IterateResults(RSResultsIterator *iter, size_t *len);

//int RSL_RSQueryFromTSQuery(RedisModuleString **argv, int start, 
//                            char **queryStr, size_t *queryLen, int query_count);

RSResultsIterator * GetRSIter(RedisModuleString **argv, int count, char **err);

Label *RSLabelToLabels(Label *dest, RSLabel *labels, size_t count);
void FreeRSLabels(RSLabel *labels, size_t count, bool freeRMString);

int parseFieldsFromArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, 
                                size_t *label_count, RSLabel **rsLabels);

#endif // __RS_LITE_H__