#ifndef INDEXER_H
#define INDEXER_H

#include <sys/types.h>
#include "redismodule.h"

typedef struct {

} Indexer;

typedef struct {
    RedisModuleString *key;
    RedisModuleString *value;
} Label;

typedef enum  {
    EQ,
    NEQ,
    // Contains a label
    CONTAINS,
    // Not Contains a label
    NCONTAINS,
    // REQ,
    // NREQ
} PredicateType;

typedef struct QueryPredicate {
    PredicateType type;
    Label label;
} QueryPredicate;

void IndexInit();
void IndexMetric(RedisModuleCtx *ctx, RedisModuleString *ts_key, Label *labels, size_t labels_count);
void RemoveIndexedMetric(RedisModuleCtx *ctx, RedisModuleString *ts_key, Label *labels, size_t labels_count);
RedisModuleDict *QueryIndex(RedisModuleCtx *ctx, QueryPredicate *index_predicate, size_t predicate_count);
int parseLabel(RedisModuleCtx *ctx, RedisModuleString *label, Label *retLabel, const char *separator);
int CountPredicateType(QueryPredicate *queries, size_t query_count, PredicateType type);
#endif
