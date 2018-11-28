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
    // REQ,
    // NREQ
} PredicateType;

typedef struct {
    PredicateType type;
    Label label;
} QueryPredicate;

void IndexMetric(RedisModuleCtx *ctx, RedisModuleString *ts_key, Label *labels, size_t labels_count);
void RemoveIndexedMetric(RedisModuleCtx *ctx, RedisModuleString *ts_key, Label *labels, size_t labels_count);
RedisModuleDict *QueryIndex(RedisModuleCtx *ctx, QueryPredicate *index_predicate, size_t predicate_count, size_t *result_count);
int parseLabel(RedisModuleString *label, Label *retLabel, const char *separator);
#endif
