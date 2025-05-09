/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef INDEXER_H
#define INDEXER_H

#include "RedisModulesSDK/redismodule.h"

#include <stdint.h>
#include <stdbool.h>

typedef struct
{
    RedisModuleString *key;
    RedisModuleString *value;
} Label;

typedef enum
{
    EQ,
    NEQ,
    // Contains a label
    CONTAINS,
    // Not Contains a label
    NCONTAINS,
    LIST_MATCH,    // List of matching predicates
    LIST_NOTMATCH, // List of non-matching predicates
    // REQ,
    // NREQ
} PredicateType;

#define IS_INCLUSION(type) ((type) == EQ || (type) == CONTAINS || (type) == LIST_MATCH)

typedef struct QueryPredicate
{
    PredicateType type;
    RedisModuleString *key;
    RedisModuleString **valuesList;
    size_t valueListCount;
} QueryPredicate;

typedef struct QueryPredicateList
{
    QueryPredicate *list;
    size_t count;
    size_t ref;
} QueryPredicateList;

int parsePredicate(RedisModuleCtx *ctx,
                   const char *label_value_pair,
                   size_t label_value_pair_size,
                   QueryPredicate *retQuery,
                   const char *separator);
void QueryPredicate_Free(QueryPredicate *predicate, size_t count);
void QueryPredicateList_Free(QueryPredicateList *list);

void IndexInit();
int DefragIndex(RedisModuleDefragCtx *ctx);
void FreeLabels(void *value, size_t labelsCount);
void IndexMetric(RedisModuleString *ts_key, Label *labels, size_t labels_count);
void RemoveIndexedMetric(RedisModuleString *ts_key);
void RemoveAllIndexedMetrics();
void RemoveAllIndexedMetrics_generic(RedisModuleDict *_labelsIndex,
                                     RedisModuleDict **_tsLabelIndex);
int IsKeyIndexed(RedisModuleString *ts_key);
RedisModuleDict *QueryIndex(RedisModuleCtx *ctx,
                            QueryPredicate *index_predicate,
                            size_t predicate_count,
                            bool *hasPermissionError);

int CountPredicateType(QueryPredicateList *queries, PredicateType type);
#endif
