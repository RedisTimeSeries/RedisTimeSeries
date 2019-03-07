/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#include <string.h>
#include <rmutil/alloc.h>
#include <rmutil/vector.h>

#include "consts.h"
#include "indexer.h"

RedisModuleDict *labelsIndex;

#define KV_PREFIX "__index_%s=%s"
#define K_PREFIX "__key_index_%s"


void IndexInit() {
    labelsIndex = RedisModule_CreateDict(NULL);
}

int parseLabel(RedisModuleCtx *ctx, RedisModuleString *label, Label *retLabel, const char *separator) {
    char *token;
    char *iter_ptr;
    size_t _s;
    const char *labelRaw = RedisModule_StringPtrLen(label, &_s);
    char *labelstr = RedisModule_PoolAlloc(ctx, _s + 1);
    labelstr[_s] = '\0';
    strncpy(labelstr, labelRaw, _s);
    token = strtok_r(labelstr, separator, &iter_ptr);
    for (int i=0; i<2; i++)
    {
        if (token == NULL && i==0) {
            return TSDB_ERROR;
        }
        if (i == 0) {
            retLabel->key = RedisModule_CreateString(NULL, token, strlen(token));
        } else {
            if (token != NULL) {
                retLabel->value = RedisModule_CreateString(NULL, token, strlen(token));
            } else {
                retLabel->value = NULL;
            }
        }
        token = strtok_r (NULL, separator, &iter_ptr);
    }
    return TSDB_OK;
}

int CountPredicateType(QueryPredicate *queries, size_t query_count, PredicateType type) {
    int count = 0;
    for (int i=0; i<query_count; i++) {
        if (queries[i].type == type) {
        count++;
        }
    }
    return count;
}

void indexUnderKey(const char *op, RedisModuleString *key, RedisModuleString *ts_key) {
    int nokey = 0;
    RedisModuleDict *leaf = RedisModule_DictGet(labelsIndex, key, &nokey);
    if (nokey) {
        leaf = RedisModule_CreateDict(NULL);
        RedisModule_DictSet(labelsIndex, key, leaf);
    }

    if (strcmp(op, "SADD") == 0) {
        RedisModule_DictSet(leaf, ts_key, NULL);
    } else if (strcmp(op, "SREM") == 0) {
        RedisModule_DictDel(leaf, ts_key, NULL);
    }
}

void IndexOperation(RedisModuleCtx *ctx, const char *op, RedisModuleString *ts_key, Label *labels, size_t labels_count) {
    const char *key_string, *value_string;
    for (int i=0; i<labels_count; i++) {
        size_t _s;
        key_string = RedisModule_StringPtrLen(labels[i].key, &_s);
        value_string = RedisModule_StringPtrLen(labels[i].value, &_s);
        RedisModuleString *indexed_key_value = RedisModule_CreateStringPrintf(ctx, KV_PREFIX,
                                                                              key_string,
                                                                              value_string);
        RedisModuleString *indexed_key = RedisModule_CreateStringPrintf(ctx, K_PREFIX,
                                                                        key_string);

        indexUnderKey(op, indexed_key_value, ts_key);
        indexUnderKey(op, indexed_key, ts_key);
    }
}

void IndexMetric(RedisModuleCtx *ctx, RedisModuleString *ts_key, Label *labels, size_t labels_count) {
    IndexOperation(ctx, "SADD", ts_key, labels, labels_count);
}

void RemoveIndexedMetric(RedisModuleCtx *ctx, RedisModuleString *ts_key, Label *labels, size_t labels_count) {
    IndexOperation(ctx, "SREM", ts_key, labels, labels_count);
}

int _intersect(RedisModuleCtx *ctx, RedisModuleDict *left, RedisModuleDict *right, RedisModuleString **lastKey) {
    RedisModuleDictIter *iter;
    if (*lastKey == NULL) {
        iter = RedisModule_DictIteratorStartC(left, "^", NULL, 0);
    } else {
        iter = RedisModule_DictIteratorStart(left, ">=", *lastKey);
    }

    char *currentKey;
    size_t currentKeyLen;
    while((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        int doesNotExist = 0;
        RedisModule_DictGetC(right, currentKey, currentKeyLen, &doesNotExist);
        if (doesNotExist == 0) {
            continue;
        }
        *lastKey = RedisModule_CreateString(ctx, currentKey, currentKeyLen);
        RedisModule_DictDelC(left, currentKey, currentKeyLen, NULL);
        RedisModule_DictIteratorStop(iter);
        return 1;
    }
    RedisModule_DictIteratorStop(iter);
    return 0;
}

int _difference(RedisModuleCtx *ctx, RedisModuleDict *left, RedisModuleDict *right, RedisModuleString **lastKey) {
    RedisModuleDictIter *iter;
    if (*lastKey == NULL) {
        iter = RedisModule_DictIteratorStartC(right, "^", NULL, 0);
    } else {
        iter = RedisModule_DictIteratorStart(right, ">=", *lastKey);
    }

    char *currentKey;
    size_t currentKeyLen;
    while((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        int doesNotExist = 0;
        RedisModule_DictGetC(left, currentKey, currentKeyLen, &doesNotExist);
        if (doesNotExist == 1) {
            continue;
        }
        *lastKey = RedisModule_CreateString(ctx, currentKey, currentKeyLen);
        RedisModule_DictDelC(left, currentKey, currentKeyLen, NULL);
        RedisModule_DictIteratorStop(iter);
        return 1;
    }
    RedisModule_DictIteratorStop(iter);
    return 0;
}

RedisModuleDict * QueryIndexPredicate(RedisModuleCtx *ctx, QueryPredicate *predicate, RedisModuleDict *prevResults) {
    RedisModuleDict *localResult = RedisModule_CreateDict(ctx);
    RedisModuleDict *currentLeaf;
    RedisModuleString *index_key;
    size_t _s;


    if (predicate->type == NCONTAINS || predicate->type == CONTAINS) {
        index_key = RedisModule_CreateStringPrintf(ctx, K_PREFIX,
                RedisModule_StringPtrLen(predicate->label.key, &_s));

    } else {
        const char *key = RedisModule_StringPtrLen(predicate->label.key, &_s);
        const char *value = RedisModule_StringPtrLen(predicate->label.value, &_s);
        index_key = RedisModule_CreateStringPrintf(ctx, KV_PREFIX, key, value);
    }
    int nokey;
    currentLeaf = RedisModule_DictGet(labelsIndex, index_key, &nokey);
    if (nokey) {
        currentLeaf = NULL;
    } else {
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(currentLeaf, "^", NULL, 0);
        RedisModuleString *currentKey;
        while((currentKey = RedisModule_DictNext(ctx, iter, NULL)) != NULL) {
            RedisModule_DictSet(localResult, currentKey, (void *)1);
        }
        RedisModule_DictIteratorStop(iter);
    }

    if (prevResults != NULL) {
        RedisModuleString *lastKey = NULL;
        if (predicate->type == EQ || predicate->type == CONTAINS) {
            while (_intersect(ctx, prevResults, localResult, &lastKey) != 0) {}
        } else  if (predicate->type == NCONTAINS) {
            while (_difference(ctx, prevResults, localResult, &lastKey) != 0) {}
        } else if (predicate->type == NEQ){
            while (_difference(ctx, prevResults, localResult, &lastKey) != 0) {}
        }
        return prevResults;
    } else if (predicate->type == EQ) {
        return localResult;
    } else {
        return prevResults;
    }
}

RedisModuleDict * QueryIndex(RedisModuleCtx *ctx, QueryPredicate *index_predicate, size_t predicate_count) {
    RedisModuleDict *result = NULL;

    // EQ or Contains
    for (int i=0; i < predicate_count; i++) {
        if (index_predicate[i].type == EQ || index_predicate[i].type == CONTAINS) {
            result = QueryIndexPredicate(ctx, &index_predicate[i], result);
        }
    }

    // The next two types of queries are reducers so we run them after the matcher
    // NCONTAINS or NEQ
    for (int i=0; i < predicate_count; i++) {
        if (index_predicate[i].type == NCONTAINS || index_predicate[i].type == NEQ) {
            result = QueryIndexPredicate(ctx, &index_predicate[i], result);
        }
    }

    if (result == NULL) {
        return RedisModule_CreateDict(ctx);
    }
    return result;
}
