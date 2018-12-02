#include <string.h>
#include <rmutil/alloc.h>
#include <rmutil/vector.h>

#include "consts.h"
#include "indexer.h"

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

void IndexOperation(RedisModuleCtx *ctx, const char *op, RedisModuleString *ts_key, Label *labels, size_t labels_count) {
    RedisModuleCallReply *reply;

    for (int i=0; i<labels_count; i++) {
        size_t _s;
        RedisModuleString *indexed_key_value = RedisModule_CreateStringPrintf(ctx, "__index_%s=%s",
                RedisModule_StringPtrLen(labels[i].key, &_s),
                RedisModule_StringPtrLen(labels[i].value, &_s));
        RedisModuleString *indexed_key = RedisModule_CreateStringPrintf(ctx, "__index_%s",
                RedisModule_StringPtrLen(labels[i].key, &_s),
                RedisModule_StringPtrLen(labels[i].value, &_s));
        reply = RedisModule_Call(ctx, op, "ss", indexed_key_value, ts_key);
        if (reply)
            RedisModule_FreeCallReply(reply);
        reply = RedisModule_Call(ctx, op, "ss", indexed_key, ts_key);
        if (reply)
            RedisModule_FreeCallReply(reply);
    }
}

void IndexMetric(RedisModuleCtx *ctx, RedisModuleString *ts_key, Label *labels, size_t labels_count) {
    IndexOperation(ctx, "SADD", ts_key, labels, labels_count);
}

void RemoveIndexedMetric(RedisModuleCtx *ctx, RedisModuleString *ts_key, Label *labels, size_t labels_count) {
    IndexOperation(ctx, "SREM", ts_key, labels, labels_count);
}

int _intersect(RedisModuleDict *left, RedisModuleDict *right, char **lastKey, size_t *lastKeySize) {
    RedisModuleDictIter *iter;
    if (*lastKey == NULL) {
        iter = RedisModule_DictIteratorStartC(left, "^", NULL, 0);
    } else {
        iter = RedisModule_DictIteratorStartC(left, "^", lastKey, *lastKeySize);
    }

    char *currentKey;
    size_t currentKeyLen;
    while((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        int doesNotExist = 0;
        RedisModule_DictGetC(right, currentKey, currentKeyLen, &doesNotExist);
        if (doesNotExist == 0) {
            continue;
        }
        *lastKey = currentKey;
        *lastKeySize = currentKeyLen;
        RedisModule_DictDelC(left, currentKey, currentKeyLen, NULL);
        RedisModule_DictIteratorStop(iter);
        return 1;
    }
    RedisModule_DictIteratorStop(iter);
    return 0;
}

int _difference(RedisModuleDict *left, RedisModuleDict *right, char **lastKey, size_t *lastKeySize) {
    RedisModuleDictIter *iter;
    if (*lastKey == NULL) {
        iter = RedisModule_DictIteratorStartC(right, "^", NULL, 0);
    } else {
        iter = RedisModule_DictIteratorStartC(right, "^", lastKey, *lastKeySize);
    }

    char *currentKey;
    size_t currentKeyLen;
    while((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        int doesNotExist = 0;
        RedisModule_DictGetC(left, currentKey, currentKeyLen, &doesNotExist);
        if (doesNotExist == 1) {
            continue;
        }
        *lastKey = currentKey;
        *lastKeySize = currentKeyLen;
        RedisModule_DictDelC(left, currentKey, currentKeyLen, NULL);
        RedisModule_DictIteratorStop(iter);
        return 1;
    }
    RedisModule_DictIteratorStop(iter);
    return 0;
}

RedisModuleDict * QueryIndexPredicate(RedisModuleCtx *ctx, QueryPredicate *predicate, RedisModuleDict *prevResults) {
    RedisModuleDict *localResult = RedisModule_CreateDict(ctx);
    RedisModuleCallReply *reply;
    size_t _s;


    if (predicate->type == NCONTAINS) {
        RedisModuleString *index_key = RedisModule_CreateStringPrintf(ctx, "__index_%s",
                RedisModule_StringPtrLen(predicate->label.key, &_s));
        reply = RedisModule_Call(ctx, "SMEMBERS", "s", index_key);
    } else {
        RedisModuleString *index_key_value = RedisModule_CreateStringPrintf(ctx, "__index_%s=%s",
                                                                            RedisModule_StringPtrLen(predicate->label.key, &_s),
                                                                            RedisModule_StringPtrLen(predicate->label.value, &_s));
        reply = RedisModule_Call(ctx, "SMEMBERS", "s", index_key_value);
    }
    size_t items = RedisModule_CallReplyLength(reply);

    for (size_t j=0; j < items; j++) {
        RedisModuleCallReply *item = RedisModule_CallReplyArrayElement(reply, j);
        RedisModuleString *key_name = RedisModule_CreateStringFromCallReply(item);
        RedisModule_DictSet(localResult, key_name, (void *)1);

    }

    if (reply)
        RedisModule_FreeCallReply(reply);

    if (prevResults != NULL) {
        char *lastKey = NULL;
        size_t lastKeySize;
        if (predicate->type == EQ) {
            while (_intersect(prevResults, localResult, &lastKey, &lastKeySize) != 0) {}
        } else  if (predicate->type == NCONTAINS) {
            while (_difference(prevResults, localResult, &lastKey, &lastKeySize) != 0) {}
        } else if (predicate->type == NEQ){
            while (_difference(prevResults, localResult, &lastKey, &lastKeySize) != 0) {}
        }
    } else if (predicate->type == EQ) {
        return localResult;
    } else {
        return NULL;
    }
}

RedisModuleDict * QueryIndex(RedisModuleCtx *ctx, QueryPredicate *index_predicate, size_t predicate_count) {
    RedisModuleDict *result = NULL;

    // EQ
    for (int i=0; i < predicate_count; i++) {
        if (index_predicate[i].type == EQ) {
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
