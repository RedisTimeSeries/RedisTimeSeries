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
        if (token == NULL) {
            return TSDB_ERROR;
        }
        if (i == 0) {
            retLabel->key = RedisModule_CreateString(NULL, token, strlen(token));
        } else {
            retLabel->value = RedisModule_CreateString(NULL, token, strlen(token));
        }
        token = strtok_r (NULL, "=", &iter_ptr);
    }
    return TSDB_OK;
}

void IndexOperation(RedisModuleCtx *ctx, const char *op, RedisModuleString *ts_key, Label *labels, size_t labels_count) {
    RedisModuleCallReply *reply;

    for (int i=0; i<labels_count; i++) {
        size_t _s;
        RedisModuleString *sk = RedisModule_CreateStringPrintf(ctx, "__index_%s=%s",
                RedisModule_StringPtrLen(labels[i].key, &_s),
                RedisModule_StringPtrLen(labels[i].value, &_s));
        reply = RedisModule_Call(ctx, op, "ss", sk, ts_key);
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


// Go over the results dictionary (dictEq) and remove keys that didn't match on all equal matchers
int _remove_not_intersected(int eqCount, RedisModuleDict *dictEq, char **lastKey, size_t *lastKeySize) {
    RedisModuleDictIter *iter;
    if (*lastKey == NULL) {
        iter = RedisModule_DictIteratorStartC(dictEq, "^", NULL, 0);
    } else {
        iter = RedisModule_DictIteratorStartC(dictEq, "^", lastKey, *lastKeySize);
    }

    char *currentKey;
    size_t currentKeyLen;
    uintptr_t currentCount = 0;
    while((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, &currentCount)) != NULL) {
        if (currentCount == eqCount) {
            continue;
        }
        *lastKey = currentKey;
        *lastKeySize = currentKeyLen;
        RedisModule_DictDelC(dictEq, currentKey, currentKeyLen, NULL);
        RedisModule_DictIteratorStop(iter);
        return 1;
    }
    RedisModule_DictIteratorStop(iter);
    return 0;
}

RedisModuleDict * QueryIndex(RedisModuleCtx *ctx, QueryPredicate *index_predicate, size_t predicate_count) {
    int eqCount = 0;
    RedisModuleDict *dictEq = RedisModule_CreateDict(ctx);
    RedisModuleDict *dictNeq = RedisModule_CreateDict(ctx);
    for (int i=0; i < predicate_count; i++) {
        size_t _s;
        RedisModuleString *index_key = RedisModule_CreateStringPrintf(ctx, "__index_%s=%s",
                                                                      RedisModule_StringPtrLen(index_predicate[i].label.key, &_s),
                                                                      RedisModule_StringPtrLen(index_predicate[i].label.value, &_s));

        RedisModuleCallReply *reply = RedisModule_Call(ctx, "SMEMBERS", "s", index_key);
        if (index_predicate[i].type == EQ) {
            eqCount++;
        }

        size_t items = RedisModule_CallReplyLength(reply);
        for (size_t j=0; j < items; j++) {
            RedisModuleCallReply *item = RedisModule_CallReplyArrayElement(reply, j);
            RedisModuleString *key_name = RedisModule_CreateStringFromCallReply(item);
            if (index_predicate[i].type == EQ) {
                int nokey;
                long long lastCount;
                lastCount = (int) RedisModule_DictGet(dictEq, key_name, &nokey);
                if (nokey == 1) {
                    RedisModule_DictSet(dictEq, key_name, (void *)1);
                } else {
                    RedisModule_DictReplace(dictEq, key_name, (void *)(lastCount + 1));
                }
            } else {
                RedisModule_DictSet(dictNeq, key_name, NULL);
            }
        }

        if (reply)
            RedisModule_FreeCallReply(reply);
    }

    // Remove all keys that intersect with the NEQ query
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(dictNeq, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;
    while((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModule_DictDelC(dictEq, currentKey, currentKeyLen, NULL);
    }

    RedisModule_DictIteratorStop(iter);

    // Keep only intersected keys
    char *lastKey = NULL;
    size_t lastKeySize;
    while (_remove_not_intersected(eqCount, dictEq, &lastKey, &lastKeySize) != 0){

    }

    return dictEq;
}
