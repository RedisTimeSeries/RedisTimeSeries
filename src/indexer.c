/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "indexer.h"
#include "module.h"
#include "common.h"

#include "consts.h"
#include "utils/overflow.h"

#include <assert.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <rmutil/alloc.h>

#include "utils/strmap.h"

RedisModuleDict *labelsIndex;  // maps label to it's ts keys.
RedisModuleDict *tsLabelIndex; // maps ts_key to it's dict in labelsIndex
extern bool isReshardTrimming, isAsmTrimming, isAsmImporting;

#define KV_PREFIX "__index_%s=%s"
#define K_PREFIX "__key_index_%s"

typedef enum
{
    Indexer_Add = 0x1,
    Indexer_Remove = 0x2,
} INDEXER_OPERATION_T;

/* Module-owned (worker-friendly) in-memory index.
 *
 * labelsIndexMem: index_key(string) -> StrMap*(set of ts_key strings)
 * tsLabelIndexMem: ts_key(string) -> StrMap*(set of index_key strings)
 *
 * Protected by an RW lock so many readers (queries) can run concurrently. */
static pthread_rwlock_t labelsIndexMemLock = PTHREAD_RWLOCK_INITIALIZER;
static StrMap labelsIndexMem;   /* values: StrMap* */
static StrMap tsLabelIndexMem;  /* values: StrMap* */

static void FreeStrMapPtr(void *v) {
    StrMap *m = (StrMap *)v;
    if (!m) return;
    StrMap_Free(m, NULL);
    free(m);
}

static StrMap *GetOrCreateSet(StrMap *top, const char *key) {
    void *v = NULL;
    if (StrMap_Get(top, key, &v)) {
        return (StrMap *)v;
    }
    StrMap *set = malloc(sizeof(*set));
    if (!set) return NULL;
    StrMap_Init(set);
    if (StrMap_Set(top, key, set, FreeStrMapPtr) != 0) {
        StrMap_Free(set, NULL);
        free(set);
        return NULL;
    }
    return set;
}

static void MemIndex_Add(const char *ts_key_cstr, const char *index_key_cstr) {
    pthread_rwlock_wrlock(&labelsIndexMemLock);
    StrMap *ts_set = GetOrCreateSet(&labelsIndexMem, index_key_cstr);
    if (ts_set) {
        (void)StrMap_Set(ts_set, ts_key_cstr, NULL, NULL);
    }
    StrMap *lbl_set = GetOrCreateSet(&tsLabelIndexMem, ts_key_cstr);
    if (lbl_set) {
        (void)StrMap_Set(lbl_set, index_key_cstr, NULL, NULL);
    }
    pthread_rwlock_unlock(&labelsIndexMemLock);
}

static void MemIndex_Remove(const char *ts_key_cstr) {
    pthread_rwlock_wrlock(&labelsIndexMemLock);
    void *v = NULL;
    if (!StrMap_Get(&tsLabelIndexMem, ts_key_cstr, &v) || !v) {
        pthread_rwlock_unlock(&labelsIndexMemLock);
        return;
    }
    StrMap *lbl_set = (StrMap *)v;

    StrMapIter it = {0};
    void *dummy = NULL;
    const char *idx_key = NULL;
    while ((idx_key = StrMapIter_Next(lbl_set, &it, &dummy)) != NULL) {
        void *sv = NULL;
        if (StrMap_Get(&labelsIndexMem, idx_key, &sv) && sv) {
            StrMap *ts_set = (StrMap *)sv;
            (void)StrMap_Del(ts_set, ts_key_cstr, NULL);
            if (StrMap_Len(ts_set) == 0) {
                (void)StrMap_Del(&labelsIndexMem, idx_key, FreeStrMapPtr);
            }
        }
    }

    (void)StrMap_Del(&tsLabelIndexMem, ts_key_cstr, FreeStrMapPtr);
    pthread_rwlock_unlock(&labelsIndexMemLock);
}

void IndexInit() {
    labelsIndex = RedisModule_CreateDict(NULL);
    tsLabelIndex = RedisModule_CreateDict(NULL);

    StrMap_Init(&labelsIndexMem);
    StrMap_Init(&tsLabelIndexMem);
}

static int DefragIndexLeaf(RedisModuleDefragCtx *ctx,
                           void *data,
                           __unused unsigned char *key,
                           __unused size_t keylen,
                           void **newptr) {
    static RedisModuleString *seekTo = NULL;
    *newptr = (void *)defragDict(ctx, (RedisModuleDict *)data, NULL, &seekTo);
    return (seekTo == NULL) ? DefragStatus_Finished : DefragStatus_Paused;
}

int DefragIndex(RedisModuleDefragCtx *ctx) {
    static RedisModuleString *seekTo = NULL;
    static RedisModuleDict **index = &labelsIndex;

    // can only defrag one index at a time
    *index = defragDict(ctx, *index, DefragIndexLeaf, &seekTo);
    if (seekTo != NULL) { // defrag paused
        return DefragStatus_Paused;
    }

    index = (index == &labelsIndex) ? &tsLabelIndex : &labelsIndex;
    if (index == &labelsIndex) { // defragged both indexes, done
        return DefragStatus_Finished;
    }

    return DefragIndex(ctx);
}

void FreeLabels(void *value, size_t labelsCount) {
    Label *labels = (Label *)value;
    for (int i = 0; i < labelsCount; ++i) {
        if (labels[i].key)
            RedisModule_FreeString(NULL, labels[i].key);
        if (labels[i].value)
            RedisModule_FreeString(NULL, labels[i].value);
    }
    free(labels);
}

static int parseValueList(char *token, size_t *count, RedisModuleString ***values) {
    char *iter_ptr;

    if (token == NULL) {
        return TSDB_ERROR;
    }

    const size_t token_len = strnlen(token, PTRDIFF_MAX);

    if (token[token_len - 1] == ')') {
        token[token_len - 1] = '\0'; // remove closing parentheses
    } else {
        return TSDB_ERROR;
    }

    size_t filterCount = 0;

    for (size_t i = 0; token[i] != '\0' && i < token_len; ++i) {
        if (token[i] == ',') {
            filterCount++;
        }
    }

    if (token_len <= 1) {
        // when the token is <=1 it means that we have an empty list
        *count = 0;
        *values = NULL;
        return TSDB_OK;
    } else if (filterCount == SIZE_MAX) {
        // Returning before the overflow.
        return TSDB_ERROR;
    } else {
        *count = filterCount + 1;
    }

    if (check_mul_overflow(*count, sizeof(RedisModuleString *))) {
        return TSDB_ERROR;
    }

    *values = calloc(*count, sizeof(RedisModuleString *));

    char *subToken = strtok_r(token, ",", &iter_ptr);
    for (int i = 0; i < *count; i++) {
        if (subToken == NULL) {
            return TSDB_ERROR;
        }
        (*values)[i] = RedisModule_CreateStringPrintf(NULL, "%s", subToken);
        subToken = strtok_r(NULL, ",", &iter_ptr);
    }
    return TSDB_OK;
}

int parsePredicate(RedisModuleCtx *ctx,
                   const char *label_value_pair,
                   size_t label_value_pair_size,
                   QueryPredicate *retQuery,
                   const char *separator) {
    char *token;
    char *iter_ptr;
    char *labelstr = malloc((label_value_pair_size + 1) * sizeof(char));
    labelstr[label_value_pair_size] = '\0';
    strncpy(labelstr, label_value_pair, label_value_pair_size);

    // Extract key
    token = strtok_r(labelstr, separator, &iter_ptr);
    if (token == NULL) {
        free(labelstr);
        return TSDB_ERROR;
    }
    retQuery->key = RedisModule_CreateString(NULL, token, strlen(token));
    retQuery->keyCStr = strdup(token);
    retQuery->valuesCStr = NULL;

    // Extract value
    token = strtok_r(NULL, separator, &iter_ptr);
    if (strstr(separator, "=(") != NULL) {
        if (parseValueList(token, &retQuery->valueListCount, &retQuery->valuesList) == TSDB_ERROR) {
            RedisModule_FreeString(NULL, retQuery->key);
            retQuery->key = NULL;
            free(retQuery->keyCStr);
            retQuery->keyCStr = NULL;
            free(labelstr);
            return TSDB_ERROR;
        }
    } else if (token != NULL) {
        retQuery->valueListCount = 1;
        retQuery->valuesList = malloc(sizeof(RedisModuleString *));
        retQuery->valuesList[0] = RedisModule_CreateString(NULL, token, strlen(token));
    } else {
        retQuery->valuesList = NULL;
        retQuery->valueListCount = 0;
    }

    if (retQuery->valueListCount > 0 && retQuery->valuesList) {
        retQuery->valuesCStr = calloc(retQuery->valueListCount, sizeof(char *));
        for (size_t i = 0; i < retQuery->valueListCount; i++) {
            if (!retQuery->valuesList[i]) continue;
            size_t slen = 0;
            const char *s = RedisModule_StringPtrLen(retQuery->valuesList[i], &slen);
            retQuery->valuesCStr[i] = strndup(s, slen);
        }
    }
    free(labelstr);
    return TSDB_OK;
}

int CountPredicateType(QueryPredicateList *queries, PredicateType type) {
    int count = 0;
    for (int i = 0; i < queries->count; i++) {
        if (queries->list[i].type == type) {
            count++;
        }
    }
    return count;
}

static inline void labelsIndexRemoveTsKey(RedisModuleDict *leaf,
                                          RedisModuleString *key,
                                          RedisModuleString *ts_key,
                                          RedisModuleDict *_labelsIndex) {
    RedisModule_DictDel(leaf, ts_key, NULL);
    if (RedisModule_DictSize(leaf) == 0) {
        RedisModule_FreeDict(NULL, leaf);
        RedisModule_DictDel(_labelsIndex, key, NULL);
    }
}

void labelIndexUnderKey(INDEXER_OPERATION_T op,
                        RedisModuleString *key,
                        RedisModuleString *ts_key,
                        RedisModuleDict *_labelsIndex,
                        RedisModuleDict *_tsLabelIndex) {
    int nokey = 0;
    RedisModuleDict *leaf = RedisModule_DictGet(_labelsIndex, key, &nokey);
    if (nokey) {
        leaf = RedisModule_CreateDict(NULL);
        RedisModule_DictSet(_labelsIndex, key, leaf);
    }

    RedisModuleDict *ts_leaf = RedisModule_DictGet(_tsLabelIndex, ts_key, &nokey);
    if (nokey) {
        ts_leaf = RedisModule_CreateDict(NULL);
        RedisModule_DictSet(_tsLabelIndex, ts_key, ts_leaf);
    }

    if (op & Indexer_Add) {
        RedisModule_DictSet(leaf, ts_key, NULL);
        RedisModule_DictSet(ts_leaf, key, NULL);
    } else if (op & Indexer_Remove) {
        labelsIndexRemoveTsKey(leaf, key, ts_key, _labelsIndex);
    }
}

void IndexMetric(RedisModuleString *ts_key, Label *labels, size_t labels_count) {
    const char *key_string, *value_string;
    size_t ts_len = 0;
    const char *ts_key_cstr = RedisModule_StringPtrLen(ts_key, &ts_len);
    for (int i = 0; i < labels_count; i++) {
        size_t _s;
        key_string = RedisModule_StringPtrLen(labels[i].key, &_s);
        value_string = RedisModule_StringPtrLen(labels[i].value, &_s);
        RedisModuleString *indexed_key_value =
            RedisModule_CreateStringPrintf(NULL, KV_PREFIX, key_string, value_string);
        RedisModuleString *indexed_key = RedisModule_CreateStringPrintf(NULL, K_PREFIX, key_string);

        labelIndexUnderKey(Indexer_Add, indexed_key_value, ts_key, labelsIndex, tsLabelIndex);
        labelIndexUnderKey(Indexer_Add, indexed_key, ts_key, labelsIndex, tsLabelIndex);

        /* Mirror to module-owned index for worker threads. */
        {
            char buf1[512];
            char buf2[512];
            snprintf(buf1, sizeof(buf1), KV_PREFIX, key_string, value_string);
            snprintf(buf2, sizeof(buf2), K_PREFIX, key_string);
            MemIndex_Add(ts_key_cstr, buf1);
            MemIndex_Add(ts_key_cstr, buf2);
        }

        RedisModule_FreeString(NULL, indexed_key_value);
        RedisModule_FreeString(NULL, indexed_key);
    }
}

// Removes the ts from the label index and from the inverse index, if exist.
// del_key should be false if caller wants to avoid iterator invalidation.
void RemoveIndexedMetric_generic(RedisModuleString *ts_key,
                                 RedisModuleDict *_labelsIndex,
                                 RedisModuleDict *_tsLabelIndex,
                                 bool del_key) {
    int nokey = 0;
    RedisModuleDict *ts_leaf = RedisModule_DictGet(_tsLabelIndex, ts_key, &nokey);
    if (nokey) { // series has no labels or already been removed from index
        return;
    }

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(ts_leaf, "^", NULL, 0);
    RedisModuleString *currentLabelKey;
    while ((currentLabelKey = RedisModule_DictNext(NULL, iter, NULL)) != NULL) {
        labelIndexUnderKey(Indexer_Remove, currentLabelKey, ts_key, _labelsIndex, _tsLabelIndex);
        RedisModule_FreeString(NULL, currentLabelKey);
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_FreeDict(NULL, ts_leaf);
    if (del_key) {
        RedisModule_DictDel(_tsLabelIndex, ts_key, NULL);
    }
}

// Removes the ts from the label index and from the inverse index, if exist.
void RemoveIndexedMetric(RedisModuleString *ts_key) {
    RemoveIndexedMetric_generic(ts_key, labelsIndex, tsLabelIndex, true);
    size_t ts_len = 0;
    const char *ts_key_cstr = RedisModule_StringPtrLen(ts_key, &ts_len);
    MemIndex_Remove(ts_key_cstr);
}

// Removes all indexed metrics
void RemoveAllIndexedMetrics_generic(RedisModuleDict *_labelsIndex,
                                     RedisModuleDict **_tsLabelIndex) {
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(*_tsLabelIndex, "^", NULL, 0);
    RedisModuleString *currentTSKey;
    while ((currentTSKey = RedisModule_DictNext(NULL, iter, NULL)) != NULL) {
        RemoveIndexedMetric_generic(currentTSKey, _labelsIndex, *_tsLabelIndex, false);
        RedisModule_FreeString(NULL, currentTSKey);
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_FreeDict(NULL, *_tsLabelIndex);
    *_tsLabelIndex = RedisModule_CreateDict(NULL);
}

void RemoveAllIndexedMetrics() {
    RemoveAllIndexedMetrics_generic(labelsIndex, &tsLabelIndex);
    pthread_rwlock_wrlock(&labelsIndexMemLock);
    StrMap_Free(&labelsIndexMem, FreeStrMapPtr);
    StrMap_Free(&tsLabelIndexMem, FreeStrMapPtr);
    StrMap_Init(&labelsIndexMem);
    StrMap_Init(&tsLabelIndexMem);
    pthread_rwlock_unlock(&labelsIndexMemLock);
}

int IsKeyIndexed(RedisModuleString *ts_key) {
    int nokey;
    RedisModule_DictGet(tsLabelIndex, ts_key, &nokey);
    return !nokey;
}

static uint64_t _calc_dicts_total_size(RedisModuleDict **dicts, size_t dict_size) {
    uint64_t total_size = 0;
    for (size_t i = 0; i < dict_size; i++) {
        if (dicts[i] != NULL) {
            total_size += RedisModule_DictSize(dicts[i]);
        }
    }

    return total_size;
}

void GetPredicateKeysDicts(RedisModuleCtx *ctx,
                           const QueryPredicate *predicate,
                           RedisModuleDict ***dicts,
                           size_t *dicts_size) {
    /*
     * Return the dictionary of all the keys that match the predicate.
     */
    RedisModuleString *index_key;
    size_t _s;
    const char *key = RedisModule_StringPtrLen(predicate->key, &_s);
    const char *value;

    if (predicate->type == NCONTAINS || predicate->type == CONTAINS) {
        *dicts = (RedisModuleDict **)malloc(sizeof(RedisModuleDict *));
        *dicts_size = 1;
        index_key = RedisModule_CreateStringPrintf(
            ctx, K_PREFIX, RedisModule_StringPtrLen(predicate->key, &_s));
        (*dicts)[0] = RedisModule_DictGet(labelsIndex, index_key, NULL);
        RedisModule_FreeString(ctx, index_key);

        return;
    }

    size_t to_allocate = 0;

    if (__builtin_mul_overflow(
            predicate->valueListCount, sizeof(RedisModuleDict *), &to_allocate)) {
        return;
    }

    // one or more entries
    *dicts = (RedisModuleDict **)malloc(to_allocate);
    *dicts_size = predicate->valueListCount;

    for (size_t i = 0; i < predicate->valueListCount; ++i) {
        value = RedisModule_StringPtrLen(predicate->valuesList[i], &_s);
        index_key = RedisModule_CreateStringPrintf(ctx, KV_PREFIX, key, value);
        (*dicts)[i] = RedisModule_DictGet(labelsIndex, index_key, NULL);
        RedisModule_FreeString(ctx, index_key);
    }
}

void PromoteSmallestPredicateToFront(RedisModuleCtx *ctx,
                                     QueryPredicate *index_predicate,
                                     size_t predicate_count) {
    /*
     * Find the predicate that has the minimal amount of keys that match to it, and move it to the
     * beginning of the predicate list so we will start our calculation from the smallest predicate.
     * This is an optimization, so we will copy the smallest dict possible.
     */
    if (predicate_count <= 1) {
        return;
    }

    int minIndex = 0;
    uint64_t minSize = UINT64_MAX;
    bool isCloned;
    uint64_t currentDictSize;
    RedisModuleDict **dicts = NULL;
    size_t dicts_size;
    for (size_t i = 0; i < predicate_count; ++i) {
        if (!IS_INCLUSION(index_predicate[i].type)) {
            // There is at least 1 inclusion predicate
            continue;
        }

        dicts_size = 0;
        GetPredicateKeysDicts(ctx, &index_predicate[i], &dicts, &dicts_size);
        uint64_t curSize = _calc_dicts_total_size(dicts, dicts_size);
        free(dicts);
        if (curSize < minSize) {
            minIndex = i;
            minSize = curSize;
        }
    }

    // switch between the minimal predicate and the predicate in the first place
    if (minIndex != 0) {
        __SWAP(index_predicate[minIndex], index_predicate[0]);
    }
}

static bool _isKeySatisfyAllPredicates(RedisModuleCtx *ctx,
                                       const char *currentKey,
                                       size_t currentKeyLen,
                                       const QueryPredicate *index_predicate,
                                       size_t predicate_count) {
    RedisModuleDict **dicts = NULL;
    size_t dicts_size;
    for (size_t i = 1; i < min(predicate_count, SIZE_MAX - 1); ++i) {
        bool inclusion = IS_INCLUSION(index_predicate[i].type);
        GetPredicateKeysDicts(ctx, &index_predicate[i], &dicts, &dicts_size);
        bool found = false;
        for (size_t j = 0; j < dicts_size; j++) {
            RedisModuleDict *curDict = dicts[j];
            if (!curDict) { // empty dict
                continue;
            }

            int doesNotExist = 0;
            RedisModule_DictGetC(curDict, (char *)currentKey, currentKeyLen, &doesNotExist);
            if (!doesNotExist) {
                found = true;
            }
        }
        free(dicts);

        if ((inclusion && !found) || (!inclusion && found)) {
            return false;
        }
    }

    return true;
}

static inline bool OwnKeyDuringSharding(
    RedisModuleString *key) { // RE version; during non-ASM reshards
    int slot = RedisModule_ShardingGetKeySlot(key);
    if (slot < 0) // sharding config not set
        return false;
    int firstSlot, lastSlot;
    RedisModule_ShardingGetSlotRange(&firstSlot, &lastSlot);
    return firstSlot <= slot && slot <= lastSlot;
}

static inline bool OwnKeyDuringASM(RedisModuleString *key) { // ASM version
    unsigned int slot = RedisModule_ClusterKeySlot(key);
    return RedisModule_ClusterCanAccessKeysInSlot(slot);
}

RedisModuleDict *QueryIndex(RedisModuleCtx *ctx,
                            QueryPredicate *index_predicate,
                            size_t predicate_count,
                            bool *hasPermissionError) {
    PromoteSmallestPredicateToFront(ctx, index_predicate, predicate_count);

    RedisModuleDict *res = RedisModule_CreateDict(ctx);
    QueryPredicate *predicate = &index_predicate[0];

    if (!IS_INCLUSION(predicate->type)) {
        return res;
    }

    RedisModuleDict **dicts = NULL;
    size_t dicts_size = 0;
    GetPredicateKeysDicts(ctx, &index_predicate[0], &dicts, &dicts_size);

    for (size_t i = 0; i < dicts_size; i++) {
        RedisModuleDict *dict = dicts[i];
        if (dict == NULL) {
            continue;
        }

        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(dict, "^", NULL, 0);
        char *currentKey = NULL;
        size_t currentKeyLen = 0;
        while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
            if (hasPermissionError) {
                if (!CheckKeyIsAllowedToReadC(ctx, currentKey, currentKeyLen)) {
                    *hasPermissionError = true;
                    continue;
                }
            }
            if (_isKeySatisfyAllPredicates(
                    ctx, currentKey, currentKeyLen, index_predicate, predicate_count)) {
                RedisModule_DictSetC(res, currentKey, currentKeyLen, (void *)1);
            }
        }
        RedisModule_DictIteratorStop(iter);
    }

    free(dicts);

    if (unlikely(isReshardTrimming || isAsmTrimming || isAsmImporting)) {
        // During those periods modules might see keys whose slots are no longer
        // (or not yet) owned by the current shard, so we need to filter them out of the results
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(res, "^", NULL, 0);
        RedisModuleString *currentKey;
        while ((currentKey = RedisModule_DictNext(NULL, iter, NULL)) != NULL) {
            bool ownCurrentKey =
                (isReshardTrimming ? OwnKeyDuringSharding : OwnKeyDuringASM)(currentKey);
            if (!ownCurrentKey) {
                RedisModule_DictDel(res, currentKey, NULL);
                RedisModule_DictIteratorReseek(iter, ">", currentKey);
            }
            RedisModule_FreeString(NULL, currentKey);
        }
        RedisModule_DictIteratorStop(iter);
    }

    return res;
}

void FreeQueryIndexKeys(char **keys, size_t count) {
    if (!keys) {
        return;
    }
    for (size_t i = 0; i < count; ++i) {
        free(keys[i]);
    }
    free(keys);
}

static StrMap *MemIndex_GetSetForKey(const char *index_key) {
    void *v = NULL;
    if (StrMap_Get(&labelsIndexMem, index_key, &v)) {
        return (StrMap *)v;
    }
    return NULL;
}

static int MemIndex_PredicateHasKey(const QueryPredicate *p, const char *ts_key) {
    char buf[512];
    if (p->type == CONTAINS || p->type == NCONTAINS) {
        snprintf(buf, sizeof(buf), K_PREFIX, p->keyCStr ? p->keyCStr : "");
        StrMap *set = MemIndex_GetSetForKey(buf);
        if (!set) return 0;
        return StrMap_Get(set, ts_key, NULL);
    }

    /* EQ / NEQ / LIST_*: check any of the values */
    for (size_t i = 0; i < p->valueListCount; ++i) {
        const char *v = (p->valuesCStr && p->valuesCStr[i]) ? p->valuesCStr[i] : "";
        snprintf(buf, sizeof(buf), KV_PREFIX, p->keyCStr ? p->keyCStr : "", v);
        StrMap *set = MemIndex_GetSetForKey(buf);
        if (!set) {
            continue;
        }
        if (StrMap_Get(set, ts_key, NULL)) {
            return 1;
        }
    }
    return 0;
}

static uint64_t MemIndex_PredicateTotalSize(const QueryPredicate *p) {
    char buf[512];
    uint64_t total = 0;
    if (!p || !p->keyCStr) return 0;
    if (p->type == CONTAINS || p->type == NCONTAINS) {
        snprintf(buf, sizeof(buf), K_PREFIX, p->keyCStr);
        StrMap *set = MemIndex_GetSetForKey(buf);
        return set ? (uint64_t)StrMap_Len(set) : 0;
    }
    for (size_t i = 0; i < p->valueListCount; ++i) {
        const char *v = (p->valuesCStr && p->valuesCStr[i]) ? p->valuesCStr[i] : "";
        snprintf(buf, sizeof(buf), KV_PREFIX, p->keyCStr, v);
        StrMap *set = MemIndex_GetSetForKey(buf);
        total += set ? (uint64_t)StrMap_Len(set) : 0;
    }
    return total;
}

static void MemIndex_ProcessSetCandidates(StrMap *set,
                                         const QueryPredicate *preds,
                                         size_t predicate_count,
                                         size_t seed,
                                         StrMap *seen,
                                         char ***out,
                                         size_t *out_count,
                                         size_t *cap) {
    if (!set) {
        return;
    }
    StrMapIter it = {0};
    void *dummy = NULL;
    const char *ts = NULL;
    while ((ts = StrMapIter_Next(set, &it, &dummy)) != NULL) {
        if (StrMap_Get(seen, ts, NULL)) {
            continue;
        }
        (void)StrMap_Set(seen, ts, NULL, NULL);

        int ok = 1;
        for (size_t j = 0; j < predicate_count; ++j) {
            if (j == seed) {
                continue;
            }
            int found = MemIndex_PredicateHasKey(&preds[j], ts);
            int inclusion = IS_INCLUSION(preds[j].type);
            if ((inclusion && !found) || (!inclusion && found)) {
                ok = 0;
                break;
            }
        }
        if (!ok) {
            continue;
        }
        if (*out_count == *cap) {
            *cap *= 2;
            char **tmp = realloc(*out, (*cap) * sizeof(char *));
            if (!tmp) {
                return;
            }
            *out = tmp;
        }
        (*out)[*out_count] = strdup(ts);
        (*out_count)++;
    }
}

char **QueryIndexKeys(const QueryPredicate *index_predicate,
                      size_t predicate_count,
                      size_t *out_count,
                      bool *hasPermissionError) {
    (void)hasPermissionError; /* worker path does not do ACL checks */
    *out_count = 0;
    if (!index_predicate || predicate_count == 0) {
        return NULL;
    }

    pthread_rwlock_rdlock(&labelsIndexMemLock);

    /* Pick smallest inclusion predicate as seed. */
    size_t seed = 0;
    uint64_t seed_sz = UINT64_MAX;
    for (size_t i = 0; i < predicate_count; ++i) {
        if (!IS_INCLUSION(index_predicate[i].type)) {
            continue;
        }
        uint64_t sz = MemIndex_PredicateTotalSize(&index_predicate[i]);
        if (sz < seed_sz) {
            seed_sz = sz;
            seed = i;
        }
    }

    /* Collect candidates from seed predicate sets. */
    StrMap seen;
    StrMap_Init(&seen);
    size_t cap = 256;
    char **out = malloc(cap * sizeof(char *));
    if (!out) {
        pthread_rwlock_unlock(&labelsIndexMemLock);
        return NULL;
    }

    char buf[512];
    const QueryPredicate *p0 = &index_predicate[seed];

    if (p0->type == CONTAINS || p0->type == NCONTAINS) {
        snprintf(buf, sizeof(buf), K_PREFIX, p0->keyCStr ? p0->keyCStr : "");
        StrMap *set = MemIndex_GetSetForKey(buf);
        MemIndex_ProcessSetCandidates(set, index_predicate, predicate_count, seed, &seen, &out, out_count, &cap);
    } else {
        for (size_t i = 0; i < p0->valueListCount; ++i) {
            const char *v = (p0->valuesCStr && p0->valuesCStr[i]) ? p0->valuesCStr[i] : "";
            snprintf(buf, sizeof(buf), KV_PREFIX, p0->keyCStr ? p0->keyCStr : "", v);
            StrMap *set = MemIndex_GetSetForKey(buf);
            MemIndex_ProcessSetCandidates(set, index_predicate, predicate_count, seed, &seen, &out, out_count, &cap);
        }
    }

    StrMap_Free(&seen, NULL);
    pthread_rwlock_unlock(&labelsIndexMemLock);

    return out;
}

void QueryPredicate_Free(QueryPredicate *predicate_list, size_t count) {
    for (size_t predicate_index = 0; predicate_index < count; predicate_index++) {
        QueryPredicate *predicate = &predicate_list[predicate_index];
        if (predicate->valuesList != NULL) {
            for (size_t i = 0; i < predicate->valueListCount; i++) {
                if (predicate->valuesList[i] != NULL) {
                    RedisModule_FreeString(NULL, predicate->valuesList[i]);
                }
            }
        }
        if (predicate->key) {
            RedisModule_FreeString(NULL, predicate->key);
        }
        free(predicate->valuesList);

        free(predicate->keyCStr);
        if (predicate->valuesCStr) {
            for (size_t i = 0; i < predicate->valueListCount; i++) {
                free(predicate->valuesCStr[i]);
            }
        }
        free(predicate->valuesCStr);
    }
}

void QueryPredicateList_Free(QueryPredicateList *list) {
    if (list->ref > 1) {
        --list->ref;
        return;
    }

    assert(list->ref == 1);

    for (size_t i = 0; i < list->count; i++) {
        QueryPredicate_Free(&list->list[i], 1);
    }

    free(list->list);
    free(list);
}
