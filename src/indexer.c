/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */
#include "indexer.h"

#include "consts.h"

#include <assert.h>
#include <limits.h>
#include <string.h>
#include <rmutil/alloc.h>

RedisModuleDict *labelsIndex;  // maps label to it's ts keys.
RedisModuleDict *tsLabelIndex; // maps ts_key to it's dict in labelsIndex
extern bool isTrimming;

#define KV_PREFIX "__index_%s=%s"
#define K_PREFIX "__key_index_%s"

typedef enum
{
    Indexer_Add = 0x1,
    Indexer_Remove = 0x2,
} INDEXER_OPERATION_T;

void IndexInit() {
    labelsIndex = RedisModule_CreateDict(NULL);
    tsLabelIndex = RedisModule_CreateDict(NULL);
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
    size_t token_len = strlen(token);

    if (token[token_len - 1] == ')') {
        token[token_len - 1] = '\0'; // remove closing parentheses
    } else {
        return TSDB_ERROR;
    }

    int filterCount = 0;
    for (int i = 0; token[i] != '\0'; i++) {
        if (token[i] == ',') {
            filterCount++;
        }
    }
    if (token_len <= 1) {
        // when the token is <=1 it means that we have an empty list
        *count = 0;
        *values = NULL;
        return TSDB_OK;
    } else {
        *count = filterCount + 1;
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

    // Extract value
    token = strtok_r(NULL, separator, &iter_ptr);
    if (strstr(separator, "=(") != NULL) {
        if (parseValueList(token, &retQuery->valueListCount, &retQuery->valuesList) == TSDB_ERROR) {
            RedisModule_FreeString(NULL, retQuery->key);
            retQuery->key = NULL;
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
    for (int i = 0; i < labels_count; i++) {
        size_t _s;
        key_string = RedisModule_StringPtrLen(labels[i].key, &_s);
        value_string = RedisModule_StringPtrLen(labels[i].value, &_s);
        RedisModuleString *indexed_key_value =
            RedisModule_CreateStringPrintf(NULL, KV_PREFIX, key_string, value_string);
        RedisModuleString *indexed_key = RedisModule_CreateStringPrintf(NULL, K_PREFIX, key_string);

        labelIndexUnderKey(Indexer_Add, indexed_key_value, ts_key, labelsIndex, tsLabelIndex);
        labelIndexUnderKey(Indexer_Add, indexed_key, ts_key, labelsIndex, tsLabelIndex);

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
    } else { // one or more entries
        *dicts = (RedisModuleDict **)malloc(sizeof(RedisModuleDict *) * predicate->valueListCount);
        *dicts_size = predicate->valueListCount;
        for (int i = 0; i < predicate->valueListCount; i++) {
            value = RedisModule_StringPtrLen(predicate->valuesList[i], &_s);
            index_key = RedisModule_CreateStringPrintf(ctx, KV_PREFIX, key, value);
            (*dicts)[i] = RedisModule_DictGet(labelsIndex, index_key, NULL);
            RedisModule_FreeString(ctx, index_key);
        }
    }
    return;
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
    for (int i = 0; i < predicate_count; i++) {
        if (!IS_INCLUSION(index_predicate[i].type)) {
            // There is at least 1 inclusion predicate
            continue;
        }

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
    for (size_t i = 1; i < predicate_count; i++) {
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

RedisModuleDict *QueryIndex(RedisModuleCtx *ctx,
                            QueryPredicate *index_predicate,
                            size_t predicate_count) {
    PromoteSmallestPredicateToFront(ctx, index_predicate, predicate_count);

    RedisModuleDict *res = RedisModule_CreateDict(ctx);
    QueryPredicate *predicate = &index_predicate[0];

    if (!IS_INCLUSION(predicate->type)) {
        return res;
    }

    RedisModuleDict **dicts = NULL;
    size_t dicts_size;
    GetPredicateKeysDicts(ctx, &index_predicate[0], &dicts, &dicts_size);

    for (size_t i = 0; i < dicts_size; i++) {
        RedisModuleDict *dict = dicts[i];
        if (dict == NULL) {
            continue;
        }

        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(dict, "^", NULL, 0);
        char *currentKey;
        size_t currentKeyLen;
        while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
            if (_isKeySatisfyAllPredicates(
                    ctx, currentKey, currentKeyLen, index_predicate, predicate_count)) {
                RedisModule_DictSetC(res, currentKey, currentKeyLen, (void *)1);
            }
        }
        RedisModule_DictIteratorStop(iter);
    }

    free(dicts);

    if (unlikely(isTrimming)) {
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(res, "^", NULL, 0);
        RedisModuleString *currentKey;
        int slot, firstSlot, lastSlot;
        RedisModule_ShardingGetSlotRange(&firstSlot, &lastSlot);
        while ((currentKey = RedisModule_DictNext(NULL, iter, NULL)) != NULL) {
            slot = RedisModule_ShardingGetKeySlot(currentKey);
            if (firstSlot > slot || lastSlot < slot) {
                RedisModule_DictDel(res, currentKey, NULL);
                RedisModule_DictIteratorReseek(iter, ">", currentKey);
            }
            RedisModule_FreeString(NULL, currentKey);
        }
        RedisModule_DictIteratorStop(iter);
    }

    return res;
}

void QueryPredicate_Free(QueryPredicate *predicate_list, size_t count) {
    for (int predicate_index = 0; predicate_index < count; predicate_index++) {
        QueryPredicate *predicate = &predicate_list[predicate_index];
        if (predicate->valuesList != NULL) {
            for (int i = 0; i < predicate->valueListCount; i++) {
                if (predicate->valuesList[i] != NULL) {
                    RedisModule_FreeString(NULL, predicate->valuesList[i]);
                }
            }
        }
        free(predicate->key);
        free(predicate->valuesList);
    }
}

void QueryPredicateList_Free(QueryPredicateList *list) {
    if (list->ref > 1) {
        list->ref--;
        return;
    }
    assert(list->ref == 1);

    for (int i = 0; i < list->count; i++) {
        QueryPredicate_Free(&list->list[i], 1);
    }
    free(list->list);
    free(list);
}
