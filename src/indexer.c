/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
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

void _union(RedisModuleCtx *ctx, RedisModuleDict *dest, RedisModuleDict *src) {
    /*
     * Copy all elements from src to dest
     */
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(src, "^", NULL, 0);
    RedisModuleString *currentKey;
    while ((currentKey = RedisModule_DictNext(ctx, iter, NULL)) != NULL) {
        RedisModule_DictSet(dest, currentKey, (void *)1);
        RedisModule_FreeString(ctx, currentKey);
    }
    RedisModule_DictIteratorStop(iter);
}

void _intersect(RedisModuleCtx *ctx, RedisModuleDict *left, RedisModuleDict *right) {
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(left, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;
    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        int doesNotExist = 0;
        RedisModule_DictGetC(right, currentKey, currentKeyLen, &doesNotExist);
        if (doesNotExist == 0) {
            continue;
        }
        RedisModule_DictDelC(left, currentKey, currentKeyLen, NULL);
        RedisModule_DictIteratorReseekC(iter, ">", currentKey, currentKeyLen);
    }
    RedisModule_DictIteratorStop(iter);
}

void _difference(RedisModuleCtx *ctx, RedisModuleDict *left, RedisModuleDict *right) {
    if (RedisModule_DictSize(right) == 0) {
        // the right leaf is empty, this means that the diff is basically no-op since the left will
        // remain intact.
        return;
    }

    // iterating over the left dict (which is always smaller) will allow us to have less data to
    // iterate over
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(left, "^", NULL, 0);

    char *currentKey;
    size_t currentKeyLen;
    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        int doesNotExist = 0;
        RedisModule_DictGetC(right, currentKey, currentKeyLen, &doesNotExist);
        if (doesNotExist == 1) {
            continue;
        }
        RedisModule_DictDelC(left, currentKey, currentKeyLen, NULL);
        RedisModule_DictIteratorReseekC(iter, ">", currentKey, currentKeyLen);
    }
    RedisModule_DictIteratorStop(iter);
}

RedisModuleDict *GetPredicateKeysDict(RedisModuleCtx *ctx,
                                      QueryPredicate *predicate,
                                      bool *isCloned) {
    /*
     * Return the dictionary of all the keys that match the predicate.
     */
    RedisModuleDict *currentLeaf = NULL;
    *isCloned = false;
    RedisModuleString *index_key;
    size_t _s;
    const char *key = RedisModule_StringPtrLen(predicate->key, &_s);
    const char *value;

    int nokey;

    if (predicate->type == NCONTAINS || predicate->type == CONTAINS) {
        index_key = RedisModule_CreateStringPrintf(
            ctx, K_PREFIX, RedisModule_StringPtrLen(predicate->key, &_s));
        currentLeaf = RedisModule_DictGet(labelsIndex, index_key, &nokey);
        RedisModule_FreeString(ctx, index_key);
    } else { // one or more entries
        RedisModuleDict *singleEntryLeaf;
        int unioned_count = 0;
        for (int i = 0; i < predicate->valueListCount; i++) {
            value = RedisModule_StringPtrLen(predicate->valuesList[i], &_s);
            index_key = RedisModule_CreateStringPrintf(ctx, KV_PREFIX, key, value);
            singleEntryLeaf = RedisModule_DictGet(labelsIndex, index_key, &nokey);
            RedisModule_FreeString(ctx, index_key);
            if (singleEntryLeaf != NULL) {
                // if there's only 1 item left to fetch from the index we can just return it
                if (unioned_count == 0 && predicate->valueListCount - i == 1) {
                    return singleEntryLeaf;
                }
                if (currentLeaf == NULL) {
                    currentLeaf = RedisModule_CreateDict(ctx);
                    *isCloned = true;
                }
                _union(ctx, currentLeaf, singleEntryLeaf);
                unioned_count++;
            }
        }
    }
    return currentLeaf;
}

RedisModuleDict *QueryIndexPredicate(RedisModuleCtx *ctx,
                                     QueryPredicate *predicate,
                                     RedisModuleDict *prevResults) {
    RedisModuleDict *localResult = RedisModule_CreateDict(ctx);
    RedisModuleDict *currentLeaf;
    bool isCloned;
    currentLeaf = GetPredicateKeysDict(ctx, predicate, &isCloned);

    if (currentLeaf != NULL) {
        // Copy everything to new dict only in case this is the first predicate.
        // In the next iteration, when prevResults is no longer NULL, there is
        // no need to copy again. We can work on currentLeaf, since only the left dict is being
        // changed during intersection / difference.
        if (prevResults == NULL) {
            RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(currentLeaf, "^", NULL, 0);
            RedisModuleString *currentKey;
            while ((currentKey = RedisModule_DictNext(ctx, iter, NULL)) != NULL) {
                RedisModule_DictSet(localResult, currentKey, (void *)1);
                RedisModule_FreeString(ctx, currentKey);
            }
            RedisModule_DictIteratorStop(iter);
        } else {
            RedisModule_FreeDict(ctx, localResult);
            localResult = currentLeaf;
        }
    }

    RedisModuleDict *result = NULL;
    if (prevResults != NULL) {
        if (predicate->type == EQ || predicate->type == CONTAINS) {
            _intersect(ctx, prevResults, localResult);
        } else if (predicate->type == LIST_MATCH) {
            _intersect(ctx, prevResults, localResult);
        } else if (predicate->type == LIST_NOTMATCH) {
            _difference(ctx, prevResults, localResult);
        } else if (predicate->type == NCONTAINS) {
            _difference(ctx, prevResults, localResult);
        } else if (predicate->type == NEQ) {
            _difference(ctx, prevResults, localResult);
        }
        result = prevResults;
    } else if (predicate->type == EQ || predicate->type == CONTAINS ||
               predicate->type == LIST_MATCH) {
        result = localResult;
    } else {
        result = prevResults; // always NULL
    }

    if (result != localResult && localResult != currentLeaf && localResult != NULL) {
        RedisModule_FreeDict(ctx, localResult);
    }
    if (isCloned && currentLeaf != result) {
        RedisModule_FreeDict(ctx, currentLeaf);
    }
    return result;
}

void PromoteSmallestPredicateToFront(RedisModuleCtx *ctx,
                                     QueryPredicate *index_predicate,
                                     size_t predicate_count) {
    /*
     * Find the predicate that has the minimal amount of keys that match to it, and move it to the
     * beginning of the predicate list so we will start our calculation from the smallest predicate.
     * This is an optimization, so we will copy the smallest dict possible.
     */
    if (predicate_count > 1) {
        int minIndex = 0;
        unsigned int minDictSize = UINT_MAX;
        bool isCloned;
        for (int i = 0; i < predicate_count; i++) {
            RedisModuleDict *currentPredicateKeys =
                GetPredicateKeysDict(ctx, &index_predicate[i], &isCloned);
            int currentDictSize =
                (currentPredicateKeys != NULL) ? RedisModule_DictSize(currentPredicateKeys) : 0;
            if (currentDictSize < minDictSize) {
                minIndex = i;
                minDictSize = currentDictSize;
            }
            if (currentPredicateKeys != NULL && isCloned) {
                RedisModule_FreeDict(ctx, currentPredicateKeys);
            }
        }

        // switch between the minimal predicate and the predicate in the first place
        if (minIndex != 0) {
            QueryPredicate temp = index_predicate[minIndex];
            index_predicate[minIndex] = index_predicate[0];
            index_predicate[0] = temp;
        }
    }
}

RedisModuleDict *QueryIndex(RedisModuleCtx *ctx,
                            QueryPredicate *index_predicate,
                            size_t predicate_count) {
    RedisModuleDict *result = NULL;

    PromoteSmallestPredicateToFront(ctx, index_predicate, predicate_count);

    // EQ or Contains
    for (int i = 0; i < predicate_count; i++) {
        if (index_predicate[i].type == EQ || index_predicate[i].type == CONTAINS ||
            index_predicate[i].type == LIST_MATCH) {
            result = QueryIndexPredicate(ctx, &index_predicate[i], result);
            if (result == NULL) {
                return RedisModule_CreateDict(ctx);
            }
        }
    }

    // The next two types of queries are reducers so we run them after the matcher
    // NCONTAINS or NEQ
    for (int i = 0; i < predicate_count; i++) {
        if (index_predicate[i].type == NCONTAINS || index_predicate[i].type == NEQ ||
            index_predicate[i].type == LIST_NOTMATCH) {
            result = QueryIndexPredicate(ctx, &index_predicate[i], result);
            if (result == NULL) {
                return RedisModule_CreateDict(ctx);
            }
        }
    }

    if (result == NULL) {
        return RedisModule_CreateDict(ctx);
    }

    if (unlikely(isTrimming)) {
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
        RedisModuleString *currentKey;
        int slot, firstSlot, lastSlot;
        RedisModule_ShardingGetSlotRange(&firstSlot, &lastSlot);
        while ((currentKey = RedisModule_DictNext(NULL, iter, NULL)) != NULL) {
            slot = RedisModule_ShardingGetKeySlot(currentKey);
            if (firstSlot > slot || lastSlot < slot) {
                RedisModule_DictDel(result, currentKey, NULL);
                RedisModule_DictIteratorReseek(iter, ">", currentKey);
            }
            RedisModule_FreeString(NULL, currentKey);
        }
        RedisModule_DictIteratorStop(iter);
    }

    return result;
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
