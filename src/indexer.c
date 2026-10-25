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

#include <limits.h>
#include <stdint.h>
#include <string.h>
#include <rmutil/alloc.h>

RedisModuleDict *labelsIndex;  // maps label to it's ts keys.
RedisModuleDict *tsLabelIndex; // maps ts_key to it's dict in labelsIndex
extern bool isReshardTrimming, isAsmTrimming, isAsmImporting;

#define KV_PREFIX_LITERAL "__index_"
#define K_PREFIX_LITERAL "__key_index_"
#define KV_PREFIX KV_PREFIX_LITERAL "%s=%s"
#define K_PREFIX K_PREFIX_LITERAL "%s"

typedef enum
{
    Indexer_Add = 0x1,
    Indexer_Remove = 0x2,
} INDEXER_OPERATION_T;

void IndexInit() {
    labelsIndex = RedisModule_CreateDict(NULL);
    tsLabelIndex = RedisModule_CreateDict(NULL);
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
    RedisModuleString *owned = NULL;
    RedisModule_DictDel(leaf, ts_key, (void **)&owned);
    if (owned)
        RedisModule_FreeString(NULL, owned);
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
        RedisModuleString *owned = RedisModule_HoldString(NULL, ts_key);
        if (RedisModule_DictSet(leaf, ts_key, owned) == REDISMODULE_ERR)
            RedisModule_FreeString(NULL, owned);
        RedisModule_DictSet(ts_leaf, key, NULL);
    } else if (op & Indexer_Remove) {
        labelsIndexRemoveTsKey(leaf, key, ts_key, _labelsIndex);
    }
}

void IndexMetric(RedisModuleString *ts_key, Label *labels, size_t labels_count) {
    if (!labels_count)
        return;
    // Index references must not share refcounts with a Series freed on a background thread.
    RedisModuleString *index_key = RedisModule_CreateStringFromString(NULL, ts_key);
    const char *key_string, *value_string;
    for (int i = 0; i < labels_count; i++) {
        size_t _s;
        key_string = RedisModule_StringPtrLen(labels[i].key, &_s);
        value_string = RedisModule_StringPtrLen(labels[i].value, &_s);
        RedisModuleString *indexed_key_value =
            RedisModule_CreateStringPrintf(NULL, KV_PREFIX, key_string, value_string);
        RedisModuleString *indexed_key = RedisModule_CreateStringPrintf(NULL, K_PREFIX, key_string);

        labelIndexUnderKey(Indexer_Add, indexed_key_value, index_key, labelsIndex, tsLabelIndex);
        labelIndexUnderKey(Indexer_Add, indexed_key, index_key, labelsIndex, tsLabelIndex);

        RedisModule_FreeString(NULL, indexed_key_value);
        RedisModule_FreeString(NULL, indexed_key);
    }
    RedisModule_FreeString(NULL, index_key);
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

// Estimates the label-index memory attributable to a single time series (MOD-6409 #6).
//
// The index is global module state rather than part of the value object, but the
// ticket lists it as a memUsage() fix, so it is summed into SeriesMemUsage() and thus
// reported by both TS.INFO and MEMORY USAGE. Apportioning the shared leaves by 1/N
// keeps the property that summing over all keys counts the index exactly once.
//
// Two contributions are summed:
//   1. The inverse-index dict (tsLabelIndex[ts_key]), which is owned exclusively by
//      this key, is counted in full.
//   2. Each shared leaf dict in labelsIndex (one per "label=value" / "label" the key
//      indexes) is shared by every key carrying that same label, so only this key's
//      per-entry slice (size / entry-count) is attributed to it.
//
// Dict sizes come from RedisModule_MallocSizeDict(), which is itself an approximation,
// so the result is an estimate.
//
// Note: the per-key number is not static — it shifts as other keys with the same
// label are added or removed, because entries (the denominator) changes. This is
// intentional: the 1/N slice keeps the sum-over-all-keys invariant correct, which
// is what matters for capacity estimates. Individual MEMORY USAGE calls on a live
// system will show small fluctuations as the keyspace changes.
size_t IndexMemUsage(RedisModuleString *ts_key) {
    if (ts_key == NULL) {
        return 0;
    }
    int nokey = 0;
    RedisModuleDict *ts_leaf = RedisModule_DictGet(tsLabelIndex, ts_key, &nokey);
    if (nokey) { // series has no labels or is not indexed
        return 0;
    }

    size_t total = RedisModule_MallocSizeDict(ts_leaf);
    bool countedKey = false;

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(ts_leaf, "^", NULL, 0);
    RedisModuleString *labelKey;
    while ((labelKey = RedisModule_DictNext(NULL, iter, NULL)) != NULL) {
        int leaf_nokey = 0;
        RedisModuleDict *leaf = RedisModule_DictGet(labelsIndex, labelKey, &leaf_nokey);
        if (!leaf_nokey && leaf != NULL) {
            const uint64_t entries = RedisModule_DictSize(leaf);
            if (entries > 0) {
                total += RedisModule_MallocSizeDict(leaf) / entries;
                if (!countedKey) {
                    RedisModuleString *owned = RedisModule_DictGet(leaf, ts_key, NULL);
                    if (owned) {
                        total += RedisModule_MallocSizeString(owned);
                        countedKey = true;
                    }
                }
            }
        }
        RedisModule_FreeString(NULL, labelKey);
    }
    RedisModule_DictIteratorStop(iter);

    return total;
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

// Results here come from our in-memory label index, not the Redis keyspace, so core's
// per-slot ownership filtering never applies to them. The index is cleaned lazily (via
// unlink/keyspace notifications), so during a slot migration it still contains keys for
// slots this shard no longer owns (reshard/ASM trim) or does not own yet (ASM import).
// Drop those so we don't report series that aren't this shard's responsibility.
static void TrimUnownedKeysDuringReshard(RedisModuleDict *res) {
    if (likely(!(isReshardTrimming || isAsmTrimming || isAsmImporting))) {
        return;
    }
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

RedisModuleDict *QueryIndex(RedisModuleCtx *ctx,
                            QueryPredicate *index_predicate,
                            size_t predicate_count,
                            bool *hasPermissionError) {
    RedisModuleDict *res = RedisModule_CreateDict(ctx);
    if (predicate_count == 0) {
        return res;
    }

    PromoteSmallestPredicateToFront(ctx, index_predicate, predicate_count);

    QueryPredicate *predicate = &index_predicate[0];

    if (!IS_INCLUSION(predicate->type)) {
        return res;
    }

    RedisModuleDict **dicts = NULL;
    size_t dicts_size = 0;
    GetPredicateKeysDicts(ctx, &index_predicate[0], &dicts, &dicts_size);

    // Resolve the user once for the whole multi-dict scan so ACL checks
    // below don't alloc/free a RedisModuleUser per candidate key.
    User_Ctx_t userCtx = { .user = NULL, .is_owned = false };
    if (hasPermissionError) {
        userCtx = GetUserFromContext(ctx);
    }

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
                if (!CheckKeyIsAllowedToReadC(ctx, userCtx.user, currentKey, currentKeyLen)) {
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

    FreeUser(&userCtx);
    free(dicts);

    TrimUnownedKeysDuringReshard(res);

    return res;
}

typedef struct
{
    RedisModuleDict **dicts;
    size_t count;
    bool inclusion;
} SnapshotPredicate;

typedef struct
{
    union
    {
        size_t offset;
        const char *name;
    } key;
    size_t len;
    RedisModuleString *owned;
} SnapshotKey;

// Short names are packed for cache-friendly replies. For names of 64 bytes or
// more, QUERYINDEX keeps the owned string until after replying, avoiding a second
// large copy. QUERYLABELS always copies names because an unfiltered scan visits
// the inverse index, whose values are label dictionaries rather than key strings.

typedef struct
{
    RedisModuleDictIter *iter;
    const char *name;
    size_t len;
    void *value;
} SnapshotCursor;

static int CompareNames(const char *a, size_t alen, const char *b, size_t blen) {
    int cmp = memcmp(a, b, min(alen, blen));
    return cmp ? cmp : (alen > blen) - (alen < blen);
}

static int CompareDictPointers(const void *a, const void *b) {
    uintptr_t pa = (uintptr_t) * (RedisModuleDict *const *)a;
    uintptr_t pb = (uintptr_t) * (RedisModuleDict *const *)b;
    return (pa > pb) - (pa < pb);
}

static void FixSnapshotHeap(SnapshotCursor *heap, size_t size, size_t pos) {
    SnapshotCursor root = heap[pos];
    while (pos < size / 2) {
        size_t child = pos * 2 + 1;
        if (child + 1 < size &&
            CompareNames(
                heap[child + 1].name, heap[child + 1].len, heap[child].name, heap[child].len) < 0)
            ++child;
        if (CompareNames(root.name, root.len, heap[child].name, heap[child].len) <= 0)
            break;
        heap[pos] = heap[child];
        pos = child;
    }
    heap[pos] = root;
}

size_t VisitQueryIndex(RedisModuleCtx *ctx,
                       QueryPredicateList *queries,
                       bool filterExpiry,
                       void (*visit)(RedisModuleCtx *, const char *, size_t, void *),
                       void *data) {
    RedisModule_Assert(!filterExpiry || queries);
    size_t count = queries ? queries->count : 1;
    if (!count)
        return 0;
    if (queries)
        PromoteSmallestPredicateToFront(ctx, queries->list, count);
    if (queries && !IS_INCLUSION(queries->list[0].type))
        return 0;
    SnapshotPredicate *predicates = calloc(count, sizeof(*predicates));
    if (queries) {
        for (size_t i = 0; i < count; ++i) {
            GetPredicateKeysDicts(
                ctx, &queries->list[i], &predicates[i].dicts, &predicates[i].count);
            predicates[i].inclusion = IS_INCLUSION(queries->list[i].type);
        }
    } else {
        predicates[0].dicts = malloc(sizeof(RedisModuleDict *));
        predicates[0].dicts[0] = tsLabelIndex;
        predicates[0].count = 1;
    }
    // Merge sorted leaves instead of materializing duplicate candidates. Repeated
    // values in the first matcher must not multiply the snapshot's memory use.
    SnapshotPredicate *first = &predicates[0];
    if (first->count > 1) {
        qsort(first->dicts, first->count, sizeof(*first->dicts), CompareDictPointers);
        size_t unique = 0;
        for (size_t i = 0; i < first->count; ++i)
            if (first->dicts[i] && (!unique || first->dicts[i] != first->dicts[unique - 1]))
                first->dicts[unique++] = first->dicts[i];
        first->count = unique;
    }
    size_t heapSize = 0;
    SnapshotCursor *heap = calloc(first->count, sizeof(*heap));
    for (size_t i = 0; i < first->count; ++i) {
        if (!first->dicts[i])
            continue;
        SnapshotCursor cursor = { .iter = RedisModule_DictIteratorStartC(
                                      first->dicts[i], "^", NULL, 0) };
        cursor.name = RedisModule_DictNextC(cursor.iter, &cursor.len, &cursor.value);
        if (cursor.name)
            heap[heapSize++] = cursor;
        else
            RedisModule_DictIteratorStop(cursor.iter);
    }
    for (size_t i = heapSize / 2; i > 0; --i)
        FixSnapshotHeap(heap, heapSize, i - 1);
    size_t size = 0, capacity = 0, bytes = 0, allocated = 0;
    if (count == 1 && predicates[0].count == 1 && predicates[0].dicts[0])
        capacity = RedisModule_DictSize(predicates[0].dicts[0]);
    RedisModule_Assert(capacity <= SIZE_MAX / sizeof(SnapshotKey));
    SnapshotKey *keys = capacity ? malloc(capacity * sizeof(*keys)) : NULL;
    char *names = malloc(1);
    bool merge = first->count > 1;
    RedisModuleDictIter *single = (!merge && heapSize) ? heap[0].iter : NULL;
    const char *name = heapSize ? heap[0].name : NULL;
    size_t len = heapSize ? heap[0].len : 0;
    void *value = heapSize ? heap[0].value : NULL;
    while (name) {
        if (merge && size && len == keys[size - 1].len &&
            !memcmp(name, filterExpiry && len >= 64 ? keys[size - 1].key.name
                                                  : names + keys[size - 1].key.offset, len))
            goto next;
        bool matches = true;
        for (size_t j = 1; j < count && matches; ++j) {
            bool found = false;
            for (size_t k = 0; k < predicates[j].count && !found; ++k) {
                if (!predicates[j].dicts[k])
                    continue;
                int missing = 0;
                RedisModule_DictGetC(predicates[j].dicts[k], (void *)name, len, &missing);
                found = !missing;
            }
            matches = (found == predicates[j].inclusion);
        }
        if (!matches)
            goto next;
        if (isReshardTrimming || isAsmTrimming || isAsmImporting) {
            RedisModuleString *key = RedisModule_CreateString(NULL, name, len);
            bool owned = (isReshardTrimming ? OwnKeyDuringSharding : OwnKeyDuringASM)(key);
            RedisModule_FreeString(NULL, key);
            if (!owned)
                goto next;
        }
        if (size == capacity) {
            RedisModule_Assert(capacity <= SIZE_MAX / 2 / sizeof(*keys));
            capacity = capacity ? capacity * 2 : 64;
            keys = realloc(keys, capacity * sizeof(*keys));
        }
        RedisModuleString *owned = filterExpiry ? RedisModule_HoldString(NULL, value) : NULL;
        if (owned && len >= 64) {
            keys[size++] = (SnapshotKey){.key.name = RedisModule_StringPtrLen(owned, NULL),
                                        .len = len, .owned = owned};
            goto next;
        }
        RedisModule_Assert(len <= SIZE_MAX - bytes);
        if (bytes + len > allocated) {
            RedisModule_Assert(allocated <= SIZE_MAX / 2);
            allocated = max(allocated ? allocated * 2 : 1024, bytes + len);
            names = realloc(names, allocated);
        }
        keys[size++] =
            (SnapshotKey){ .key.offset = bytes,
                           .len = len,
                           .owned = owned };
        memcpy(names + bytes, name, len);
        bytes += len;
next:
        if (!merge) {
            name = RedisModule_DictNextC(single, &len, &value);
            continue;
        }
        heap[0].name = RedisModule_DictNextC(heap[0].iter, &heap[0].len, &heap[0].value);
        if (!heap[0].name) {
            RedisModule_DictIteratorStop(heap[0].iter);
            heap[0] = heap[--heapSize];
        }
        if (heapSize > 1)
            FixSnapshotHeap(heap, heapSize, 0);
        name = heapSize ? heap[0].name : NULL;
        if (heapSize) {
            len = heap[0].len;
            value = heap[0].value;
        }
    }
    if (single)
        RedisModule_DictIteratorStop(single);
    free(heap);
    for (size_t i = 0; i < count; ++i)
        free(predicates[i].dicts);
    free(predicates);
    for (size_t i = 0; i < size; ++i)
        if (!(filterExpiry && keys[i].len >= 64))
            keys[i].key.name = names + keys[i].key.offset;
    size_t visible = size;
    // All live-index iterators are closed before a lookup can expire a series.
    // Ownership checks above cannot expire keys. Keep Redis's own expiry rules
    // here, including logical expiry on replicas and the frozen time in scripts.
    if (filterExpiry) {
        for (size_t i = 0; i < size; ++i) {
            RedisModuleString *key = keys[i].owned;
            if (i + 16 < size)
                __builtin_prefetch(keys[i + 16].owned, 0, 1);
            bool exists = RedisModule_KeyExists(ctx, key);
            if (keys[i].len < 64) RedisModule_FreeString(NULL, key);
            if (!exists) {
                keys[i].key.name = NULL;
                --visible;
            }
        }
    }
    for (size_t i = 0; i < size; ++i) {
        if (keys[i].key.name)
            visit(ctx, keys[i].key.name, keys[i].len, data);
        if (filterExpiry && keys[i].len >= 64) RedisModule_FreeString(NULL, keys[i].owned);
    }
    free(names);
    free(keys);
    return visible;
}

RedisModuleString *QueryLabelsBuildPrefix(QueryLabelsSubtype subtype,
                                          RedisModuleString *labelFilter) {
    return subtype == QueryLabelsSubtype_Labels
               ? RedisModule_CreateStringPrintf(NULL, K_PREFIX, "")
               : RedisModule_CreateStringPrintf(
                     NULL, KV_PREFIX, RedisModule_StringPtrLen(labelFilter, NULL), "");
}

// Label keys can contain '='. For such a series, a KV entry's "key=value" content
// is ambiguous: e.g. key "a=b" value "c" and key "a" value "b=c" both flatten differently,
// but a VALUES(a) lookup can't tell "a=b=c" (real key "a=b") from a genuine key "a" without
// cross-checking. A real key always has an exact "__key_index_"-prefixed leaf entry, so if
// some OTHER real key strictly longer than candidateLabel is itself a "key=" prefix of this
// entry's content, the content truly belongs to that longer key, not to candidateLabel.
static bool ValuesMatchShadowedByLongerKey(RedisModuleDict *leaf,
                                           const char *content,
                                           size_t contentLen,
                                           size_t candidateLabelLen) {
    size_t kLitLen = strlen(K_PREFIX_LITERAL);
    bool shadowed = false;
    RedisModuleDictIter *kiter = RedisModule_DictIteratorStartC(leaf, "^", NULL, 0);
    char *kEntryBuf;
    size_t kEntryLen;
    while (!shadowed && (kEntryBuf = RedisModule_DictNextC(kiter, &kEntryLen, NULL)) != NULL) {
        if (kEntryLen <= kLitLen || memcmp(kEntryBuf, K_PREFIX_LITERAL, kLitLen) != 0) {
            continue;
        }
        const char *otherKey = kEntryBuf + kLitLen;
        size_t otherKeyLen = kEntryLen - kLitLen;
        if (otherKeyLen > candidateLabelLen && otherKeyLen < contentLen &&
            memcmp(content, otherKey, otherKeyLen) == 0 && content[otherKeyLen] == '=') {
            shadowed = true;
        }
    }
    RedisModule_DictIteratorStop(kiter);
    return shadowed;
}

void QueryLabelsFromIndex(const char *tsKey,
                          size_t tsKeyLen,
                          QueryLabelsSubtype subtype,
                          const char *prefixBuf,
                          size_t prefixLen,
                          void (*emit)(void *userData, const char *buf, size_t len),
                          void *userData) {
    int nokey = 0;
    RedisModuleDict *leaf = RedisModule_DictGetC(tsLabelIndex, (void *)tsKey, tsKeyLen, &nokey);
    if (nokey) {
        return;
    }

    size_t kvLitLen = strlen(KV_PREFIX_LITERAL);
    size_t candidateLabelLen = subtype == QueryLabelsSubtype_Values ? prefixLen - kvLitLen - 1 : 0;

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(leaf, "^", NULL, 0);
    char *entryBuf;
    size_t entryLen;
    while ((entryBuf = RedisModule_DictNextC(iter, &entryLen, NULL)) != NULL) {
        if (entryLen < prefixLen || memcmp(entryBuf, prefixBuf, prefixLen) != 0) {
            continue;
        }
        if (subtype == QueryLabelsSubtype_Values &&
            ValuesMatchShadowedByLongerKey(
                leaf, entryBuf + kvLitLen, entryLen - kvLitLen, candidateLabelLen)) {
            continue;
        }
        emit(userData, entryBuf + prefixLen, entryLen - prefixLen);
        if (subtype == QueryLabelsSubtype_Values) {
            break; // a series has at most one value for a given label name
        }
    }
    RedisModule_DictIteratorStop(iter);
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
        free(predicate->key);
        free(predicate->valuesList);
    }
}

void QueryPredicateList_Free(QueryPredicateList *list) {
    // Atomic: LibMR duplicates/frees the QueryPredicates_Arg/QueryLabelsArg holding this list
    // from its own execution threads (QueryPredicates_Duplicate/ObjectFree, QueryLabelsArg_*),
    // concurrently with whichever thread drops this module's own reference. Every touch of
    // `ref` - increments in libmr_commands.c included - has to be atomic for the count spread
    // across those threads to stay consistent.
    if (__atomic_sub_fetch(&list->ref, 1, __ATOMIC_RELAXED) > 0) {
        return;
    }

    for (size_t i = 0; i < list->count; i++) {
        QueryPredicate_Free(&list->list[i], 1);
    }

    free(list->list);
    free(list);
}
