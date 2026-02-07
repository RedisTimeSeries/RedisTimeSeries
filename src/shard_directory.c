/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "shard_directory.h"

#include "common.h"
#include "config.h"
#include "endianconv.h"
#include "libmr_integration.h"
#include "module.h"
#include "utils/arr.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#define RTS_CLUSTER_LP_ADD 0x45
#define RTS_CLUSTER_LP_REM 0x46

#define SHARD_DIRECTORY_FLUSH_INTERVAL_MS 100

typedef struct ShardDirectoryUpdate {
    uint64_t label_hash;
    bool is_add;
} ShardDirectoryUpdate;

typedef struct ShardDirectory {
    RedisModuleDict *shardsByLabelPair;
    RedisModuleDict *localRefcounts;
    pthread_rwlock_t lock;

    ShardDirectoryUpdate *updates;
    char **nodeIds;
    size_t nodeIdsCount;
    size_t localShardIndex;
    uint64_t clusterSig;
    bool enabled;
    bool needsRefresh;
    RedisModuleTimerID flushTimer;
} ShardDirectory;

static ShardDirectory shardDirectory;
static RTSShardDirectoryStats shardDirectoryStats;

static void ShardDirectory_ProcessQueue(RedisModuleCtx *ctx, void *data);
static void ShardDirectory_OnClusterMessage(RedisModuleCtx *ctx,
                                            const char *sender_id,
                                            uint8_t type,
                                            const unsigned char *payload,
                                            uint32_t len);

static inline void StatsIncrement(uint64_t *counter, uint64_t value) {
    __atomic_add_fetch(counter, value, __ATOMIC_RELAXED);
}

const RTSShardDirectoryStats *ShardDirectory_GetStats(void) {
    return &shardDirectoryStats;
}

void ShardDirectory_StatsIncrementFullFanout(void) {
    StatsIncrement(&shardDirectoryStats.mrangecoord_full_fanout, 1);
}

void ShardDirectory_StatsIncrementPruned(size_t targetCount) {
    StatsIncrement(&shardDirectoryStats.mrangecoord_pruned, 1);
    StatsIncrement(&shardDirectoryStats.mrangecoord_targets_total, targetCount);
}

void ShardDirectory_StatsIncrementUnknownLookup(void) {
    StatsIncrement(&shardDirectoryStats.directory_unknown_lookups, 1);
}

void ShardDirectory_StatsIncrementUpdatesSent(size_t count) {
    StatsIncrement(&shardDirectoryStats.directory_updates_sent, count);
}

void ShardDirectory_StatsIncrementUpdatesReceived(size_t count) {
    StatsIncrement(&shardDirectoryStats.directory_updates_received, count);
}

static inline size_t bitset_word_count(size_t bitCount) {
    return (bitCount + 63) / 64;
}

static RTSShardBitset *ShardBitset_New(size_t bitCount) {
    RTSShardBitset *bitset = malloc(sizeof(*bitset));
    if (!bitset) {
        return NULL;
    }
    bitset->nwords = bitset_word_count(bitCount);
    bitset->words = calloc(bitset->nwords, sizeof(uint64_t));
    if (!bitset->words) {
        free(bitset);
        return NULL;
    }
    return bitset;
}

static void ShardBitset_Free(RTSShardBitset *bitset) {
    if (!bitset) {
        return;
    }
    free(bitset->words);
    free(bitset);
}

void ShardDirectory_FreeBitset(RTSShardBitset *bitset) {
    if (!bitset) {
        return;
    }
    free(bitset->words);
    bitset->words = NULL;
    bitset->nwords = 0;
}

bool ShardDirectory_BitsetIsEmpty(const RTSShardBitset *bitset) {
    return ShardBitset_IsEmpty(bitset);
}

bool ShardDirectory_LocalShardIncluded(const RTSShardBitset *bitset) {
    if (!bitset) {
        return false;
    }
    ShardDirectory *dir = &shardDirectory;
    ShardDirectory_EnsureClusterLayout(dir);
    pthread_rwlock_rdlock(&dir->lock);
    const bool included =
        (dir->localShardIndex != SIZE_MAX) && ShardBitset_Test(bitset, dir->localShardIndex);
    pthread_rwlock_unlock(&dir->lock);
    return included;
}

static RTSShardBitset ShardBitset_Copy(const RTSShardBitset *src) {
    RTSShardBitset out = { 0 };
    if (!src || !src->words || src->nwords == 0) {
        return out;
    }
    out.nwords = src->nwords;
    out.words = malloc(sizeof(uint64_t) * out.nwords);
    if (!out.words) {
        out.nwords = 0;
        return out;
    }
    memcpy(out.words, src->words, sizeof(uint64_t) * out.nwords);
    return out;
}

static void ShardBitset_AndInPlace(RTSShardBitset *dst, const RTSShardBitset *src) {
    if (!dst || !src || !dst->words || !src->words) {
        return;
    }
    const size_t n = dst->nwords < src->nwords ? dst->nwords : src->nwords;
    for (size_t i = 0; i < n; i++) {
        dst->words[i] &= src->words[i];
    }
    for (size_t i = n; i < dst->nwords; i++) {
        dst->words[i] = 0;
    }
}

static bool ShardBitset_IsEmpty(const RTSShardBitset *bitset) {
    if (!bitset || !bitset->words) {
        return true;
    }
    for (size_t i = 0; i < bitset->nwords; i++) {
        if (bitset->words[i] != 0) {
            return false;
        }
    }
    return true;
}

static size_t ShardBitset_Count(const RTSShardBitset *bitset) {
    if (!bitset || !bitset->words) {
        return 0;
    }
    size_t count = 0;
    for (size_t i = 0; i < bitset->nwords; i++) {
        count += (size_t)__builtin_popcountll(bitset->words[i]);
    }
    return count;
}

static void ShardBitset_Set(RTSShardBitset *bitset, size_t index) {
    if (!bitset || !bitset->words) {
        return;
    }
    const size_t word = index / 64;
    const size_t bit = index % 64;
    if (word >= bitset->nwords) {
        return;
    }
    bitset->words[word] |= (1ULL << bit);
}

static void ShardBitset_Clear(RTSShardBitset *bitset, size_t index) {
    if (!bitset || !bitset->words) {
        return;
    }
    const size_t word = index / 64;
    const size_t bit = index % 64;
    if (word >= bitset->nwords) {
        return;
    }
    bitset->words[word] &= ~(1ULL << bit);
}

static bool ShardBitset_Test(const RTSShardBitset *bitset, size_t index) {
    if (!bitset || !bitset->words) {
        return false;
    }
    const size_t word = index / 64;
    const size_t bit = index % 64;
    if (word >= bitset->nwords) {
        return false;
    }
    return (bitset->words[word] & (1ULL << bit)) != 0;
}

static uint64_t HashBytes(uint64_t hash, const unsigned char *data, size_t len) {
    const uint64_t prime = 1099511628211ULL;
    for (size_t i = 0; i < len; i++) {
        hash ^= (uint64_t)data[i];
        hash *= prime;
    }
    return hash;
}

static uint64_t HashLabelValue(const char *label,
                               size_t label_len,
                               const char *value,
                               size_t value_len) {
    uint64_t hash = 1469598103934665603ULL;
    hash = HashBytes(hash, (const unsigned char *)label, label_len);
    hash = HashBytes(hash, (const unsigned char *)"\0", 1);
    hash = HashBytes(hash, (const unsigned char *)value, value_len);
    return hash;
}

static int CompareNodeId(const void *a, const void *b) {
    const char *lhs = *(const char *const *)a;
    const char *rhs = *(const char *const *)b;
    return strcmp(lhs, rhs);
}

static bool ShardDirectory_IsClusterEnabled(void) {
    if (!IsMRCluster()) {
        return false;
    }
    if (!RedisModule_RegisterClusterMessageReceiver || !RedisModule_SendClusterMessage ||
        !RedisModule_GetClusterNodesList || !RedisModule_FreeClusterNodesList ||
        !RedisModule_GetClusterNodeInfo || !RedisModule_GetMyClusterID) {
        return false;
    }
    return true;
}

static void ShardDirectory_ClearNodeIds_locked(ShardDirectory *dir) {
    if (!dir->nodeIds) {
        return;
    }
    for (size_t i = 0; i < dir->nodeIdsCount; i++) {
        free(dir->nodeIds[i]);
    }
    free(dir->nodeIds);
    dir->nodeIds = NULL;
    dir->nodeIdsCount = 0;
}

static void ShardDirectory_ClearBitsets_locked(ShardDirectory *dir, bool recreate) {
    if (!dir->shardsByLabelPair) {
        return;
    }
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(dir->shardsByLabelPair, "^", NULL, 0);
    void *entry = NULL;
    while (RedisModule_DictNextC(iter, NULL, &entry) != NULL) {
        ShardBitset_Free((RTSShardBitset *)entry);
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_FreeDict(NULL, dir->shardsByLabelPair);
    dir->shardsByLabelPair = recreate ? RedisModule_CreateDict(NULL) : NULL;
}

static void ShardDirectory_RebuildLocalBits_locked(ShardDirectory *dir, bool enqueueUpdates) {
    ShardDirectory_ClearBitsets_locked(dir, true);
    if (dir->localShardIndex == SIZE_MAX || dir->nodeIdsCount == 0) {
        return;
    }
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(dir->localRefcounts, "^", NULL, 0);
    uint32_t *refcount = NULL;
    size_t key_len = 0;
    const char *key = NULL;
    while ((key = RedisModule_DictNextC(iter, &key_len, (void **)&refcount)) != NULL) {
        if (key_len != sizeof(uint64_t)) {
            continue;
        }
        uint64_t hash = 0;
        memcpy(&hash, key, sizeof(hash));
        if (!refcount || *refcount == 0) {
            continue;
        }
        RTSShardBitset *bitset =
            RedisModule_DictGetC(dir->shardsByLabelPair, &hash, sizeof(hash), NULL);
        if (!bitset) {
            bitset = ShardBitset_New(dir->nodeIdsCount);
            if (!bitset) {
                continue;
            }
            RedisModule_DictSetC(dir->shardsByLabelPair, &hash, sizeof(hash), bitset);
        }
        ShardBitset_Set(bitset, dir->localShardIndex);
        if (enqueueUpdates) {
            ShardDirectoryUpdate update = { .label_hash = hash, .is_add = true };
            dir->updates = array_append(dir->updates, update);
        }
    }
    RedisModule_DictIteratorStop(iter);
}

static bool ShardDirectory_RefreshClusterLayout_locked(ShardDirectory *dir) {
    size_t total = 0;
    char **nodeIds = RedisModule_GetClusterNodesList(rts_staticCtx, &total);
    if (!nodeIds) {
        return false;
    }
    char **masters = malloc(sizeof(char *) * total);
    if (!masters) {
        RedisModule_FreeClusterNodesList(nodeIds);
        return false;
    }
    size_t mastersCount = 0;
    for (size_t i = 0; i < total; i++) {
        int flags = 0;
        if (RedisModule_GetClusterNodeInfo(
                rts_staticCtx, nodeIds[i], NULL, NULL, NULL, &flags) != REDISMODULE_OK) {
            continue;
        }
        if ((flags & REDISMODULE_NODE_MASTER) == 0) {
            continue;
        }
        masters[mastersCount++] = strdup(nodeIds[i]);
    }
    RedisModule_FreeClusterNodesList(nodeIds);
    if (mastersCount == 0) {
        free(masters);
        return false;
    }
    qsort(masters, mastersCount, sizeof(char *), CompareNodeId);
    uint64_t sig = 1469598103934665603ULL;
    for (size_t i = 0; i < mastersCount; i++) {
        sig = HashBytes(sig, (const unsigned char *)masters[i], strlen(masters[i]) + 1);
    }
    const bool sameLayout = (sig == dir->clusterSig) && (mastersCount == dir->nodeIdsCount);
    if (sameLayout) {
        for (size_t i = 0; i < mastersCount; i++) {
            free(masters[i]);
        }
        free(masters);
        dir->needsRefresh = false;
        return true;
    }
    ShardDirectory_ClearNodeIds_locked(dir);
    dir->nodeIds = masters;
    dir->nodeIdsCount = mastersCount;
    dir->clusterSig = sig;
    dir->localShardIndex = SIZE_MAX;
    const char *localId = RedisModule_GetMyClusterID();
    if (localId) {
        for (size_t i = 0; i < mastersCount; i++) {
            if (strcmp(localId, masters[i]) == 0) {
                dir->localShardIndex = i;
                break;
            }
        }
    }
    ShardDirectory_RebuildLocalBits_locked(dir, true);
    dir->needsRefresh = false;
    return true;
}

static bool ShardDirectory_EnsureClusterLayout(ShardDirectory *dir) {
    if (!dir->enabled) {
        return false;
    }
    pthread_rwlock_wrlock(&dir->lock);
    if (!dir->needsRefresh && RedisModule_GetClusterSize) {
        const size_t clusterSize = RedisModule_GetClusterSize();
        if (clusterSize > 0 && clusterSize != dir->nodeIdsCount) {
            dir->needsRefresh = true;
        }
    }
    if (!dir->needsRefresh) {
        pthread_rwlock_unlock(&dir->lock);
        return true;
    }
    const bool refreshed = ShardDirectory_RefreshClusterLayout_locked(dir);
    pthread_rwlock_unlock(&dir->lock);
    return refreshed;
}

static RTSShardBitset *ShardDirectory_GetOrCreateBitset_locked(ShardDirectory *dir, uint64_t hash) {
    RTSShardBitset *bitset =
        RedisModule_DictGetC(dir->shardsByLabelPair, &hash, sizeof(hash), NULL);
    if (!bitset) {
        bitset = ShardBitset_New(dir->nodeIdsCount);
        if (!bitset) {
            return NULL;
        }
        RedisModule_DictSetC(dir->shardsByLabelPair, &hash, sizeof(hash), bitset);
    }
    return bitset;
}

static void ShardDirectory_UpdateRefcount_locked(ShardDirectory *dir, uint64_t hash, int delta) {
    uint32_t *refcount = RedisModule_DictGetC(dir->localRefcounts, &hash, sizeof(hash), NULL);
    if (!refcount) {
        if (delta < 0) {
            return;
        }
        refcount = malloc(sizeof(*refcount));
        if (!refcount) {
            return;
        }
        *refcount = 0;
        RedisModule_DictSetC(dir->localRefcounts, &hash, sizeof(hash), refcount);
    }
    const uint32_t prev = *refcount;
    if (delta < 0 && prev == 0) {
        return;
    }
    const uint32_t next = (uint32_t)((int64_t)prev + delta);
    *refcount = next;
    if (dir->localShardIndex == SIZE_MAX || dir->nodeIdsCount == 0) {
        return;
    }
    if (prev == 0 && next == 1) {
        RTSShardBitset *bitset = ShardDirectory_GetOrCreateBitset_locked(dir, hash);
        if (bitset) {
            ShardBitset_Set(bitset, dir->localShardIndex);
        }
        ShardDirectoryUpdate update = { .label_hash = hash, .is_add = true };
        dir->updates = array_append(dir->updates, update);
    } else if (prev == 1 && next == 0) {
        RTSShardBitset *bitset = ShardDirectory_GetOrCreateBitset_locked(dir, hash);
        if (bitset) {
            ShardBitset_Clear(bitset, dir->localShardIndex);
        }
        ShardDirectoryUpdate update = { .label_hash = hash, .is_add = false };
        dir->updates = array_append(dir->updates, update);
    }
    if (next == 0) {
        RedisModule_DictDelC(dir->localRefcounts, &hash, sizeof(hash), NULL);
        free(refcount);
    }
}

static void ShardDirectory_IncrementRefcount(uint64_t hash) {
    ShardDirectory *dir = &shardDirectory;
    ShardDirectory_EnsureClusterLayout(dir);
    pthread_rwlock_wrlock(&dir->lock);
    ShardDirectory_UpdateRefcount_locked(dir, hash, 1);
    pthread_rwlock_unlock(&dir->lock);
}

static void ShardDirectory_DecrementRefcount(uint64_t hash) {
    ShardDirectory *dir = &shardDirectory;
    ShardDirectory_EnsureClusterLayout(dir);
    pthread_rwlock_wrlock(&dir->lock);
    ShardDirectory_UpdateRefcount_locked(dir, hash, -1);
    pthread_rwlock_unlock(&dir->lock);
}

static bool LabelPairEquals(const Label *lhs, const Label *rhs) {
    size_t lkey_len = 0;
    size_t rkey_len = 0;
    size_t lval_len = 0;
    size_t rval_len = 0;
    const char *lkey = RedisModule_StringPtrLen(lhs->key, &lkey_len);
    const char *rkey = RedisModule_StringPtrLen(rhs->key, &rkey_len);
    if (lkey_len != rkey_len || memcmp(lkey, rkey, lkey_len) != 0) {
        return false;
    }
    const char *lval = RedisModule_StringPtrLen(lhs->value, &lval_len);
    const char *rval = RedisModule_StringPtrLen(rhs->value, &rval_len);
    return lval_len == rval_len && memcmp(lval, rval, lval_len) == 0;
}

static bool LabelPairExists(const Label *labels, size_t count, const Label *needle) {
    for (size_t i = 0; i < count; i++) {
        if (LabelPairEquals(&labels[i], needle)) {
            return true;
        }
    }
    return false;
}

void ShardDirectory_OnSeriesCreated(const Label *labels, size_t count) {
    if (!shardDirectory.enabled || count == 0) {
        return;
    }
    for (size_t i = 0; i < count; i++) {
        size_t key_len = 0;
        size_t val_len = 0;
        const char *key = RedisModule_StringPtrLen(labels[i].key, &key_len);
        const char *val = RedisModule_StringPtrLen(labels[i].value, &val_len);
        const uint64_t hash = HashLabelValue(key, key_len, val, val_len);
        ShardDirectory_IncrementRefcount(hash);
    }
}

void ShardDirectory_OnSeriesDeleted(const Label *labels, size_t count) {
    if (!shardDirectory.enabled || count == 0) {
        return;
    }
    for (size_t i = 0; i < count; i++) {
        size_t key_len = 0;
        size_t val_len = 0;
        const char *key = RedisModule_StringPtrLen(labels[i].key, &key_len);
        const char *val = RedisModule_StringPtrLen(labels[i].value, &val_len);
        const uint64_t hash = HashLabelValue(key, key_len, val, val_len);
        ShardDirectory_DecrementRefcount(hash);
    }
}

void ShardDirectory_OnSeriesLabelsChanged(const Label *oldLabels,
                                          size_t oldCount,
                                          const Label *newLabels,
                                          size_t newCount) {
    if (!shardDirectory.enabled) {
        return;
    }
    for (size_t i = 0; i < oldCount; i++) {
        if (!LabelPairExists(newLabels, newCount, &oldLabels[i])) {
            size_t key_len = 0;
            size_t val_len = 0;
            const char *key = RedisModule_StringPtrLen(oldLabels[i].key, &key_len);
            const char *val = RedisModule_StringPtrLen(oldLabels[i].value, &val_len);
            const uint64_t hash = HashLabelValue(key, key_len, val, val_len);
            ShardDirectory_DecrementRefcount(hash);
        }
    }
    for (size_t i = 0; i < newCount; i++) {
        if (!LabelPairExists(oldLabels, oldCount, &newLabels[i])) {
            size_t key_len = 0;
            size_t val_len = 0;
            const char *key = RedisModule_StringPtrLen(newLabels[i].key, &key_len);
            const char *val = RedisModule_StringPtrLen(newLabels[i].value, &val_len);
            const uint64_t hash = HashLabelValue(key, key_len, val, val_len);
            ShardDirectory_IncrementRefcount(hash);
        }
    }
}

typedef struct IndexLabelPairCtx {
    bool is_add;
} IndexLabelPairCtx;

static void ShardDirectory_LabelPairCallback(const char *label,
                                             size_t label_len,
                                             const char *value,
                                             size_t value_len,
                                             void *ctx) {
    IndexLabelPairCtx *cb_ctx = ctx;
    const uint64_t hash = HashLabelValue(label, label_len, value, value_len);
    if (cb_ctx->is_add) {
        ShardDirectory_IncrementRefcount(hash);
    } else {
        ShardDirectory_DecrementRefcount(hash);
    }
}

void ShardDirectory_OnSeriesDeletedByKey(RedisModuleString *ts_key) {
    if (!shardDirectory.enabled) {
        return;
    }
    IndexLabelPairCtx ctx = { .is_add = false };
    IndexIterateLabelPairs(ts_key, ShardDirectory_LabelPairCallback, &ctx);
}

const RTSShardBitset *ShardDirectory_GetShardsForLabelPair(const RedisModuleString *label,
                                                           const RedisModuleString *value) {
    ShardDirectory *dir = &shardDirectory;
    if (!dir->enabled) {
        return NULL;
    }
    ShardDirectory_EnsureClusterLayout(dir);
    size_t label_len = 0;
    size_t value_len = 0;
    const char *label_cstr = RedisModule_StringPtrLen(label, &label_len);
    const char *value_cstr = RedisModule_StringPtrLen(value, &value_len);
    const uint64_t hash = HashLabelValue(label_cstr, label_len, value_cstr, value_len);
    pthread_rwlock_rdlock(&dir->lock);
    RTSShardBitset *bitset =
        RedisModule_DictGetC(dir->shardsByLabelPair, &hash, sizeof(hash), NULL);
    pthread_rwlock_unlock(&dir->lock);
    return bitset;
}

RTSShardBitset ShardDirectory_ResolveTargets(const QueryPredicateList *predicates, bool *isKnown) {
    RTSShardBitset out = { 0 };
    if (isKnown) {
        *isKnown = false;
    }
    if (!shardDirectory.enabled || !TSGlobalConfig.shardPruningEnabled) {
        return out;
    }
    if (!predicates || predicates->count == 0) {
        return out;
    }
    ShardDirectory_EnsureClusterLayout(&shardDirectory);
    pthread_rwlock_rdlock(&shardDirectory.lock);
    if (shardDirectory.nodeIdsCount == 0 || shardDirectory.localShardIndex == SIZE_MAX) {
        pthread_rwlock_unlock(&shardDirectory.lock);
        return out;
    }
    const RTSShardBitset *pivot = NULL;
    size_t pivot_cardinality = 0;
    for (size_t i = 0; i < predicates->count; i++) {
        const QueryPredicate *pred = &predicates->list[i];
        if (pred->type != EQ || pred->valueListCount != 1) {
            pthread_rwlock_unlock(&shardDirectory.lock);
            ShardDirectory_StatsIncrementUnknownLookup();
            return out;
        }
        size_t key_len = 0;
        size_t val_len = 0;
        const char *key = RedisModule_StringPtrLen(pred->key, &key_len);
        const char *val = RedisModule_StringPtrLen(pred->valuesList[0], &val_len);
        const uint64_t hash = HashLabelValue(key, key_len, val, val_len);
        const RTSShardBitset *bitset =
            RedisModule_DictGetC(shardDirectory.shardsByLabelPair, &hash, sizeof(hash), NULL);
        if (!bitset) {
            pthread_rwlock_unlock(&shardDirectory.lock);
            ShardDirectory_StatsIncrementUnknownLookup();
            return out;
        }
        const size_t cardinality = ShardBitset_Count(bitset);
        if (!pivot || cardinality < pivot_cardinality) {
            pivot = bitset;
            pivot_cardinality = cardinality;
        }
    }
    if (!pivot) {
        pthread_rwlock_unlock(&shardDirectory.lock);
        ShardDirectory_StatsIncrementUnknownLookup();
        return out;
    }
    out = ShardBitset_Copy(pivot);
    for (size_t i = 0; i < predicates->count; i++) {
        const QueryPredicate *pred = &predicates->list[i];
        size_t key_len = 0;
        size_t val_len = 0;
        const char *key = RedisModule_StringPtrLen(pred->key, &key_len);
        const char *val = RedisModule_StringPtrLen(pred->valuesList[0], &val_len);
        const uint64_t hash = HashLabelValue(key, key_len, val, val_len);
        const RTSShardBitset *bitset =
            RedisModule_DictGetC(shardDirectory.shardsByLabelPair, &hash, sizeof(hash), NULL);
        if (!bitset) {
            ShardDirectory_FreeBitset(&out);
            pthread_rwlock_unlock(&shardDirectory.lock);
            ShardDirectory_StatsIncrementUnknownLookup();
            return out;
        }
        ShardBitset_AndInPlace(&out, bitset);
    }
    pthread_rwlock_unlock(&shardDirectory.lock);
    if (isKnown) {
        *isKnown = true;
    }
    return out;
}

bool ShardDirectory_BuildTargetNodeIds(const RTSShardBitset *bitset,
                                       bool includeLocal,
                                       char ***outNodeIds,
                                       size_t *outCount) {
    if (!outNodeIds || !outCount) {
        return false;
    }
    *outNodeIds = NULL;
    *outCount = 0;
    if (!bitset || !bitset->words || bitset->nwords == 0) {
        return false;
    }
    ShardDirectory *dir = &shardDirectory;
    ShardDirectory_EnsureClusterLayout(dir);
    pthread_rwlock_rdlock(&dir->lock);
    if (dir->nodeIdsCount == 0) {
        pthread_rwlock_unlock(&dir->lock);
        return false;
    }
    const size_t maxCount = dir->nodeIdsCount;
    char **ids = calloc(maxCount, sizeof(*ids));
    if (!ids) {
        pthread_rwlock_unlock(&dir->lock);
        return false;
    }
    size_t count = 0;
    for (size_t index = 0; index < dir->nodeIdsCount; index++) {
        if (!ShardBitset_Test(bitset, index)) {
            continue;
        }
        if (!includeLocal && index == dir->localShardIndex) {
            continue;
        }
        ids[count++] = strdup(dir->nodeIds[index]);
    }
    pthread_rwlock_unlock(&dir->lock);
    *outNodeIds = ids;
    *outCount = count;
    return true;
}

void ShardDirectory_FreeTargetNodeIds(char **nodeIds, size_t count) {
    if (!nodeIds) {
        return;
    }
    for (size_t i = 0; i < count; i++) {
        free(nodeIds[i]);
    }
    free(nodeIds);
}

size_t ShardDirectory_GetShardCount(void) {
    ShardDirectory *dir = &shardDirectory;
    ShardDirectory_EnsureClusterLayout(dir);
    pthread_rwlock_rdlock(&dir->lock);
    const size_t count = dir->nodeIdsCount;
    pthread_rwlock_unlock(&dir->lock);
    return count;
}

static void ShardDirectory_SendUpdates(RedisModuleCtx *ctx,
                                       bool is_add,
                                       const uint64_t *hashes,
                                       size_t count) {
    if (count == 0) {
        return;
    }
    ShardDirectory *dir = &shardDirectory;
    pthread_rwlock_rdlock(&dir->lock);
    if (dir->nodeIdsCount == 0 || dir->localShardIndex == SIZE_MAX) {
        pthread_rwlock_unlock(&dir->lock);
        return;
    }
    const size_t headerWords = 3;
    const size_t totalWords = headerWords + count;
    const size_t payloadSize = totalWords * sizeof(uint64_t);
    unsigned char *payload = malloc(payloadSize);
    if (!payload) {
        pthread_rwlock_unlock(&dir->lock);
        return;
    }
    uint64_t *cursor = (uint64_t *)payload;
    cursor[0] = htonu64(dir->clusterSig);
    cursor[1] = htonu64((uint64_t)dir->localShardIndex);
    cursor[2] = htonu64((uint64_t)count);
    for (size_t i = 0; i < count; i++) {
        cursor[headerWords + i] = htonu64(hashes[i]);
    }
    const uint8_t type = is_add ? RTS_CLUSTER_LP_ADD : RTS_CLUSTER_LP_REM;
    for (size_t i = 0; i < dir->nodeIdsCount; i++) {
        if (i == dir->localShardIndex) {
            continue;
        }
        RedisModule_SendClusterMessage(
            ctx, dir->nodeIds[i], type, (const char *)payload, (uint32_t)payloadSize);
    }
    pthread_rwlock_unlock(&dir->lock);
    free(payload);
    ShardDirectory_StatsIncrementUpdatesSent(count);
}

static void ShardDirectory_ProcessQueue(RedisModuleCtx *ctx, void *data) {
    REDISMODULE_NOT_USED(data);
    ShardDirectory *dir = &shardDirectory;
    if (!dir->enabled) {
        return;
    }
    ShardDirectory_EnsureClusterLayout(dir);
    pthread_rwlock_wrlock(&dir->lock);
    const size_t updatesCount = array_len(dir->updates);
    if (updatesCount == 0) {
        pthread_rwlock_unlock(&dir->lock);
        goto reschedule;
    }
    uint64_t *adds = calloc(updatesCount, sizeof(uint64_t));
    uint64_t *rems = calloc(updatesCount, sizeof(uint64_t));
    size_t addCount = 0;
    size_t remCount = 0;
    for (size_t i = 0; i < updatesCount; i++) {
        ShardDirectoryUpdate *update = dir->updates + i;
        if (update->is_add) {
            adds[addCount++] = update->label_hash;
        } else {
            rems[remCount++] = update->label_hash;
        }
    }
    array_free(dir->updates);
    dir->updates = array_new(ShardDirectoryUpdate, 4);
    pthread_rwlock_unlock(&dir->lock);

    ShardDirectory_SendUpdates(ctx, true, adds, addCount);
    ShardDirectory_SendUpdates(ctx, false, rems, remCount);
    free(adds);
    free(rems);

reschedule:
    dir->flushTimer =
        RedisModule_CreateTimer(ctx, SHARD_DIRECTORY_FLUSH_INTERVAL_MS, ShardDirectory_ProcessQueue, NULL);
}

static uint64_t ShardDirectory_ReadU64(const unsigned char *payload, size_t offset) {
    uint64_t value = 0;
    memcpy(&value, payload + offset, sizeof(value));
    return ntohu64(value);
}

static void ShardDirectory_OnClusterMessage(RedisModuleCtx *ctx,
                                            const char *sender_id,
                                            uint8_t type,
                                            const unsigned char *payload,
                                            uint32_t len) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(sender_id);
    if (!payload || len < sizeof(uint64_t) * 3) {
        return;
    }
    const uint64_t sig = ShardDirectory_ReadU64(payload, 0);
    const uint64_t shardId = ShardDirectory_ReadU64(payload, sizeof(uint64_t));
    const uint64_t count = ShardDirectory_ReadU64(payload, sizeof(uint64_t) * 2);
    const size_t expectedLen = (size_t)(3 + count) * sizeof(uint64_t);
    if (expectedLen != len) {
        return;
    }
    ShardDirectory *dir = &shardDirectory;
    ShardDirectory_EnsureClusterLayout(dir);
    pthread_rwlock_wrlock(&dir->lock);
    if (sig != dir->clusterSig || shardId >= dir->nodeIdsCount) {
        dir->needsRefresh = true;
        pthread_rwlock_unlock(&dir->lock);
        return;
    }
    const bool is_add = (type == RTS_CLUSTER_LP_ADD);
    const unsigned char *cursor = payload + sizeof(uint64_t) * 3;
    for (uint64_t i = 0; i < count; i++) {
        uint64_t hash = ShardDirectory_ReadU64(cursor, (size_t)i * sizeof(uint64_t));
        RTSShardBitset *bitset = ShardDirectory_GetOrCreateBitset_locked(dir, hash);
        if (!bitset) {
            continue;
        }
        if (is_add) {
            ShardBitset_Set(bitset, (size_t)shardId);
        } else {
            ShardBitset_Clear(bitset, (size_t)shardId);
        }
    }
    pthread_rwlock_unlock(&dir->lock);
    ShardDirectory_StatsIncrementUpdatesReceived(count);
}

void ShardDirectory_Init(RedisModuleCtx *ctx) {
    memset(&shardDirectory, 0, sizeof(shardDirectory));
    memset(&shardDirectoryStats, 0, sizeof(shardDirectoryStats));
    shardDirectory.enabled = ShardDirectory_IsClusterEnabled();
    shardDirectory.needsRefresh = true;
    shardDirectory.localShardIndex = SIZE_MAX;
    pthread_rwlock_init(&shardDirectory.lock, NULL);
    shardDirectory.shardsByLabelPair = RedisModule_CreateDict(NULL);
    shardDirectory.localRefcounts = RedisModule_CreateDict(NULL);
    shardDirectory.updates = array_new(ShardDirectoryUpdate, 4);
    if (shardDirectory.enabled) {
        ShardDirectory_EnsureClusterLayout(&shardDirectory);
        RedisModule_RegisterClusterMessageReceiver(ctx, RTS_CLUSTER_LP_ADD, ShardDirectory_OnClusterMessage);
        RedisModule_RegisterClusterMessageReceiver(ctx, RTS_CLUSTER_LP_REM, ShardDirectory_OnClusterMessage);
        shardDirectory.flushTimer =
            RedisModule_CreateTimer(ctx, SHARD_DIRECTORY_FLUSH_INTERVAL_MS, ShardDirectory_ProcessQueue, NULL);
    }
}

void ShardDirectory_Free(void) {
    ShardDirectory *dir = &shardDirectory;
    if (dir->flushTimer && RedisModule_StopTimer) {
        RedisModule_StopTimer(rts_staticCtx, dir->flushTimer, NULL);
        dir->flushTimer = 0;
    }
    pthread_rwlock_wrlock(&dir->lock);
    ShardDirectory_ClearBitsets_locked(dir, false);
    if (dir->localRefcounts) {
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(dir->localRefcounts, "^", NULL, 0);
        void *entry = NULL;
        while (RedisModule_DictNextC(iter, NULL, &entry) != NULL) {
            free(entry);
        }
        RedisModule_DictIteratorStop(iter);
        RedisModule_FreeDict(NULL, dir->localRefcounts);
        dir->localRefcounts = NULL;
    }
    ShardDirectory_ClearNodeIds_locked(dir);
    if (dir->updates) {
        array_free(dir->updates);
        dir->updates = NULL;
    }
    pthread_rwlock_unlock(&dir->lock);
    pthread_rwlock_destroy(&dir->lock);
}
