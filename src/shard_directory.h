/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef SHARD_DIRECTORY_H
#define SHARD_DIRECTORY_H

#include "RedisModulesSDK/redismodule.h"
#include "indexer.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct RTSShardBitset {
    uint64_t *words;
    size_t nwords;
} RTSShardBitset;

typedef struct RTSShardDirectoryStats {
    uint64_t mrangecoord_full_fanout;
    uint64_t mrangecoord_pruned;
    uint64_t mrangecoord_targets_total;
    uint64_t directory_updates_sent;
    uint64_t directory_updates_received;
    uint64_t directory_unknown_lookups;
} RTSShardDirectoryStats;

void ShardDirectory_Init(RedisModuleCtx *ctx);
void ShardDirectory_Free(void);

void ShardDirectory_OnSeriesCreated(const Label *labels, size_t count);
void ShardDirectory_OnSeriesDeleted(const Label *labels, size_t count);
void ShardDirectory_OnSeriesLabelsChanged(const Label *oldLabels,
                                          size_t oldCount,
                                          const Label *newLabels,
                                          size_t newCount);
void ShardDirectory_OnSeriesDeletedByKey(RedisModuleString *ts_key);

const RTSShardBitset *ShardDirectory_GetShardsForLabelPair(const RedisModuleString *label,
                                                           const RedisModuleString *value);
RTSShardBitset ShardDirectory_ResolveTargets(const QueryPredicateList *predicates, bool *isKnown);
void ShardDirectory_FreeBitset(RTSShardBitset *bitset);
bool ShardDirectory_BitsetIsEmpty(const RTSShardBitset *bitset);
bool ShardDirectory_LocalShardIncluded(const RTSShardBitset *bitset);

bool ShardDirectory_BuildTargetNodeIds(const RTSShardBitset *bitset,
                                       bool includeLocal,
                                       char ***outNodeIds,
                                       size_t *outCount);
void ShardDirectory_FreeTargetNodeIds(char **nodeIds, size_t count);
size_t ShardDirectory_GetShardCount(void);

const RTSShardDirectoryStats *ShardDirectory_GetStats(void);
void ShardDirectory_StatsIncrementFullFanout(void);
void ShardDirectory_StatsIncrementPruned(size_t targetCount);
void ShardDirectory_StatsIncrementUnknownLookup(void);
void ShardDirectory_StatsIncrementUpdatesSent(size_t count);
void ShardDirectory_StatsIncrementUpdatesReceived(size_t count);

#endif /* SHARD_DIRECTORY_H */
