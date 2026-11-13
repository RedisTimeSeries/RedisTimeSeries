/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef TS_DISTRIBUTED_GROUP_H
#define TS_DISTRIBUTED_GROUP_H

#include "query_language.h"
#include "tsdb.h"
#include "LibMR/deps/hiredis/hiredis.h"

typedef struct DistributedGroups DistributedGroups;
double ShardReplyDouble(const redisReply *reply);

bool DistributedGroupSupported(TS_AGG_TYPES_T reducer);
DistributedGroups *DistributedGroups_New(TS_AGG_TYPES_T reducer);
void DistributedGroups_Free(DistributedGroups *groups);
void DistributedGroups_AddSeries(DistributedGroups *groups,
                                 Series *series,
                                 const char *label,
                                 const RangeArgs *args,
                                 bool excludeEmpty);
void DistributedGroups_Merge(DistributedGroups *dest, const DistributedGroups *source);
void DistributedGroups_ReplyPartial(RedisModuleCtx *ctx, const DistributedGroups *groups);
DistributedGroups *DistributedGroups_Parse(const redisReply *reply);
void DistributedGroups_Reply(RedisModuleCtx *ctx,
                             DistributedGroups *groups,
                             const MRangeArgs *args);

#define TS_GROUP_REPLY_VERSION "TS.GROUPS.1"

#endif
