/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */

#ifndef RDB_H
#define RDB_H

#include "tsdb.h"

#include "RedisModulesSDK/redismodule.h"

#define TS_ENC_VER 0
#define TS_UNCOMPRESSED_VER 1
#define TS_SIZE_RDB_VER 2
#define TS_IS_RESSETED_DUP_POLICY_RDB_VER 3
#define TS_OVERFLOW_RDB_VER 4
#define TS_REPLICAOF_SUPPORT_VER 5
#define TS_ALIGNMENT_TS_VER 6
#define TS_LAST_AGGREGATION_EMPTY 7
#define TS_CREATE_IGNORE_VER 8

// This flag should be updated whenever a new rdb version is introduced
#define TS_LATEST_ENCVER TS_CREATE_IGNORE_VER

extern int last_rdb_load_version;

void *series_rdb_load(RedisModuleIO *io, int encver);
void series_rdb_save(RedisModuleIO *io, void *value);

#endif
