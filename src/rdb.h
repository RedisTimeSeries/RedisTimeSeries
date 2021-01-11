/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "redismodule.h"
#include "tsdb.h"

#ifndef RDB_H
#define RDB_H

#define TS_ENC_VER 0
#define TS_UNCOMPRESSED_VER 1
#define TS_SIZE_RDB_VER 2

void *series_rdb_load(RedisModuleIO *io, int encver);
void series_rdb_save(RedisModuleIO *io, void *value);

#endif
