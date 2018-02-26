#include "redismodule.h"
#include "tsdb.h"

#ifndef RDB_H
#define RDB_H

#define TS_ENC_VER 0

void *series_rdb_load(RedisModuleIO *io, int encver);
void series_rdb_save(RedisModuleIO *io, void *value);

#endif