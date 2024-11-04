#pragma once

struct RedisModuleUser;
struct RedisModuleCtx;
struct RedisModuleString;
struct RedisModuleUser * (*RedisModule_GetModuleUserFromUserName)(struct RedisModuleString *name);
struct RedisModuleString * (*RedisModule_GetCurrentUserName)(struct RedisModuleCtx *ctx);
void (*RedisModule_FreeString)(struct RedisModuleCtx *ctx, struct RedisModuleString *str);

#include <stdlib.h>

#define RTS_ERR "ERR"
#define RTS_ReplyError(ctx, err_type, msg) RedisModule_ReplyWithError(ctx, err_type " " msg);
#define RTS_ReplyGeneralError(ctx, msg) RTS_ReplyError(ctx, RTS_ERR, msg);


#if (defined(DEBUG) || defined(_DEBUG)) && !defined(NDEBUG)
#include "readies/cetara/diag/gdb.h"
#endif
