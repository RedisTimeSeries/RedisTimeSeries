
#pragma once
#include <stdbool.h>

#define RTS_ERR "ERR"
#define RTS_ReplyError(ctx, err_type, msg) RedisModule_ReplyWithError(ctx, err_type " " msg);
extern bool isErrorReplied;
static inline void Unset_ErrorFlag() { isErrorReplied = false; }
static inline bool Get_ErrorFlag() { return isErrorReplied; }
#define RTS_ReplyGeneralError(ctx, msg)      \
__extension__({                              \
    if(!isErrorReplied) {                    \
        isErrorReplied = true;               \
        RTS_ReplyError(ctx, RTS_ERR, msg);   \
    }                                        \
    REDISMODULE_OK;                          \
})
