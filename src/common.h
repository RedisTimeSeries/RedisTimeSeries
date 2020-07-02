
#pragma once

#define ERR "ERR"
#define RTS_ReplyError(ctx, err_type, msg) RedisModule_ReplyWithError(ctx, err_type " " msg);
#define RTS_ReplyGeneralError(ctx, msg) RTS_ReplyError(ctx, ERR, msg);
