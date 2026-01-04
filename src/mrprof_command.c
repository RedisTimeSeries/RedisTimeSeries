/*
 * TS.MRPROF - expose LibMR profiling counters (MRProf) via a Redis command.
 *
 * Usage:
 *   TS.MRPROF GET
 *   TS.MRPROF RESET
 *   TS.MRPROF ENABLE 0|1
 *
 * Reply (GET):
 *   [ enabled, [ [stage, count, total_ns, avg_ns], ... ] ]
 */

#include "RedisModulesSDK/redismodule.h"
#include "reply.h"

#include "LibMR/src/mr_prof.h"

#include <string.h>

static const char *MRProfStageToString(MRProfStage s) {
    switch (s) {
        case MRPROF_STAGE_TS_CMD_ENTRY:
            return "ts_cmd_entry";
        case MRPROF_STAGE_TS_COORD_MERGE_REPLY:
            return "ts_coord_merge_reply";
        case MRPROF_STAGE_MAIN_INNERCOMM_CMD:
            return "main_innercomm_cmd";
        case MRPROF_STAGE_EL_SENDMSG_TASK:
            return "el_sendmsg_task";
        case MRPROF_STAGE_EL_SEND_ASYNC_CMD:
            return "el_send_async_cmd";
        case MRPROF_STAGE_EL_INNERCOMM_DISPATCH:
            return "el_innercomm_dispatch";
        case MRPROF_STAGE_WORKER_QUEUE_WAIT:
            return "worker_queue_wait";
        case MRPROF_STAGE_WORKER_DESERIALIZE_EXEC:
            return "worker_deserialize_exec";
        case MRPROF_STAGE_WORKER_SET_RECORD:
            return "worker_set_record";
        case MRPROF_STAGE_WORKER_STEP_DONE:
            return "worker_step_done";
        case MRPROF_STAGE_RTS_CTX_LOCK:
            return "rts_ctx_lock";
        case MRPROF_STAGE_RTS_CTX_LOCK_WAIT:
            return "rts_ctx_lock_wait";
        case MRPROF_STAGE_RTS_CTX_LOCK_HOLD:
            return "rts_ctx_lock_hold";
        case MRPROF_STAGE_RTS_QUERYINDEX:
            return "rts_queryindex";
        case MRPROF_STAGE_RTS_GETSERIES_LOOP:
            return "rts_getseries_loop";
        default:
            return "unknown";
    }
}

int TSDB_mrprof(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    size_t op_len = 0;
    const char *op = RedisModule_StringPtrLen(argv[1], &op_len);

    if (op_len == 3 && !strncasecmp(op, "GET", 3)) {
        MRProfSnapshot snap;
        MRProf_GetSnapshot(&snap);

        RedisModule_ReplyWithArray(ctx, 2);
        RedisModule_ReplyWithLongLong(ctx, snap.enabled);

        RedisModule_ReplyWithArray(ctx, MRPROF_STAGE_MAX);
        for (int i = 0; i < MRPROF_STAGE_MAX; ++i) {
            const char *name = MRProfStageToString((MRProfStage)i);
            uint64_t count = snap.stages[i].count;
            uint64_t total_ns = snap.stages[i].total_ns;
            uint64_t avg_ns = count ? (total_ns / count) : 0;

            RedisModule_ReplyWithArray(ctx, 4);
            RedisModule_ReplyWithCString(ctx, name);
            RedisModule_ReplyWithLongLong(ctx, (long long)count);
            RedisModule_ReplyWithLongLong(ctx, (long long)total_ns);
            RedisModule_ReplyWithLongLong(ctx, (long long)avg_ns);
        }

        return REDISMODULE_OK;
    }

    if (op_len == 5 && !strncasecmp(op, "RESET", 5)) {
        MRProf_Reset();
        RedisModule_ReplyWithSimpleString(ctx, "OK");
        return REDISMODULE_OK;
    }

    if (op_len == 6 && !strncasecmp(op, "ENABLE", 6)) {
        if (argc != 3) {
            return RedisModule_WrongArity(ctx);
        }
        long long enabled = 0;
        if (RedisModule_StringToLongLong(argv[2], &enabled) != REDISMODULE_OK ||
            (enabled != 0 && enabled != 1)) {
            RedisModule_ReplyWithError(ctx, "ERR ENABLE expects 0 or 1");
            return REDISMODULE_OK;
        }
        MRProf_SetEnabled((int)enabled);
        RedisModule_ReplyWithSimpleString(ctx, "OK");
        return REDISMODULE_OK;
    }

    RedisModule_ReplyWithError(ctx, "ERR unknown subcommand (expected GET|RESET|ENABLE)");
    return REDISMODULE_OK;
}


