/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef REDISTIMESERIES_LIBMR_RESULT_VALIDATION_H
#define REDISTIMESERIES_LIBMR_RESULT_VALIDATION_H

#include "libmr_integration.h"

// Validate every shard before emitting any reply. A malformed later shard must
// fail the whole query rather than return a partial result or an invalid RESP reply.
static inline bool validate_gears_results(ExecutionCtx *eCtx,
                                          RedisModuleCtx *ctx,
                                          MRRecordType *containerType,
                                          MRRecordType *elementType) {
    const bool isMap = containerType == GetMapRecordType();
    const size_t len = MR_ExecutionCtxGetResultsLen(eCtx);
    for (size_t i = 0; i < len; i++) {
        Record *record = MR_ExecutionCtxGetResult(eCtx, i);
        if (!record || record->recordType != containerType)
            goto invalid;
        const size_t count =
            isMap ? MapRecord_GetLen((MapRecord *)record) : ListRecord_GetLen((ListRecord *)record);
        if (isMap && count % 2 != 0)
            goto invalid;
        for (size_t j = 0; j < count; j++) {
            Record *child = isMap ? MapRecord_GetRecord((MapRecord *)record, j)
                                  : ListRecord_GetRecord((ListRecord *)record, j);
            MRRecordType *expected = isMap && j % 2 == 0 ? GetStringRecordType() : elementType;
            if (!child || child->recordType != expected)
                goto invalid;
        }
    }
    return true;

invalid:
    RedisModule_Log(ctx, "warning", "Unexpected shard result type in multi-key command");
    RedisModule_ReplyWithError(ctx, "ERR Unexpected shard result type in multi-key command");
    return false;
}

#endif
