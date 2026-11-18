/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef RTS_RDB_LOAD_LOGGING_H
#define RTS_RDB_LOAD_LOGGING_H

#include "redismodule.h"
#include <stdarg.h>
#include <stdio.h>

/* Bound and hex-encode binary keys so a malformed key cannot inject log lines.
 * Log metadata only: never pass sample values or serialized payloads here. */
static inline __attribute__((format(printf, 2, 3))) void RdbLoad_LogError(RedisModuleIO *io,
                                                                          const char *fmt,
                                                                          ...) {
    // Unit tests and some module callbacks can reach failure paths without an IO context.
    // The Redis APIs are also optional on older supported Redis versions.
    if (io == NULL || RedisModule_LogIOError == NULL) {
        return;
    }

    char reason[512] = { 0 };
    va_list args;
    va_start(args, fmt);
    vsnprintf(reason, sizeof(reason), fmt, args);
    va_end(args);
    reason[sizeof(reason) - 1] = '\0';

    const RedisModuleString *key = NULL;
    size_t keyLen = 0;
    const char *bytes = NULL;
    if (RedisModule_GetKeyNameFromIO != NULL) {
        key = RedisModule_GetKeyNameFromIO(io);
    }
    if (key != NULL && RedisModule_StringPtrLen != NULL) {
        bytes = RedisModule_StringPtrLen(key, &keyLen);
    }

    const size_t shown = keyLen < 128 ? keyLen : 128;
    char hex[257];
    static const char digits[] = "0123456789abcdef";
    if (bytes == NULL) {
        key = NULL;
        keyLen = 0;
    } else {
        for (size_t i = 0; i < shown; ++i) {
            const unsigned char byte = (unsigned char)bytes[i];
            hex[2 * i] = digits[byte >> 4];
            hex[2 * i + 1] = digits[byte & 15];
        }
        hex[2 * shown] = '\0';
    }

    // Redis has no "error" log level: it silently falls back to verbose.
    RedisModule_LogIOError(io,
                           "warning",
                           "RDB load failed: key_hex=%s key_len=%zu key_truncated=%d %s",
                           key ? hex : "unavailable",
                           keyLen,
                           keyLen > shown,
                           reason);
}
#endif
