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
    char reason[512];
    va_list args;
    va_start(args, fmt);
    vsnprintf(reason, sizeof(reason), fmt, args);
    va_end(args);

    const RedisModuleString *key = RedisModule_GetKeyNameFromIO(io);
    size_t keyLen = 0;
    const char *bytes = key ? RedisModule_StringPtrLen(key, &keyLen) : NULL;
    const size_t shown = keyLen < 128 ? keyLen : 128;
    char hex[257];
    static const char digits[] = "0123456789abcdef";
    for (size_t i = 0; i < shown; ++i) {
        const unsigned char byte = (unsigned char)bytes[i];
        hex[2 * i] = digits[byte >> 4];
        hex[2 * i + 1] = digits[byte & 15];
    }
    hex[2 * shown] = '\0';
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
