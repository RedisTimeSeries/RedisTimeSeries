/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 * Copyright 2020 IoT.BzH
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "blob.h"

#include "redismodule.h"

#include <string.h>
#include "rmutil/alloc.h"

#define EMPTY_BLOB ""
#define EMPTY_BLOB_SIZE 1

TSBlob *NewBlob(const char *data, size_t len) {
    TSBlob *blob = RedisModule_Calloc(1, sizeof(TSBlob));
    if (data == NULL || len == 0)
        return blob;

    blob->len = len;
    blob->data = RedisModule_Alloc(blob->len);
    memcpy(blob->data, data, blob->len);
    return blob;
}

TSBlob *CopyBlob(const TSBlob *src) {
    TSBlob *blob = RedisModule_Alloc(sizeof(TSBlob));
    blob->len = src->len;
    blob->data = RedisModule_Alloc(blob->len);
    memcpy(blob->data, src->data, blob->len);
    return blob;
}

void FreeBlob(TSBlob *blob) {
    if (blob->data)
        RedisModule_Free(blob->data);
    RedisModule_Free(blob);
}

void RedisModule_SaveBlob(RedisModuleIO *io, const TSBlob *blob) {
    if (!blob) {
        RedisModule_SaveStringBuffer(io, EMPTY_BLOB, EMPTY_BLOB_SIZE);
        return;
    }
    RedisModule_SaveStringBuffer(io, blob->data, blob->len);
}

TSBlob *RedisModule_LoadBlob(RedisModuleIO *io) {
    size_t len;
    char *data = RedisModule_LoadStringBuffer(io, &len);
    return NewBlob(data, len);
}
