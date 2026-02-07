#include "common.h"
#include "tsdb.h"
#include "indexer.h"
#include "shard_directory.h"

int NotifyCallback(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key) {
    if (strcasecmp(event, "del") ==
            0 || // unlink also notifies with del with freeseries called before
        strcasecmp(event, "type_changed") == 0 ||
        strcasecmp(event, "expired") == 0 || strcasecmp(event, "evict") == 0 ||
        strcasecmp(event, "evicted") == 0 || strcasecmp(event, "key_trimmed") == 0 ||
        strcasecmp(event, "trimmed") == 0 // only on enterprise
    ) {
        ShardDirectory_OnSeriesDeletedByKey(key);
        RemoveIndexedMetric(key);
        return REDISMODULE_OK;
    }

    if (strcasecmp(event, "restore") == 0) {
        RestoreKey(ctx, key);
        return REDISMODULE_OK;
    }

    if (strcasecmp(event, "rename_from") == 0) { // include also renamenx
        RenameSeriesFrom(ctx, key);
        return REDISMODULE_OK;
    }

    if (strcasecmp(event, "rename_to") == 0) { // include also renamenx
        RenameSeriesTo(ctx, key);
        return REDISMODULE_OK;
    }

    // Will be called in replicaof or on load rdb on load time
    if (strcasecmp(event, "loaded") == 0) {
        IndexMetricFromName(ctx, key);
        return REDISMODULE_OK;
    }

    // if (strcasecmp(event, "short read") == 0) // Nothing should be done
    return REDISMODULE_OK;
}
