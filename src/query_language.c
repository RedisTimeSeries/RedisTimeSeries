/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "query_language.h"

#include "rmutil/alloc.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"

int parseLabelsFromArgs(RedisModuleString **argv, int argc, size_t *label_count, Label **labels) {
    int pos = RMUtil_ArgIndex("LABELS", argv, argc);
    int first_label_pos = pos + 1;
    Label *labelsResult = NULL;
    *label_count = 0;
    if (pos < 0) {
        *labels = NULL;
        return REDISMODULE_OK;
    }
    *label_count = (size_t)(max(0, (argc - first_label_pos) / 2));
    if (*label_count > 0) {
        labelsResult = malloc(sizeof(Label) * (*label_count));
        for (int i = 0; i < *label_count; i++) {
            RedisModuleString *key = argv[first_label_pos + i * 2];
            RedisModuleString *value = argv[first_label_pos + i * 2 + 1];

            // Verify Label Key or Value are not empty strings
            size_t keyLen, valueLen;
            RedisModule_StringPtrLen(key, &keyLen);
            RedisModule_StringPtrLen(value, &valueLen);
            if (keyLen == 0 || valueLen == 0 ||
                strpbrk(RedisModule_StringPtrLen(value, NULL), "(),")) {
                FreeLabels(labelsResult, i); // need to release prior key values too
                return REDISMODULE_ERR;
            }

            labelsResult[i].key = RedisModule_CreateStringFromString(NULL, key);
            labelsResult[i].value = RedisModule_CreateStringFromString(NULL, value);
        };
    }
    *labels = labelsResult;
    return REDISMODULE_OK;
}

int ParseDuplicatePolicy(RedisModuleCtx *ctx,
                         RedisModuleString **argv,
                         int argc,
                         const char *arg_prefix,
                         DuplicatePolicy *policy) {
    RedisModuleString *duplicationPolicyInput = NULL;
    if (RMUtil_ArgIndex(arg_prefix, argv, argc) != -1) {
        if (RMUtil_ParseArgsAfter(arg_prefix, argv, argc, "s", &duplicationPolicyInput) !=
            REDISMODULE_OK) {
            RTS_ReplyGeneralError(ctx, "TSDB: Couldn't parse DUPLICATE_POLICY");
            return TSDB_ERROR;
        }

        DuplicatePolicy parsePolicy = RMStringLenDuplicationPolicyToEnum(duplicationPolicyInput);
        if (parsePolicy == DP_INVALID) {
            RTS_ReplyGeneralError(ctx, "TSDB: Unknown DUPLICATE_POLICY");
            return TSDB_ERROR;
        }
        *policy = parsePolicy;
        return TSDB_OK;
    }
    return TSDB_OK;
}

int parseCreateArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, CreateCtx *cCtx) {
    cCtx->retentionTime = TSGlobalConfig.retentionPolicy;
    cCtx->chunkSizeBytes = TSGlobalConfig.chunkSizeBytes;
    cCtx->labelsCount = 0;
    if (parseLabelsFromArgs(argv, argc, &cCtx->labelsCount, &cCtx->labels) == REDISMODULE_ERR) {
        RTS_ReplyGeneralError(ctx, "TSDB: Couldn't parse LABELS");
        return REDISMODULE_ERR;
    }

    if (RMUtil_ArgIndex("RETENTION", argv, argc) > 0 &&
        RMUtil_ParseArgsAfter("RETENTION", argv, argc, "l", &cCtx->retentionTime) !=
            REDISMODULE_OK) {
        RTS_ReplyGeneralError(ctx, "TSDB: Couldn't parse RETENTION");
        return REDISMODULE_ERR;
    }

    if (cCtx->retentionTime < 0) {
        RedisModule_ReplyWithError(ctx, "TSDB: Couldn't parse RETENTION");
        return REDISMODULE_ERR;
    }

    if (RMUtil_ArgIndex("CHUNK_SIZE", argv, argc) > 0 &&
        RMUtil_ParseArgsAfter("CHUNK_SIZE", argv, argc, "l", &cCtx->chunkSizeBytes) !=
            REDISMODULE_OK) {
        RTS_ReplyGeneralError(ctx, "TSDB: Couldn't parse CHUNK_SIZE");
        return REDISMODULE_ERR;
    }

    if (cCtx->chunkSizeBytes <= 0) {
        RTS_ReplyGeneralError(ctx, "TSDB: Couldn't parse CHUNK_SIZE");
        return REDISMODULE_ERR;
    }

    if (RMUtil_ArgIndex("UNCOMPRESSED", argv, argc) > 0) {
        cCtx->options |= SERIES_OPT_UNCOMPRESSED;
    }

    cCtx->duplicatePolicy = DP_NONE;
    if (ParseDuplicatePolicy(ctx, argv, argc, DUPLICATE_POLICY_ARG, &cCtx->duplicatePolicy) !=
        TSDB_OK) {
        return TSDB_ERROR;
    }

    return REDISMODULE_OK;
}

int _parseAggregationArgs(RedisModuleCtx *ctx,
                          RedisModuleString **argv,
                          int argc,
                          api_timestamp_t *time_delta,
                          int *agg_type) {
    RedisModuleString *aggTypeStr = NULL;
    int offset = RMUtil_ArgIndex("AGGREGATION", argv, argc);
    if (offset > 0) {
        long long temp_time_delta = 0;
        if (RMUtil_ParseArgs(argv, argc, offset + 1, "sl", &aggTypeStr, &temp_time_delta) !=
            REDISMODULE_OK) {
            RTS_ReplyGeneralError(ctx, "TSDB: Couldn't parse AGGREGATION");
            return TSDB_ERROR;
        }

        if (!aggTypeStr) {
            RTS_ReplyGeneralError(ctx, "TSDB: Unknown aggregation type");
            return TSDB_ERROR;
        }

        *agg_type = RMStringLenAggTypeToEnum(aggTypeStr);

        if (*agg_type < 0 || *agg_type >= TS_AGG_TYPES_MAX) {
            RTS_ReplyGeneralError(ctx, "TSDB: Unknown aggregation type");
            return TSDB_ERROR;
        }

        if (temp_time_delta <= 0) {
            RTS_ReplyGeneralError(ctx, "TSDB: timeBucket must be greater than zero");
            return TSDB_ERROR;
        } else {
            *time_delta = (api_timestamp_t)temp_time_delta;
        }

        return TSDB_OK;
    }

    return TSDB_NOTEXISTS;
}

int parseAggregationArgs(RedisModuleCtx *ctx,
                         RedisModuleString **argv,
                         int argc,
                         api_timestamp_t *time_delta,
                         AggregationClass **agg_object) {
    int agg_type;
    int result = _parseAggregationArgs(ctx, argv, argc, time_delta, &agg_type);
    if (result == TSDB_OK) {
        *agg_object = GetAggClass(agg_type);
        if (*agg_object == NULL) {
            RTS_ReplyGeneralError(ctx, "TSDB: Failed to retrieve aggregation class");
            return TSDB_ERROR;
        }
        return TSDB_OK;
    } else {
        return result;
    }
}

int parseRangeArguments(RedisModuleCtx *ctx,
                        Series *series,
                        int start_index,
                        RedisModuleString **argv,
                        api_timestamp_t *start_ts,
                        api_timestamp_t *end_ts) {
    size_t start_len;
    const char *start = RedisModule_StringPtrLen(argv[start_index], &start_len);
    if (strcmp(start, "-") == 0) {
        *start_ts = 0;
    } else {
        if (RedisModule_StringToLongLong(argv[start_index], (long long int *)start_ts) !=
            REDISMODULE_OK) {
            RTS_ReplyGeneralError(ctx, "TSDB: wrong fromTimestamp");
            return REDISMODULE_ERR;
        }
    }

    size_t end_len;
    const char *end = RedisModule_StringPtrLen(argv[start_index + 1], &end_len);
    if (strcmp(end, "+") == 0) {
        *end_ts = series->lastTimestamp;
    } else {
        if (RedisModule_StringToLongLong(argv[start_index + 1], (long long int *)end_ts) !=
            REDISMODULE_OK) {
            RTS_ReplyGeneralError(ctx, "TSDB: wrong toTimestamp");
            return REDISMODULE_ERR;
        }
    }

    return REDISMODULE_OK;
}

int parseCountArgument(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, long long *count) {
    int offset = RMUtil_ArgIndex("COUNT", argv, argc);
    if (offset > 0) {
        if (offset + 1 == argc) {
            RTS_ReplyGeneralError(ctx, "TSDB: COUNT argument is missing");
            return TSDB_ERROR;
        }
        if (strcasecmp(RedisModule_StringPtrLen(argv[offset - 1], NULL), "AGGREGATION") == 0) {
            int second_offset =
                offset + 1 + RMUtil_ArgIndex("COUNT", argv + offset + 1, argc - offset - 1);
            if (offset == second_offset || second_offset + 1 >= argc) {
                return TSDB_OK;
            }
            offset = second_offset;
        }
        if (RedisModule_StringToLongLong(argv[offset + 1], count) != REDISMODULE_OK) {
            RTS_ReplyGeneralError(ctx, "TSDB: Couldn't parse COUNT");
            return TSDB_ERROR;
        }
    }
    return TSDB_OK;
}

int parseLabelListFromArgs(RedisModuleCtx *ctx,
                           RedisModuleString **argv,
                           int start,
                           int query_count,
                           QueryPredicate *queries) {
    QueryPredicate *query = queries;
    for (int i = start; i < start + query_count; i++) {
        size_t _s;
        const char *str2 = RedisModule_StringPtrLen(argv[i], &_s);
        if (strstr(str2, "!=(") != NULL) { // order is important! Must be before "!=".
            query->type = LIST_NOTMATCH;
            if (parsePredicate(ctx, argv[i], query, "!=(") == TSDB_ERROR) {
                return TSDB_ERROR;
            }
        } else if (strstr(str2, "!=") != NULL) {
            query->type = NEQ;
            if (parsePredicate(ctx, argv[i], query, "!=") == TSDB_ERROR) {
                return TSDB_ERROR;
            }
            if (query->valueListCount == 0) {
                query->type = CONTAINS;
            }
        } else if (strstr(str2, "=(") != NULL) { // order is important! Must be before "=".
            query->type = LIST_MATCH;
            if (parsePredicate(ctx, argv[i], query, "=(") == TSDB_ERROR) {
                return TSDB_ERROR;
            }
        } else if (strstr(str2, "=") != NULL) {
            query->type = EQ;
            if (parsePredicate(ctx, argv[i], query, "=") == TSDB_ERROR) {
                return TSDB_ERROR;
            }
            if (query->valueListCount == 0) {
                query->type = NCONTAINS;
            }
        } else {
            return TSDB_ERROR;
        }
        query++;
    }
    return TSDB_OK;
}
