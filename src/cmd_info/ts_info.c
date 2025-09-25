#include "command_info.h"

// ===============================
// TS.GET key [LATEST]
// ===============================
static const RedisModuleCommandKeySpec TS_GET_KEYSPECS[] = {
    { .flags = REDISMODULE_CMD_KEY_RO,
      .begin_search_type = REDISMODULE_KSPEC_BS_INDEX,
      .bs.index = { .pos = 1 },
      .find_keys_type = REDISMODULE_KSPEC_FK_RANGE,
      .fk.range = { .lastkey = 0, .keystep = 1, .limit = 0 } },
    { 0 }
};

static const RedisModuleCommandArg TS_GET_ARGS[] = {
    { .name = "key", .type = REDISMODULE_ARG_TYPE_KEY, .key_spec_index = 0 },
    { .name = "latest",
      .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .token = "LATEST" },
    { 0 }
};

static const RedisModuleCommandInfo TS_GET_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary = "Get the sample with the highest timestamp from a given time series",
    .complexity = "O(1)",
    .since = "1.0.0",
    .arity = -2,
    .key_specs = (RedisModuleCommandKeySpec *)TS_GET_KEYSPECS,
    .args = (RedisModuleCommandArg *)TS_GET_ARGS,
};

// ===============================
// TS.DELETERULE source_key dest_key
// ===============================
static const RedisModuleCommandKeySpec TS_DELETERULE_KEYSPECS[] = {
    { .flags = REDISMODULE_CMD_KEY_RO,
      .begin_search_type = REDISMODULE_KSPEC_BS_INDEX,
      .bs.index = { .pos = 1 },
      .find_keys_type = REDISMODULE_KSPEC_FK_RANGE,
      .fk.range = { .lastkey = 0, .keystep = 1, .limit = 0 } },
    { .flags = REDISMODULE_CMD_KEY_RW,
      .begin_search_type = REDISMODULE_KSPEC_BS_INDEX,
      .bs.index = { .pos = 2 },
      .find_keys_type = REDISMODULE_KSPEC_FK_RANGE,
      .fk.range = { .lastkey = 0, .keystep = 1, .limit = 0 } },
    { 0 }
};

static const RedisModuleCommandArg TS_DELETERULE_ARGS[] = {
    { .name = "sourceKey", .type = REDISMODULE_ARG_TYPE_KEY, .key_spec_index = 0 },
    { .name = "destKey", .type = REDISMODULE_ARG_TYPE_KEY, .key_spec_index = 1 },
    { 0 }
};

static const RedisModuleCommandInfo TS_DELETERULE_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary = "Delete a compaction rule",
    .complexity = "O(1)",
    .since = "1.0.0",
    .arity = 3,
    .key_specs = (RedisModuleCommandKeySpec *)TS_DELETERULE_KEYSPECS,
    .args = (RedisModuleCommandArg *)TS_DELETERULE_ARGS,
};

// ===============================
// TS.REVRANGE key fromTimestamp toTimestamp
//  [LATEST]
//  [FILTER_BY_TS ts...]
//  [FILTER_BY_VALUE min max]
//  [COUNT count]
//  [[ALIGN align] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
// ===============================
static const RedisModuleCommandKeySpec TS_REVRANGE_KEYSPECS[] = {
    { .flags = REDISMODULE_CMD_KEY_RO,
      .begin_search_type = REDISMODULE_KSPEC_BS_INDEX,
      .bs.index = { .pos = 1 },
      .find_keys_type = REDISMODULE_KSPEC_FK_RANGE,
      .fk.range = { .lastkey = 0, .keystep = 1, .limit = 0 } },
    { 0 }
};

static const RedisModuleCommandArg TS_REVRANGE_ARGS[] = {
    { .name = "key", .type = REDISMODULE_ARG_TYPE_KEY, .key_spec_index = 0 },
    { .name = "fromTimestamp",
      .type = REDISMODULE_ARG_TYPE_STRING }, // Actually an int, but we also allow '-'
    { .name = "toTimestamp",
      .type = REDISMODULE_ARG_TYPE_STRING }, // Actually an int, but we also allow '+'
    { .name = "latest",
      .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .token = "LATEST" },
    { .name = "filter_by_ts_block",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs = (RedisModuleCommandArg[]){ { .name = "filter_by_ts_token",
                                              .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                              .token = "FILTER_BY_TS" },
                                            { .name = "ts",
                                              .type = REDISMODULE_ARG_TYPE_INTEGER,
                                              .flags = REDISMODULE_CMD_ARG_MULTIPLE },
                                            { 0 } } },
    { .name = "filter_by_value_block",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs = (RedisModuleCommandArg[]){ { .name = "filter_by_value_token",
                                              .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                              .token = "FILTER_BY_VALUE" },
                                            { .name = "min", .type = REDISMODULE_ARG_TYPE_DOUBLE },
                                            { .name = "max", .type = REDISMODULE_ARG_TYPE_DOUBLE },
                                            { 0 } } },
    { .name = "count_block",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "count_token", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "COUNT" },
              { .name = "count", .type = REDISMODULE_ARG_TYPE_INTEGER },
              { 0 } } },
    { .name = "aggregation_block",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "align_block",
                .type = REDISMODULE_ARG_TYPE_BLOCK,
                .flags = REDISMODULE_CMD_ARG_OPTIONAL,
                .subargs = (RedisModuleCommandArg[]){ { .name = "align_token",
                                                        .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                        .token = "ALIGN" },
                                                      { .name = "align",
                                                        .type = REDISMODULE_ARG_TYPE_STRING },
                                                      { 0 } } },
              { .name = "aggregation",
                .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                .token = "AGGREGATION" },
              { .name = "aggregator",
                .type = REDISMODULE_ARG_TYPE_ONEOF,
                .subargs =
                    (RedisModuleCommandArg[]){
                        { .name = "avg", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "avg" },
                        { .name = "sum", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "sum" },
                        { .name = "min", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "min" },
                        { .name = "max", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "max" },
                        { .name = "range",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .token = "range" },
                        { .name = "count",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .token = "count" },
                        { .name = "first",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .token = "first" },
                        { .name = "last",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .token = "last" },
                        { .name = "std.p",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .token = "std.p" },
                        { .name = "std.s",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .token = "std.s" },
                        { .name = "var.p",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .token = "var.p" },
                        { .name = "var.s",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .token = "var.s" },
                        { .name = "twa", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "twa" },
                        { 0 } } },
              { .name = "bucketDuration", .type = REDISMODULE_ARG_TYPE_INTEGER },
              { .name = "buckettimestamp_block",
                .type = REDISMODULE_ARG_TYPE_BLOCK,
                .flags = REDISMODULE_CMD_ARG_OPTIONAL,
                .subargs =
                    (RedisModuleCommandArg[]){
                        { .name = "buckettimestamp_token",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .token = "BUCKETTIMESTAMP" },
                        { .name = "bt",
                          .type = REDISMODULE_ARG_TYPE_ONEOF,
                          .subargs =
                              (RedisModuleCommandArg[]){ { .name = "start",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "start" },
                                                         { .name = "start",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "-" },
                                                         { .name = "end",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "end" },
                                                         { .name = "end",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "+" },
                                                         { .name = "mid",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "mid" },
                                                         { .name = "mid",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "~" },
                                                         { 0 } } },
                        { 0 } } },
              { .name = "empty_token",
                .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                .flags = REDISMODULE_CMD_ARG_OPTIONAL,
                .token = "EMPTY" },
              { 0 } } },
    { 0 }
};

static const RedisModuleCommandInfo TS_REVRANGE_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary = "Query a range in reverse direction",
    .complexity = "O(n/m+k) where n = Number of data points, m = Chunk size (data points per "
                  "chunk), k = Number of data points that are in the requested range",
    .since = "1.4.0",
    .arity = -4,
    .key_specs = (RedisModuleCommandKeySpec *)TS_REVRANGE_KEYSPECS,
    .args = (RedisModuleCommandArg *)TS_REVRANGE_ARGS,
};

int RegisterTSCommandInfos(RedisModuleCtx *ctx) {
    RedisModuleCommand *cmd_get = RedisModule_GetCommand(ctx, "TS.GET");
    if (!cmd_get || RedisModule_SetCommandInfo(cmd_get, &TS_GET_INFO) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    RedisModuleCommand *cmd_deleterule = RedisModule_GetCommand(ctx, "TS.DELETERULE");
    if (!cmd_deleterule ||
        RedisModule_SetCommandInfo(cmd_deleterule, &TS_DELETERULE_INFO) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    RedisModuleCommand *cmd_revrange = RedisModule_GetCommand(ctx, "TS.REVRANGE");
    if (!cmd_revrange ||
        RedisModule_SetCommandInfo(cmd_revrange, &TS_REVRANGE_INFO) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
