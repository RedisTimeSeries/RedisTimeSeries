#include "command_info.h"

// ===============================
// TS.REVRANGE key fromTimestamp toTimestamp [options...]
// ===============================
static const RedisModuleCommandKeySpec TS_REVRANGE_KEYSPECS[] = {
    {.notes = "",
     .flags = REDISMODULE_CMD_KEY_RO,
     .begin_search_type = REDISMODULE_KSPEC_BS_INDEX,
     .bs.index = {.pos = 1},
     .find_keys_type = REDISMODULE_KSPEC_FK_RANGE,
     .fk.range = {.lastkey = 0, .keystep = 1, .limit = 0}},
    {0}};

static const RedisModuleCommandArg TS_REVRANGE_ARGS[] = {
    {.name = "key", .type = REDISMODULE_ARG_TYPE_KEY, .key_spec_index = 0},
    {.name = "fromTimestamp", .type = REDISMODULE_ARG_TYPE_STRING},
    {.name = "toTimestamp", .type = REDISMODULE_ARG_TYPE_STRING},
    {.name = "LATEST", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .flags = REDISMODULE_CMD_ARG_OPTIONAL, .token = "LATEST"},
    {.name = "COUNT",
        .type = REDISMODULE_ARG_TYPE_BLOCK,
        .flags = REDISMODULE_CMD_ARG_OPTIONAL,
        .subargs = (RedisModuleCommandArg[]){{.name = "count",
                                            .type = REDISMODULE_ARG_TYPE_INTEGER,
                                            .token = "COUNT"},
                                            {0}}},
    {.name = "FILTER_BY_TS",
        .type = REDISMODULE_ARG_TYPE_BLOCK,
        .flags = REDISMODULE_CMD_ARG_MULTIPLE | REDISMODULE_CMD_ARG_OPTIONAL,
        .subargs = (RedisModuleCommandArg[]){{.name = "timestamp",
                                            .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                            .token = "TIMESTAMP"},
                                            {.name = "from_ts",
                                            .type = REDISMODULE_ARG_TYPE_STRING},
                                            {.name = "to_ts",
                                            .type = REDISMODULE_ARG_TYPE_STRING},
                                            {0}}},
    {.name = "FILTER_BY_VALUE",
        .type = REDISMODULE_ARG_TYPE_BLOCK,
        .flags = REDISMODULE_CMD_ARG_OPTIONAL,
        .subargs = (RedisModuleCommandArg[]){{.name = "FILTER_BY_VALUE",
                                            .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                            .token = "FILTER_BY_VALUE"},
                                            {.name = "min",
                                            .type = REDISMODULE_ARG_TYPE_DOUBLE},
                                            {.name = "max",
                                            .type = REDISMODULE_ARG_TYPE_DOUBLE},
                                            {0}}},

    {
    .name = "options",
    .type = REDISMODULE_ARG_TYPE_BLOCK,
    .flags = REDISMODULE_CMD_ARG_OPTIONAL,
    .subargs = (RedisModuleCommandArg[]){
        {.name = "EMPTY", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .flags = REDISMODULE_CMD_ARG_OPTIONAL, .token = "EMPTY"},
        {.name = "ALIGN",
        .type = REDISMODULE_ARG_TYPE_BLOCK,
        .flags = REDISMODULE_CMD_ARG_OPTIONAL,
        .subargs = (RedisModuleCommandArg[]){{.name = "alignment",
                                            .type = REDISMODULE_ARG_TYPE_STRING,
                                            .token = "ALIGN"},
                                            {0}}},
        {.name = "AGGREGATION",
        .type = REDISMODULE_ARG_TYPE_BLOCK,
        .subargs = (RedisModuleCommandArg[]){{.name = "AGGREGATION",
                                            .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                            .token = "AGGREGATION"},
                                            {.name = "aggregator",
                                            .type = REDISMODULE_ARG_TYPE_ONEOF,
                                            .subargs = (RedisModuleCommandArg[]){
                                                {.name = "avg", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "avg"},
                                                {.name = "sum", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "sum"},
                                                {.name = "min", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "min"},
                                                {.name = "max", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "max"},
                                                {.name = "range", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "range"},
                                                {.name = "count", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "count"},
                                                {.name = "first", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "first"},
                                                {.name = "last", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "last"},
                                                {.name = "std.p", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "std.p"},
                                                {.name = "std.s", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "std.s"},
                                                {.name = "var.p", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "var.p"},
                                                {.name = "var.s", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "var.s"},
                                                {.name = "twa", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "twa"},
                                                {0}}
                                            },
                                            {.name = "bucketDuration",
                                            .type = REDISMODULE_ARG_TYPE_INTEGER},
                                            {0}}},
        {.name = "BUCKETTIMESTAMP",
        .type = REDISMODULE_ARG_TYPE_ONEOF,
        .flags = REDISMODULE_CMD_ARG_OPTIONAL,
        .token = "BUCKETTIMESTAMP",
        .subargs = (RedisModuleCommandArg[]){
            {.name = "mid", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "mid"},
            {.name = "start", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "start"},
            {.name = "end", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "end"},
            {.name = "+", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "+"},
            {.name = "-", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "-"},
            {.name = "~", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "~"},
            {0}}},
        {0}}},
    {0}};

static const RedisModuleCommandInfo TS_REVRANGE_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary = "Query a range in reverse direction",
    .complexity = "O(n/m+k) where n = Number of data points, m = Chunk size (data points per chunk), k = Number of data points that are in the requested range",
    .since = "1.4.0",
    .arity = -4,
    .key_specs = (RedisModuleCommandKeySpec *)TS_REVRANGE_KEYSPECS,
    .args = (RedisModuleCommandArg *)TS_REVRANGE_ARGS,
};

int RegisterTSCommandInfos(RedisModuleCtx *ctx) {
    RedisModuleCommand *cmd_revrange = RedisModule_GetCommand(ctx, "TS.REVRANGE");
    if (!cmd_revrange) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_SetCommandInfo(cmd_revrange, &TS_REVRANGE_INFO) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}