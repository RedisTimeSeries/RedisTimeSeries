#include "command_info.h"

// ===============================
// TS.ADD key timestamp value [options...]
// ===============================
static const RedisModuleCommandKeySpec TS_ADD_KEYSPECS[] = {
    { .notes = "",
      .flags = REDISMODULE_CMD_KEY_RW | REDISMODULE_CMD_KEY_INSERT,
      .begin_search_type = REDISMODULE_KSPEC_BS_INDEX,
      .bs.index = { .pos = 1 },
      .find_keys_type = REDISMODULE_KSPEC_FK_RANGE,
      .fk.range = { .lastkey = 0, .keystep = 1, .limit = 0 } },
    { 0 }
};

static const RedisModuleCommandArg TS_ADD_ARGS[] = {
    { .name = "key", .type = REDISMODULE_ARG_TYPE_KEY, .key_spec_index = 0 },
    { .name = "timestamp", .type = REDISMODULE_ARG_TYPE_STRING },
    { .name = "value", .type = REDISMODULE_ARG_TYPE_DOUBLE },
    { .name = "RETENTION",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "retentionPeriod", .type = REDISMODULE_ARG_TYPE_INTEGER, .token = "RETENTION" },
              { 0 } } },
    { .name = "ENCODING",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "ENCODING", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "ENCODING" },
              { .name = "enc",
                .type = REDISMODULE_ARG_TYPE_ONEOF,
                .subargs =
                    (RedisModuleCommandArg[]){ { .name = "COMPRESSED",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "COMPRESSED" },
                                               { .name = "UNCOMPRESSED",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "UNCOMPRESSED" },
                                               { 0 } } },
              { 0 } } },
    { .name = "CHUNK_SIZE",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "size", .type = REDISMODULE_ARG_TYPE_INTEGER, .token = "CHUNK_SIZE" },
              { 0 } } },
    { .name = "DUPLICATE_POLICY",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "DUPLICATE_POLICY", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "DUPLICATE_POLICY" },
              { .name = "policy",
                .type = REDISMODULE_ARG_TYPE_ONEOF,
                .subargs =
                    (RedisModuleCommandArg[]){ { .name = "BLOCK",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "BLOCK" },
                                               { .name = "FIRST",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "FIRST" },
                                               { .name = "LAST",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "LAST" },
                                               { .name = "MIN",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "MIN" },
                                               { .name = "MAX",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "MAX" },
                                               { .name = "SUM",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "SUM" },
                                               { 0 } } },
              { 0 } } },
    { .name = "ON_DUPLICATE",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "ON_DUPLICATE", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "ON_DUPLICATE" },
              { .name = "policy_ovr",
                .type = REDISMODULE_ARG_TYPE_ONEOF,
                .subargs =
                    (RedisModuleCommandArg[]){ { .name = "BLOCK",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "BLOCK" },
                                               { .name = "FIRST",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "FIRST" },
                                               { .name = "LAST",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "LAST" },
                                               { .name = "MIN",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "MIN" },
                                               { .name = "MAX",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "MAX" },
                                               { .name = "SUM",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "SUM" },
                                               { 0 } } },
              { 0 } } },
    { .name = "IGNORE",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "IGNORE", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "IGNORE" },
              { .name = "ignoreMaxTimediff", .type = REDISMODULE_ARG_TYPE_INTEGER },
              { .name = "ignoreMaxValDiff", .type = REDISMODULE_ARG_TYPE_DOUBLE },
              { 0 } } },
    { .name = "LABELS",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "LABELS", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "LABELS" },
              { .name = "label_value_pairs", 
                .type = REDISMODULE_ARG_TYPE_BLOCK,
                .flags = REDISMODULE_CMD_ARG_MULTIPLE | REDISMODULE_CMD_ARG_OPTIONAL,
                .subargs = (RedisModuleCommandArg[]){
                    { .name = "label", .type = REDISMODULE_ARG_TYPE_STRING },
                    { .name = "value", .type = REDISMODULE_ARG_TYPE_STRING },
                    { 0 } } },
              { 0 } } },
    { 0 }
};

static const RedisModuleCommandInfo TS_ADD_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary = "Append a sample to a time series",
    .complexity = "O(M) where M is the number of compaction rules or O(1) with no compaction",
    .since = "1.0.0",
    .arity = -4,
    .key_specs = (RedisModuleCommandKeySpec *)TS_ADD_KEYSPECS,
    .args = (RedisModuleCommandArg *)TS_ADD_ARGS,
};

// ===============================
// TS.ALTER key [options...]
// ===============================
static const RedisModuleCommandKeySpec TS_ALTER_KEYSPECS[] = {
    { .notes = "",
      .flags = REDISMODULE_CMD_KEY_RW,
      .begin_search_type = REDISMODULE_KSPEC_BS_INDEX,
      .bs.index = { .pos = 1 },
      .find_keys_type = REDISMODULE_KSPEC_FK_RANGE,
      .fk.range = { .lastkey = 0, .keystep = 1, .limit = 0 } },
    { 0 }
};

static const RedisModuleCommandArg TS_ALTER_ARGS[] = {
    { .name = "key", .type = REDISMODULE_ARG_TYPE_KEY, .key_spec_index = 0 },
    { .name = "RETENTION",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "retentionPeriod", .type = REDISMODULE_ARG_TYPE_INTEGER, .token = "RETENTION" },
              { 0 } } },
    { .name = "CHUNK_SIZE",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "size", .type = REDISMODULE_ARG_TYPE_INTEGER, .token = "CHUNK_SIZE" },
              { 0 } } },
    { .name = "DUPLICATE_POLICY",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "DUPLICATE_POLICY", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "DUPLICATE_POLICY" },
              { .name = "policy",
                .type = REDISMODULE_ARG_TYPE_ONEOF,
                .subargs =
                    (RedisModuleCommandArg[]){ { .name = "BLOCK",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "BLOCK" },
                                               { .name = "FIRST",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "FIRST" },
                                               { .name = "LAST",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "LAST" },
                                               { .name = "MIN",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "MIN" },
                                               { .name = "MAX",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "MAX" },
                                               { .name = "SUM",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "SUM" },
                                               { 0 } } },
              { 0 } } },
    { .name = "IGNORE",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "IGNORE", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "IGNORE" },
              { .name = "ignoreMaxTimediff", .type = REDISMODULE_ARG_TYPE_INTEGER },
              { .name = "ignoreMaxValDiff", .type = REDISMODULE_ARG_TYPE_DOUBLE },
              { 0 } } },
    { .name = "LABELS",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "LABELS", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "LABELS" },
              { .name = "label_value_pairs", 
                .type = REDISMODULE_ARG_TYPE_BLOCK,
                .flags = REDISMODULE_CMD_ARG_MULTIPLE | REDISMODULE_CMD_ARG_OPTIONAL,
                .subargs = (RedisModuleCommandArg[]){
                    { .name = "label", .type = REDISMODULE_ARG_TYPE_STRING },
                    { .name = "value", .type = REDISMODULE_ARG_TYPE_STRING },
                    { 0 } } },
              { 0 } } },
    { 0 }
};

static const RedisModuleCommandInfo TS_ALTER_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary = "Update the retention, chunk size, duplicate policy, and labels of an existing time series",
    .complexity = "O(N) where N is the number of labels requested to update",
    .since = "1.0.0",
    .arity = -2,
    .key_specs = (RedisModuleCommandKeySpec *)TS_ALTER_KEYSPECS,
    .args = (RedisModuleCommandArg *)TS_ALTER_ARGS,
};

// ===============================
// TS.CREATE key [options...]
// ===============================
static const RedisModuleCommandKeySpec TS_CREATE_KEYSPECS[] = {
    { .notes = "",
      .flags = REDISMODULE_CMD_KEY_RW | REDISMODULE_CMD_KEY_INSERT,
      .begin_search_type = REDISMODULE_KSPEC_BS_INDEX,
      .bs.index = { .pos = 1 },
      .find_keys_type = REDISMODULE_KSPEC_FK_RANGE,
      .fk.range = { .lastkey = 0, .keystep = 1, .limit = 0 } },
    { 0 }
};

static const RedisModuleCommandArg TS_CREATE_ARGS[] = {
    { .name = "key", .type = REDISMODULE_ARG_TYPE_KEY, .key_spec_index = 0 },
    { .name = "RETENTION",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "retentionPeriod", .type = REDISMODULE_ARG_TYPE_INTEGER, .token = "RETENTION" },
              { 0 } } },
    { .name = "ENCODING",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "ENCODING", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "ENCODING" },
              { .name = "enc",
                .type = REDISMODULE_ARG_TYPE_ONEOF,
                .subargs =
                    (RedisModuleCommandArg[]){ { .name = "COMPRESSED",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "COMPRESSED" },
                                               { .name = "UNCOMPRESSED",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "UNCOMPRESSED" },
                                               { 0 } } },
              { 0 } } },
    { .name = "CHUNK_SIZE",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "size", .type = REDISMODULE_ARG_TYPE_INTEGER, .token = "CHUNK_SIZE" },
              { 0 } } },
    { .name = "DUPLICATE_POLICY",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "DUPLICATE_POLICY", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "DUPLICATE_POLICY" },
              { .name = "policy",
                .type = REDISMODULE_ARG_TYPE_ONEOF,
                .subargs =
                    (RedisModuleCommandArg[]){ { .name = "BLOCK",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "BLOCK" },
                                               { .name = "FIRST",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "FIRST" },
                                               { .name = "LAST",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "LAST" },
                                               { .name = "MIN",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "MIN" },
                                               { .name = "MAX",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "MAX" },
                                               { .name = "SUM",
                                                 .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                 .token = "SUM" },
                                               { 0 } } },
              { 0 } } },
    { .name = "IGNORE",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "IGNORE", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "IGNORE" },
              { .name = "ignoreMaxTimediff", .type = REDISMODULE_ARG_TYPE_INTEGER },
              { .name = "ignoreMaxValDiff", .type = REDISMODULE_ARG_TYPE_DOUBLE },
              { 0 } } },
    { .name = "LABELS",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "LABELS", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "LABELS" },
              { .name = "label_value_pairs", 
                .type = REDISMODULE_ARG_TYPE_BLOCK,
                .flags = REDISMODULE_CMD_ARG_MULTIPLE | REDISMODULE_CMD_ARG_OPTIONAL,
                .subargs = (RedisModuleCommandArg[]){
                    { .name = "label", .type = REDISMODULE_ARG_TYPE_STRING },
                    { .name = "value", .type = REDISMODULE_ARG_TYPE_STRING },
                    { 0 } } },
              { 0 } } },
    { 0 }
};

static const RedisModuleCommandInfo TS_CREATE_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary = "Create a new time series",
    .complexity = "O(1)",
    .since = "1.0.0",
    .arity = -2,
    .key_specs = (RedisModuleCommandKeySpec *)TS_CREATE_KEYSPECS,
    .args = (RedisModuleCommandArg *)TS_CREATE_ARGS,
};

// ===============================
// TS.REVRANGE key fromTimestamp toTimestamp [options...]
// ===============================
static const RedisModuleCommandKeySpec TS_REVRANGE_KEYSPECS[] = {
    { .notes = "",
      .flags = REDISMODULE_CMD_KEY_RO,
      .begin_search_type = REDISMODULE_KSPEC_BS_INDEX,
      .bs.index = { .pos = 1 },
      .find_keys_type = REDISMODULE_KSPEC_FK_RANGE,
      .fk.range = { .lastkey = 0, .keystep = 1, .limit = 0 } },
    { 0 }
};

static const RedisModuleCommandArg TS_REVRANGE_ARGS[] = {
    { .name = "key", .type = REDISMODULE_ARG_TYPE_KEY, .key_spec_index = 0 },
    { .name = "fromTimestamp", .type = REDISMODULE_ARG_TYPE_STRING },
    { .name = "toTimestamp", .type = REDISMODULE_ARG_TYPE_STRING },
    { .name = "LATEST",
      .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .token = "LATEST" },
    { .name = "COUNT",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "count", .type = REDISMODULE_ARG_TYPE_INTEGER, .token = "COUNT" },
              { 0 } } },
    { .name = "FILTER_BY_TS",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_MULTIPLE | REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){ { .name = "timestamp",
                                       .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                       .token = "TIMESTAMP" },
                                     { .name = "from_ts", .type = REDISMODULE_ARG_TYPE_STRING },
                                     { .name = "to_ts", .type = REDISMODULE_ARG_TYPE_STRING },
                                     { 0 } } },
    { .name = "FILTER_BY_VALUE",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs = (RedisModuleCommandArg[]){ { .name = "FILTER_BY_VALUE",
                                              .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                              .token = "FILTER_BY_VALUE" },
                                            { .name = "min", .type = REDISMODULE_ARG_TYPE_DOUBLE },
                                            { .name = "max", .type = REDISMODULE_ARG_TYPE_DOUBLE },
                                            { 0 } } },

    { .name = "options",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "EMPTY",
                .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                .flags = REDISMODULE_CMD_ARG_OPTIONAL,
                .token = "EMPTY" },
              { .name = "ALIGN",
                .type = REDISMODULE_ARG_TYPE_BLOCK,
                .flags = REDISMODULE_CMD_ARG_OPTIONAL,
                .subargs = (RedisModuleCommandArg[]){ { .name = "alignment",
                                                        .type = REDISMODULE_ARG_TYPE_STRING,
                                                        .token = "ALIGN" },
                                                      { 0 } } },
              { .name = "AGGREGATION",
                .type = REDISMODULE_ARG_TYPE_BLOCK,
                .subargs =
                    (RedisModuleCommandArg[]){
                        { .name = "AGGREGATION",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .token = "AGGREGATION" },
                        { .name = "aggregator",
                          .type = REDISMODULE_ARG_TYPE_ONEOF,
                          .subargs =
                              (RedisModuleCommandArg[]){ { .name = "avg",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "avg" },
                                                         { .name = "sum",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "sum" },
                                                         { .name = "min",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "min" },
                                                         { .name = "max",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "max" },
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
                                                         { .name = "twa",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "twa" },
                                                         { 0 } } },
                        { .name = "bucketDuration", .type = REDISMODULE_ARG_TYPE_INTEGER },
                        { 0 } } },
              { .name = "BUCKETTIMESTAMP",
                .type = REDISMODULE_ARG_TYPE_ONEOF,
                .flags = REDISMODULE_CMD_ARG_OPTIONAL,
                .token = "BUCKETTIMESTAMP",
                .subargs =
                    (RedisModuleCommandArg[]){
                        { .name = "mid", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "mid" },
                        { .name = "start",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .token = "start" },
                        { .name = "end", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "end" },
                        { .name = "+", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "+" },
                        { .name = "-", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "-" },
                        { .name = "~", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "~" },
                        { 0 } } },
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
    // Register TS.ADD command info
    RedisModuleCommand *cmd_add = RedisModule_GetCommand(ctx, "TS.ADD");
    if (!cmd_add) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_SetCommandInfo(cmd_add, &TS_ADD_INFO) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    // Register TS.ALTER command info
    RedisModuleCommand *cmd_alter = RedisModule_GetCommand(ctx, "TS.ALTER");
    if (!cmd_alter) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_SetCommandInfo(cmd_alter, &TS_ALTER_INFO) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    // Register TS.CREATE command info
    RedisModuleCommand *cmd_create = RedisModule_GetCommand(ctx, "TS.CREATE");
    if (!cmd_create) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_SetCommandInfo(cmd_create, &TS_CREATE_INFO) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    // Register TS.REVRANGE command info
    RedisModuleCommand *cmd_revrange = RedisModule_GetCommand(ctx, "TS.REVRANGE");
    if (!cmd_revrange) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_SetCommandInfo(cmd_revrange, &TS_REVRANGE_INFO) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}