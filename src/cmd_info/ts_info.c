#include "command_info.h"

// ===============================
// Shared Command Argument Definitions
// ===============================

// Shared policy options for DUPLICATE_POLICY and ON_DUPLICATE
static const RedisModuleCommandArg POLICY_OPTIONS[] = {
    { .name = "BLOCK", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "BLOCK" },
    { .name = "FIRST", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "FIRST" },
    { .name = "LAST", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "LAST" },
    { .name = "MIN", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "MIN" },
    { .name = "MAX", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "MAX" },
    { .name = "SUM", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "SUM" },
    { 0 }
};

// Shared aggregator options for AGGREGATION blocks
static const RedisModuleCommandArg AGGREGATOR_OPTIONS[] = {
    { .name = "avg", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "avg" },
    { .name = "sum", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "sum" },
    { .name = "min", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "min" },
    { .name = "max", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "max" },
    { .name = "range", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "range" },
    { .name = "count", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "count" },
    { .name = "first", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "first" },
    { .name = "last", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "last" },
    { .name = "std.p", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "std.p" },
    { .name = "std.s", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "std.s" },
    { .name = "var.p", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "var.p" },
    { .name = "var.s", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "var.s" },
    { .name = "twa", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "twa" },
    { 0 }
};

// Shared encoding options
static const RedisModuleCommandArg ENCODING_OPTIONS[] = {
    { .name = "COMPRESSED", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "COMPRESSED" },
    { .name = "UNCOMPRESSED", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "UNCOMPRESSED" },
    { 0 }
};

// Shared bucket timestamp options
static const RedisModuleCommandArg BUCKETTIMESTAMP_OPTIONS[] = {
    { .name = "start", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "start" },
    { .name = "start", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "-" },
    { .name = "end", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "end" },
    { .name = "end", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "+" },
    { .name = "mid", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "mid" },
    { .name = "mid", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "~" },
    { 0 }
};

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
      .subargs = (RedisModuleCommandArg[]){ { .name = "retentionPeriod",
                                              .type = REDISMODULE_ARG_TYPE_INTEGER,
                                              .token = "RETENTION" },
                                            { 0 } } },
    { .name = "ENCODING",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "ENCODING", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "ENCODING" },
              { .name = "enc",
                .type = REDISMODULE_ARG_TYPE_ONEOF,
                .subargs = (RedisModuleCommandArg *)ENCODING_OPTIONS },
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
      .subargs = (RedisModuleCommandArg[]){ { .name = "DUPLICATE_POLICY",
                                              .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                              .token = "DUPLICATE_POLICY" },
                                            { .name = "policy",
                                              .type = REDISMODULE_ARG_TYPE_ONEOF,
                                              .subargs = (RedisModuleCommandArg *)POLICY_OPTIONS },
                                            { 0 } } },
    { .name = "ON_DUPLICATE",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs = (RedisModuleCommandArg[]){ { .name = "ON_DUPLICATE",
                                              .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                              .token = "ON_DUPLICATE" },
                                            { .name = "policy_ovr",
                                              .type = REDISMODULE_ARG_TYPE_ONEOF,
                                              .subargs = (RedisModuleCommandArg *)POLICY_OPTIONS },
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
                .subargs =
                    (RedisModuleCommandArg[]){
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
// TS.INCRBY key addend
//  [TIMESTAMP timestamp]
//  [RETENTION retentionPeriod]
//  [ENCODING <COMPRESSED|UNCOMPRESSED>]
//  [CHUNK_SIZE size]
//  [DUPLICATE_POLICY policy]
//  [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
//  [LABELS [label value ...]]
// ===============================
static const RedisModuleCommandKeySpec TS_INCRBY_KEYSPECS[] = {
    { .flags = REDISMODULE_CMD_KEY_RW,
      .begin_search_type = REDISMODULE_KSPEC_BS_INDEX,
      .bs.index = { .pos = 1 },
      .find_keys_type = REDISMODULE_KSPEC_FK_RANGE,
      .fk.range = { .lastkey = 0, .keystep = 1, .limit = 0 } },
    { 0 }
};

static const RedisModuleCommandArg TS_INCRBY_ARGS[] = {
    { .name = "key", .type = REDISMODULE_ARG_TYPE_KEY, .key_spec_index = 0 },
    { .name = "addend", .type = REDISMODULE_ARG_TYPE_DOUBLE },
    { .name = "timestamp_block",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "timestamp_token",
                .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                .token = "TIMESTAMP" },
              { .name = "timestamp",
                .type = REDISMODULE_ARG_TYPE_STRING }, // unix timestamp (ms) or '*'
              { 0 } } },
    { .name = "retention_block",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs = (RedisModuleCommandArg[]){ { .name = "retention_token",
                                              .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                              .token = "RETENTION" },
                                            { .name = "retentionPeriod",
                                              .type = REDISMODULE_ARG_TYPE_INTEGER },
                                            { 0 } } },
    { .name = "encoding_block",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){ { .name = "encoding_token",
                                       .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                       .token = "ENCODING" },
                                     { .name = "enc",
                                       .type = REDISMODULE_ARG_TYPE_ONEOF,
                                       .subargs = (RedisModuleCommandArg *)ENCODING_OPTIONS },
                                     { 0 } } },
    { .name = "chunk_size_block",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){ { .name = "chunk_size_token",
                                       .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                       .token = "CHUNK_SIZE" },
                                     { .name = "size", .type = REDISMODULE_ARG_TYPE_INTEGER },
                                     { 0 } } },
    { .name = "duplicate_policy_block",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs = (RedisModuleCommandArg[]){ { .name = "duplicate_policy_token",
                                              .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                              .token = "DUPLICATE_POLICY" },
                                            { .name = "policy",
                                              .type = REDISMODULE_ARG_TYPE_ONEOF,
                                              .subargs = (RedisModuleCommandArg *)POLICY_OPTIONS },
                                            { 0 } } },
    { .name = "ignore_block",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "ignore_token",
                .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                .token = "IGNORE" },
              { .name = "ignoreMaxTimeDiff", .type = REDISMODULE_ARG_TYPE_INTEGER },
              { .name = "ignoreMaxValDiff", .type = REDISMODULE_ARG_TYPE_DOUBLE },
              { 0 } } },
    { .name = "labels_block",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "labels_token",
                .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                .token = "LABELS" },
              { .name = "label-value",
                .type = REDISMODULE_ARG_TYPE_BLOCK,
                .flags = REDISMODULE_CMD_ARG_MULTIPLE,
                .subargs =
                    (RedisModuleCommandArg[]){
                        { .name = "label", .type = REDISMODULE_ARG_TYPE_STRING },
                        { .name = "value", .type = REDISMODULE_ARG_TYPE_STRING },
                        { 0 } } },
              { 0 } } },
    { 0 }
};

static const RedisModuleCommandInfo TS_INCRBY_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary = "Increase the value of the latest sample",
    .complexity = "O(M) when M is the amount of compaction rules or O(1) with no compaction",
    .since = "1.0.0",
    .arity = -3,
    .key_specs = (RedisModuleCommandKeySpec *)TS_INCRBY_KEYSPECS,
    .args = (RedisModuleCommandArg *)TS_INCRBY_ARGS,
};

// ===============================
// TS.DECRBY key subtrahend
//  [TIMESTAMP timestamp]
//  [RETENTION retentionPeriod]
//  [ENCODING <COMPRESSED|UNCOMPRESSED>]
//  [CHUNK_SIZE size]
//  [DUPLICATE_POLICY policy]
//  [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
//  [LABELS [label value ...]]
// ===============================
static const RedisModuleCommandKeySpec TS_DECRBY_KEYSPECS[] = {
    { .flags = REDISMODULE_CMD_KEY_RW,
      .begin_search_type = REDISMODULE_KSPEC_BS_INDEX,
      .bs.index = { .pos = 1 },
      .find_keys_type = REDISMODULE_KSPEC_FK_RANGE,
      .fk.range = { .lastkey = 0, .keystep = 1, .limit = 0 } },
    { 0 }
};

static const RedisModuleCommandArg TS_DECRBY_ARGS[] = {
    { .name = "key", .type = REDISMODULE_ARG_TYPE_KEY, .key_spec_index = 0 },
    { .name = "subtrahend", .type = REDISMODULE_ARG_TYPE_DOUBLE },
    { .name = "timestamp_block",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "timestamp_token",
                .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                .token = "TIMESTAMP" },
              { .name = "timestamp",
                .type = REDISMODULE_ARG_TYPE_STRING }, // unix timestamp (ms) or '*'
              { 0 } } },
    { .name = "retention_block",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs = (RedisModuleCommandArg[]){ { .name = "retention_token",
                                              .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                              .token = "RETENTION" },
                                            { .name = "retentionPeriod",
                                              .type = REDISMODULE_ARG_TYPE_INTEGER },
                                            { 0 } } },
    { .name = "encoding_block",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "encoding_token",
                .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                .token = "ENCODING" },
              { .name = "enc",
                .type = REDISMODULE_ARG_TYPE_ONEOF,
                .subargs = (RedisModuleCommandArg[]){ { .name = "compressed",
                                                        .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                        .token = "COMPRESSED" },
                                                      { .name = "uncompressed",
                                                        .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                        .token = "UNCOMPRESSED" },
                                                      { 0 } } },
              { 0 } } },
    { .name = "chunk_size_block",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){ { .name = "chunk_size_token",
                                       .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                       .token = "CHUNK_SIZE" },
                                     { .name = "size", .type = REDISMODULE_ARG_TYPE_INTEGER },
                                     { 0 } } },
    { .name = "duplicate_policy_block",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs = (RedisModuleCommandArg[]){ { .name = "duplicate_policy_token",
                                              .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                              .token = "DUPLICATE_POLICY" },
                                            { .name = "policy",
                                              .type = REDISMODULE_ARG_TYPE_ONEOF,
                                              .subargs = (RedisModuleCommandArg *)POLICY_OPTIONS },
                                            { 0 } } },
    { .name = "ignore_block",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "ignore_token",
                .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                .token = "IGNORE" },
              { .name = "ignoreMaxTimeDiff", .type = REDISMODULE_ARG_TYPE_INTEGER },
              { .name = "ignoreMaxValDiff", .type = REDISMODULE_ARG_TYPE_DOUBLE },
              { 0 } } },
    { .name = "labels_block",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "labels_token",
                .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                .token = "LABELS" },
              { .name = "label-value",
                .type = REDISMODULE_ARG_TYPE_BLOCK,
                .flags = REDISMODULE_CMD_ARG_MULTIPLE,
                .subargs =
                    (RedisModuleCommandArg[]){
                        { .name = "label", .type = REDISMODULE_ARG_TYPE_STRING },
                        { .name = "value", .type = REDISMODULE_ARG_TYPE_STRING },
                        { 0 } } },
              { 0 } } },
    { 0 }
};

static const RedisModuleCommandInfo TS_DECRBY_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary = "Decrease the value of the latest sample",
    .complexity = "O(M) when M is the amount of compaction rules or O(1) with no compaction",
    .since = "1.0.0",
    .arity = -3,
    .key_specs = (RedisModuleCommandKeySpec *)TS_DECRBY_KEYSPECS,
    .args = (RedisModuleCommandArg *)TS_DECRBY_ARGS,
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
      .subargs = (RedisModuleCommandArg[]){ { .name = "retentionPeriod",
                                              .type = REDISMODULE_ARG_TYPE_INTEGER,
                                              .token = "RETENTION" },
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
              { .name = "DUPLICATE_POLICY",
                .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                .token = "DUPLICATE_POLICY" },
              { .name = "policy",
                .type = REDISMODULE_ARG_TYPE_ONEOF,
                .subargs =
                    (RedisModuleCommandArg[]){
                        { .name = "BLOCK",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .token = "BLOCK" },
                        { .name = "FIRST",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .token = "FIRST" },
                        { .name = "LAST",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .token = "LAST" },
                        { .name = "MIN", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "MIN" },
                        { .name = "MAX", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "MAX" },
                        { .name = "SUM", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "SUM" },
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
                .subargs =
                    (RedisModuleCommandArg[]){
                        { .name = "label", .type = REDISMODULE_ARG_TYPE_STRING },
                        { .name = "value", .type = REDISMODULE_ARG_TYPE_STRING },
                        { 0 } } },
              { 0 } } },
    { 0 }
};

static const RedisModuleCommandInfo TS_ALTER_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary =
        "Update the retention, chunk size, duplicate policy, and labels of an existing time series",
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
      .subargs = (RedisModuleCommandArg[]){ { .name = "retentionPeriod",
                                              .type = REDISMODULE_ARG_TYPE_INTEGER,
                                              .token = "RETENTION" },
                                            { 0 } } },
    { .name = "ENCODING",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "ENCODING", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "ENCODING" },
              { .name = "enc",
                .type = REDISMODULE_ARG_TYPE_ONEOF,
                .subargs = (RedisModuleCommandArg *)ENCODING_OPTIONS },
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
              { .name = "DUPLICATE_POLICY",
                .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                .token = "DUPLICATE_POLICY" },
              { .name = "policy",
                .type = REDISMODULE_ARG_TYPE_ONEOF,
                .subargs =
                    (RedisModuleCommandArg[]){
                        { .name = "BLOCK",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .token = "BLOCK" },
                        { .name = "FIRST",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .token = "FIRST" },
                        { .name = "LAST",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .token = "LAST" },
                        { .name = "MIN", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "MIN" },
                        { .name = "MAX", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "MAX" },
                        { .name = "SUM", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "SUM" },
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
                .subargs =
                    (RedisModuleCommandArg[]){
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
// TS.CREATERULE sourceKey destKey AGGREGATION aggregator bucketDuration [alignTimestamp]
// ===============================
static const RedisModuleCommandKeySpec TS_CREATERULE_KEYSPECS[] = {
    { .notes = "Source time series key",
      .flags = REDISMODULE_CMD_KEY_RO,
      .begin_search_type = REDISMODULE_KSPEC_BS_INDEX,
      .bs.index = { .pos = 1 },
      .find_keys_type = REDISMODULE_KSPEC_FK_RANGE,
      .fk.range = { .lastkey = 0, .keystep = 1, .limit = 0 } },
    { .notes = "Destination time series key",
      .flags = REDISMODULE_CMD_KEY_RW,
      .begin_search_type = REDISMODULE_KSPEC_BS_INDEX,
      .bs.index = { .pos = 2 },
      .find_keys_type = REDISMODULE_KSPEC_FK_RANGE,
      .fk.range = { .lastkey = 0, .keystep = 1, .limit = 0 } },
    { 0 }
};

static const RedisModuleCommandArg TS_CREATERULE_ARGS[] = {
    { .name = "sourceKey", .type = REDISMODULE_ARG_TYPE_KEY, .key_spec_index = 0 },
    { .name = "destKey", .type = REDISMODULE_ARG_TYPE_KEY, .key_spec_index = 1 },
    { .name = "AGGREGATION",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "AGGREGATION",
                .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                .token = "AGGREGATION" },
              { .name = "aggregator",
                .type = REDISMODULE_ARG_TYPE_ONEOF,
                .subargs = (RedisModuleCommandArg *)AGGREGATOR_OPTIONS },
              { .name = "bucketDuration", .type = REDISMODULE_ARG_TYPE_INTEGER },
              { 0 } } },
    { .name = "alignTimestamp",
      .type = REDISMODULE_ARG_TYPE_INTEGER,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL },
    { 0 }
};

static const RedisModuleCommandInfo TS_CREATERULE_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary = "Create a compaction rule",
    .complexity = "O(1)",
    .since = "1.0.0",
    .arity = -5,
    .key_specs = (RedisModuleCommandKeySpec *)TS_CREATERULE_KEYSPECS,
    .args = (RedisModuleCommandArg *)TS_CREATERULE_ARGS,
};

// ===============================
// TS.DEL key fromTimestamp toTimestamp
// ===============================
static const RedisModuleCommandKeySpec TS_DEL_KEYSPECS[] = {
    { .flags = REDISMODULE_CMD_KEY_RW,
      .begin_search_type = REDISMODULE_KSPEC_BS_INDEX,
      .bs.index = { .pos = 1 },
      .find_keys_type = REDISMODULE_KSPEC_FK_RANGE,
      .fk.range = { .lastkey = 0, .keystep = 1, .limit = 0 } },
    { 0 }
};

static const RedisModuleCommandArg TS_DEL_ARGS[] = {
    { .name = "key", .type = REDISMODULE_ARG_TYPE_KEY, .key_spec_index = 0 },
    { .name = "fromTimestamp", .type = REDISMODULE_ARG_TYPE_INTEGER },
    { .name = "toTimestamp", .type = REDISMODULE_ARG_TYPE_INTEGER },
    { 0 }
};

static const RedisModuleCommandInfo TS_DEL_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary = "Delete all samples between two timestamps for a given time series",
    .complexity = "O(N) where N is the number of data points that will be removed",
    .since = "1.6.0",
    .arity = 4,
    .key_specs = (RedisModuleCommandKeySpec *)TS_DEL_KEYSPECS,
    .args = (RedisModuleCommandArg *)TS_DEL_ARGS,
};

// ===============================
// TS.DELETERULE sourceKey destKey
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
                .subargs = (RedisModuleCommandArg *)AGGREGATOR_OPTIONS },
              { .name = "bucketDuration", .type = REDISMODULE_ARG_TYPE_INTEGER },
              { .name = "buckettimestamp_block",
                .type = REDISMODULE_ARG_TYPE_BLOCK,
                .flags = REDISMODULE_CMD_ARG_OPTIONAL,
                .subargs = (RedisModuleCommandArg[]){ { .name = "buckettimestamp_token",
                                                        .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                        .token = "BUCKETTIMESTAMP" },
                                                      { .name = "bt",
                                                        .type = REDISMODULE_ARG_TYPE_ONEOF,
                                                        .subargs = (RedisModuleCommandArg *)
                                                            BUCKETTIMESTAMP_OPTIONS },
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

// ===============================
// TS.RANGE key fromTimestamp toTimestamp
//  [LATEST]
//  [FILTER_BY_TS ts...]
//  [FILTER_BY_VALUE min max]
//  [COUNT count]
//  [[ALIGN align] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
// ===============================
static const RedisModuleCommandKeySpec TS_RANGE_KEYSPECS[] = {
    { .flags = REDISMODULE_CMD_KEY_RO,
      .begin_search_type = REDISMODULE_KSPEC_BS_INDEX,
      .bs.index = { .pos = 1 },
      .find_keys_type = REDISMODULE_KSPEC_FK_RANGE,
      .fk.range = { .lastkey = 0, .keystep = 1, .limit = 0 } },
    { 0 }
};

static const RedisModuleCommandArg TS_RANGE_ARGS[] = {
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
                .subargs = (RedisModuleCommandArg *)AGGREGATOR_OPTIONS },
              { .name = "bucketDuration", .type = REDISMODULE_ARG_TYPE_INTEGER },
              { .name = "buckettimestamp_block",
                .type = REDISMODULE_ARG_TYPE_BLOCK,
                .flags = REDISMODULE_CMD_ARG_OPTIONAL,
                .subargs = (RedisModuleCommandArg[]){ { .name = "buckettimestamp_token",
                                                        .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                        .token = "BUCKETTIMESTAMP" },
                                                      { .name = "bt",
                                                        .type = REDISMODULE_ARG_TYPE_ONEOF,
                                                        .subargs = (RedisModuleCommandArg *)
                                                            BUCKETTIMESTAMP_OPTIONS },
                                                      { 0 } } },
              { .name = "empty_token",
                .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                .flags = REDISMODULE_CMD_ARG_OPTIONAL,
                .token = "EMPTY" },
              { 0 } } },
    { 0 }
};

static const RedisModuleCommandInfo TS_RANGE_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary = "Query a range in forward direction",
    .complexity = "O(n/m+k) where n = Number of data points, m = Chunk size (data points per "
                  "chunk), k = Number of data points that are in the requested range",
    .since = "1.0.0",
    .arity = -4,
    .key_specs = (RedisModuleCommandKeySpec *)TS_RANGE_KEYSPECS,
    .args = (RedisModuleCommandArg *)TS_RANGE_ARGS,
};

// ===============================
// TS.QUERYINDEX filterExpr...
// ===============================
static const RedisModuleCommandArg TS_QUERYINDEX_ARGS[] = { { .name = "filterExpr",
                                                              .type = REDISMODULE_ARG_TYPE_STRING,
                                                              .flags =
                                                                  REDISMODULE_CMD_ARG_MULTIPLE },
                                                            { 0 } };

static const RedisModuleCommandInfo TS_QUERYINDEX_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary = "Get all time series keys matching a filter list",
    .complexity = "O(n) where n is the number of time-series that match the filters",
    .since = "1.0.0",
    .arity = -2,
    .key_specs = NULL, // No key specs - this command doesn't access specific keys
    .args = (RedisModuleCommandArg *)TS_QUERYINDEX_ARGS,
};

// ===============================
// TS.INFO key [DEBUG]
// ===============================
static const RedisModuleCommandKeySpec TS_INFO_KEYSPECS[] = {
    { .flags = REDISMODULE_CMD_KEY_RO,
      .begin_search_type = REDISMODULE_KSPEC_BS_INDEX,
      .bs.index = { .pos = 1 },
      .find_keys_type = REDISMODULE_KSPEC_FK_RANGE,
      .fk.range = { .lastkey = 0, .keystep = 1, .limit = 0 } },
    { 0 }
};

static const RedisModuleCommandArg TS_INFO_ARGS[] = {
    { .name = "key", .type = REDISMODULE_ARG_TYPE_KEY, .key_spec_index = 0 },
    { .name = "DEBUG",
      .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .token = "DEBUG" },
    { 0 }
};

static const RedisModuleCommandInfo TS_INFO_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary = "Returns information and statistics for a time series",
    .complexity = "O(1)",
    .since = "1.0.0",
    .arity = -2,
    .key_specs = (RedisModuleCommandKeySpec *)TS_INFO_KEYSPECS,
    .args = (RedisModuleCommandArg *)TS_INFO_ARGS,
};

// ===============================
// TS.MADD {key timestamp value}...
// ===============================
static const RedisModuleCommandKeySpec TS_MADD_KEYSPECS[] = {
    { .flags = REDISMODULE_CMD_KEY_RW | REDISMODULE_CMD_KEY_INSERT,
      .begin_search_type = REDISMODULE_KSPEC_BS_INDEX,
      .bs.index = { .pos = 1 },
      .find_keys_type = REDISMODULE_KSPEC_FK_RANGE,
      .fk.range = { .lastkey = -1, .keystep = 3, .limit = 0 } },
    { 0 }
};

static const RedisModuleCommandArg TS_MADD_ARGS[] = {
    { .name = "ktv",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_MULTIPLE,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "key", .type = REDISMODULE_ARG_TYPE_KEY, .key_spec_index = 0 },
              { .name = "timestamp",
                .type = REDISMODULE_ARG_TYPE_STRING }, // unix timestamp (ms) or '*'
              { .name = "value", .type = REDISMODULE_ARG_TYPE_DOUBLE },
              { 0 } } },
    { 0 }
};

static const RedisModuleCommandInfo TS_MADD_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary = "Append new samples to one or more time series",
    .complexity = "O(N*M) when N is the amount of series updated and M is the amount of compaction "
                  "rules or O(N) with no compaction",
    .since = "1.0.0",
    .arity = -4,
    .key_specs = (RedisModuleCommandKeySpec *)TS_MADD_KEYSPECS,
    .args = (RedisModuleCommandArg *)TS_MADD_ARGS,
};

// ===============================
// TS.MGET [LATEST] [WITHLABELS | SELECTED_LABELS label...] FILTER filterExpr...
// ===============================
static const RedisModuleCommandArg TS_MGET_ARGS[] = {
    { .name = "latest",
      .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .token = "LATEST" },
    { .name = "labels_block",
      .type = REDISMODULE_ARG_TYPE_ONEOF,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "withlabels",
                .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                .token = "WITHLABELS" },
              { .name = "selected_labels_block",
                .type = REDISMODULE_ARG_TYPE_BLOCK,
                .subargs = (RedisModuleCommandArg[]){ { .name = "selected_labels_token",
                                                        .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                        .token = "SELECTED_LABELS" },
                                                      { .name = "label",
                                                        .type = REDISMODULE_ARG_TYPE_STRING,
                                                        .flags = REDISMODULE_CMD_ARG_MULTIPLE },
                                                      { 0 } } },
              { 0 } } },
    { .name = "filter_token", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "FILTER" },
    { .name = "filterExpr",
      .type = REDISMODULE_ARG_TYPE_STRING,
      .flags = REDISMODULE_CMD_ARG_MULTIPLE },
    { 0 }
};

static const RedisModuleCommandInfo TS_MGET_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary = "Get the sample with the highest timestamp from each time series matching a "
               "specific filter",
    .complexity = "O(n) where n is the number of time-series that match the filters",
    .since = "1.0.0",
    .arity = -3,
    .key_specs = NULL, // No key specs - this command doesn't access specific keys, uses filters
    .args = (RedisModuleCommandArg *)TS_MGET_ARGS,
};

// ===============================
// TS.MRANGE fromTimestamp toTimestamp [options...]
// ===============================
static const RedisModuleCommandKeySpec TS_MRANGE_KEYSPECS[] = { { 0 } };

static const RedisModuleCommandArg TS_MRANGE_ARGS[] = {
    { .name = "fromTimestamp", .type = REDISMODULE_ARG_TYPE_STRING },
    { .name = "toTimestamp", .type = REDISMODULE_ARG_TYPE_STRING },
    { .name = "LATEST",
      .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .token = "LATEST" },
    { .name = "FILTER_BY_TS",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_MULTIPLE | REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs = (RedisModuleCommandArg[]){ { .name = "FILTER_BY_TS",
                                              .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                              .token = "FILTER_BY_TS" },
                                            { .name = "timestamp",
                                              .type = REDISMODULE_ARG_TYPE_STRING,
                                              .flags = REDISMODULE_CMD_ARG_MULTIPLE },
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
    { .name = "labels",
      .type = REDISMODULE_ARG_TYPE_ONEOF,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "WITHLABELS",
                .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                .token = "WITHLABELS" },
              { .name = "SELECTED_LABELS",
                .type = REDISMODULE_ARG_TYPE_BLOCK,
                .subargs = (RedisModuleCommandArg[]){ { .name = "SELECTED_LABELS",
                                                        .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                        .token = "SELECTED_LABELS" },
                                                      { .name = "label",
                                                        .type = REDISMODULE_ARG_TYPE_STRING,
                                                        .flags = REDISMODULE_CMD_ARG_MULTIPLE },
                                                      { 0 } } },
              { 0 } } },
    { .name = "COUNT",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "count", .type = REDISMODULE_ARG_TYPE_INTEGER, .token = "COUNT" },
              { 0 } } },
    { .name = "aggregation",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
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
                          .subargs = (RedisModuleCommandArg *)AGGREGATOR_OPTIONS },
                        { .name = "bucketDuration", .type = REDISMODULE_ARG_TYPE_INTEGER },
                        { .name = "BUCKETTIMESTAMP",
                          .type = REDISMODULE_ARG_TYPE_ONEOF,
                          .flags = REDISMODULE_CMD_ARG_OPTIONAL,
                          .token = "BUCKETTIMESTAMP",
                          .subargs =
                              (RedisModuleCommandArg[]){ { .name = "mid",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "mid" },
                                                         { .name = "start",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "start" },
                                                         { .name = "end",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "end" },
                                                         { .name = "+",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "+" },
                                                         { .name = "-",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "-" },
                                                         { .name = "~",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "~" },
                                                         { 0 } } },
                        { .name = "EMPTY",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .flags = REDISMODULE_CMD_ARG_OPTIONAL,
                          .token = "EMPTY" },
                        { 0 } } },
              { 0 } } },
    { .name = "FILTER",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "FILTER", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "FILTER" },
              { .name = "filterExpr",
                .type = REDISMODULE_ARG_TYPE_STRING,
                .flags = REDISMODULE_CMD_ARG_MULTIPLE },
              { 0 } } },
    { .name = "GROUPBY",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "GROUPBY", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "GROUPBY" },
              { .name = "label", .type = REDISMODULE_ARG_TYPE_STRING },
              { .name = "REDUCE", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "REDUCE" },
              { .name = "reducer",
                .type = REDISMODULE_ARG_TYPE_ONEOF,
                .subargs = (RedisModuleCommandArg *)AGGREGATOR_OPTIONS },
              { 0 } } },
    { 0 }
};

static const RedisModuleCommandInfo TS_MRANGE_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary = "Query a range across multiple time series by filters in forward direction",
    .complexity = "O(n/m+k) where n = Number of data points, m = Chunk size (data points per "
                  "chunk), k = Number of data points that are in the requested ranges",
    .since = "1.0.0",
    .arity = -4,
    .key_specs = (RedisModuleCommandKeySpec *)TS_MRANGE_KEYSPECS,
    .args = (RedisModuleCommandArg *)TS_MRANGE_ARGS,
};

// ===============================
// TS.MREVRANGE fromTimestamp toTimestamp [options...]
// ===============================
static const RedisModuleCommandKeySpec TS_MREVRANGE_KEYSPECS[] = { { 0 } };

static const RedisModuleCommandArg TS_MREVRANGE_ARGS[] = {
    { .name = "fromTimestamp", .type = REDISMODULE_ARG_TYPE_STRING },
    { .name = "toTimestamp", .type = REDISMODULE_ARG_TYPE_STRING },
    { .name = "LATEST",
      .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .token = "LATEST" },
    { .name = "FILTER_BY_TS",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_MULTIPLE | REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs = (RedisModuleCommandArg[]){ { .name = "FILTER_BY_TS",
                                              .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                              .token = "FILTER_BY_TS" },
                                            { .name = "timestamp",
                                              .type = REDISMODULE_ARG_TYPE_STRING,
                                              .flags = REDISMODULE_CMD_ARG_MULTIPLE },
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
    { .name = "labels",
      .type = REDISMODULE_ARG_TYPE_ONEOF,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "WITHLABELS",
                .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                .token = "WITHLABELS" },
              { .name = "SELECTED_LABELS",
                .type = REDISMODULE_ARG_TYPE_BLOCK,
                .subargs = (RedisModuleCommandArg[]){ { .name = "SELECTED_LABELS",
                                                        .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                        .token = "SELECTED_LABELS" },
                                                      { .name = "label",
                                                        .type = REDISMODULE_ARG_TYPE_STRING,
                                                        .flags = REDISMODULE_CMD_ARG_MULTIPLE },
                                                      { 0 } } },
              { 0 } } },
    { .name = "COUNT",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "count", .type = REDISMODULE_ARG_TYPE_INTEGER, .token = "COUNT" },
              { 0 } } },
    { .name = "aggregation",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
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
                          .subargs = (RedisModuleCommandArg *)AGGREGATOR_OPTIONS },
                        { .name = "bucketDuration", .type = REDISMODULE_ARG_TYPE_INTEGER },
                        { .name = "BUCKETTIMESTAMP",
                          .type = REDISMODULE_ARG_TYPE_ONEOF,
                          .flags = REDISMODULE_CMD_ARG_OPTIONAL,
                          .token = "BUCKETTIMESTAMP",
                          .subargs =
                              (RedisModuleCommandArg[]){ { .name = "mid",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "mid" },
                                                         { .name = "start",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "start" },
                                                         { .name = "end",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "end" },
                                                         { .name = "+",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "+" },
                                                         { .name = "-",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "-" },
                                                         { .name = "~",
                                                           .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                                                           .token = "~" },
                                                         { 0 } } },
                        { .name = "EMPTY",
                          .type = REDISMODULE_ARG_TYPE_PURE_TOKEN,
                          .flags = REDISMODULE_CMD_ARG_OPTIONAL,
                          .token = "EMPTY" },
                        { 0 } } },
              { 0 } } },
    { .name = "FILTER",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "FILTER", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "FILTER" },
              { .name = "filterExpr",
                .type = REDISMODULE_ARG_TYPE_STRING,
                .flags = REDISMODULE_CMD_ARG_MULTIPLE },
              { 0 } } },
    { .name = "GROUPBY",
      .type = REDISMODULE_ARG_TYPE_BLOCK,
      .flags = REDISMODULE_CMD_ARG_OPTIONAL,
      .subargs =
          (RedisModuleCommandArg[]){
              { .name = "GROUPBY", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "GROUPBY" },
              { .name = "label", .type = REDISMODULE_ARG_TYPE_STRING },
              { .name = "REDUCE", .type = REDISMODULE_ARG_TYPE_PURE_TOKEN, .token = "REDUCE" },
              { .name = "reducer",
                .type = REDISMODULE_ARG_TYPE_ONEOF,
                .subargs = (RedisModuleCommandArg *)AGGREGATOR_OPTIONS },
              { 0 } } },
    { 0 }
};

static const RedisModuleCommandInfo TS_MREVRANGE_INFO = {
    .version = REDISMODULE_COMMAND_INFO_VERSION,
    .summary = "Query a range across multiple time series by filters in reverse direction",
    .complexity = "O(n/m+k) where n = Number of data points, m = Chunk size (data points per "
                  "chunk), k = Number of data points that are in the requested ranges",
    .since = "1.4.0",
    .arity = -4,
    .key_specs = (RedisModuleCommandKeySpec *)TS_MREVRANGE_KEYSPECS,
    .args = (RedisModuleCommandArg *)TS_MREVRANGE_ARGS,
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

    // Register TS.CREATERULE command info
    RedisModuleCommand *cmd_createrule = RedisModule_GetCommand(ctx, "TS.CREATERULE");
    if (!cmd_createrule) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_SetCommandInfo(cmd_createrule, &TS_CREATERULE_INFO) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    // Register TS.INCRBY command info
    RedisModuleCommand *cmd_incrby = RedisModule_GetCommand(ctx, "TS.INCRBY");
    if (!cmd_incrby || RedisModule_SetCommandInfo(cmd_incrby, &TS_INCRBY_INFO) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // Register TS.DECRBY command info
    RedisModuleCommand *cmd_decrby = RedisModule_GetCommand(ctx, "TS.DECRBY");
    if (!cmd_decrby || RedisModule_SetCommandInfo(cmd_decrby, &TS_DECRBY_INFO) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // Register TS.DEL command info
    RedisModuleCommand *cmd_del = RedisModule_GetCommand(ctx, "TS.DEL");
    if (!cmd_del || RedisModule_SetCommandInfo(cmd_del, &TS_DEL_INFO) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // Register TS.DELETERULE command info
    RedisModuleCommand *cmd_deleterule = RedisModule_GetCommand(ctx, "TS.DELETERULE");
    if (!cmd_deleterule ||
        RedisModule_SetCommandInfo(cmd_deleterule, &TS_DELETERULE_INFO) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // Register TS.GET command info
    RedisModuleCommand *cmd_get = RedisModule_GetCommand(ctx, "TS.GET");
    if (!cmd_get || RedisModule_SetCommandInfo(cmd_get, &TS_GET_INFO) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // Register TS.RANGE command info
    RedisModuleCommand *cmd_range = RedisModule_GetCommand(ctx, "TS.RANGE");
    if (!cmd_range || RedisModule_SetCommandInfo(cmd_range, &TS_RANGE_INFO) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // Register TS.REVRANGE command info
    RedisModuleCommand *cmd_revrange = RedisModule_GetCommand(ctx, "TS.REVRANGE");
    if (!cmd_revrange ||
        RedisModule_SetCommandInfo(cmd_revrange, &TS_REVRANGE_INFO) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // Register TS.QUERYINDEX command info
    RedisModuleCommand *cmd_queryindex = RedisModule_GetCommand(ctx, "TS.QUERYINDEX");
    if (!cmd_queryindex ||
        RedisModule_SetCommandInfo(cmd_queryindex, &TS_QUERYINDEX_INFO) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // Register TS.INFO command info
    RedisModuleCommand *cmd_info = RedisModule_GetCommand(ctx, "TS.INFO");
    if (!cmd_info || RedisModule_SetCommandInfo(cmd_info, &TS_INFO_INFO) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // Register TS.MADD command info
    RedisModuleCommand *cmd_madd = RedisModule_GetCommand(ctx, "TS.MADD");
    if (!cmd_madd || RedisModule_SetCommandInfo(cmd_madd, &TS_MADD_INFO) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // Register TS.MGET command info
    RedisModuleCommand *cmd_mget = RedisModule_GetCommand(ctx, "TS.MGET");
    if (!cmd_mget || RedisModule_SetCommandInfo(cmd_mget, &TS_MGET_INFO) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // Register TS.MRANGE command info
    RedisModuleCommand *cmd_mrange = RedisModule_GetCommand(ctx, "TS.MRANGE");
    if (!cmd_mrange) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_SetCommandInfo(cmd_mrange, &TS_MRANGE_INFO) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    // Register TS.MREVRANGE command info
    RedisModuleCommand *cmd_mrevrange = RedisModule_GetCommand(ctx, "TS.MREVRANGE");
    if (!cmd_mrevrange) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_SetCommandInfo(cmd_mrevrange, &TS_MREVRANGE_INFO) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
