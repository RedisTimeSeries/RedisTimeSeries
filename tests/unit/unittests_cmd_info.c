/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "minunit.h"
#include "redismodule.h"

#include <string.h>

// Include the command info definitions directly
#include "cmd_info/ts_info.c"

// Test that TS.ADD command info is properly structured
MU_TEST(test_ts_add_command_info_structure) {
    // Test that TS_ADD_INFO has correct basic properties
    mu_check(TS_ADD_INFO.version == REDISMODULE_COMMAND_INFO_VERSION);
    mu_check(TS_ADD_INFO.arity == -4); // At least 4 arguments: TS.ADD key timestamp value
    mu_check(TS_ADD_INFO.since != NULL);
    mu_check(strcmp(TS_ADD_INFO.since, "1.0.0") == 0);
    mu_check(TS_ADD_INFO.summary != NULL);
    mu_check(strstr(TS_ADD_INFO.summary, "Append a sample") != NULL);
    mu_check(TS_ADD_INFO.complexity != NULL);
    mu_check(strstr(TS_ADD_INFO.complexity, "O(M)") != NULL);
}

// Test that TS.ALTER command info is properly structured  
MU_TEST(test_ts_alter_command_info_structure) {
    // Test that TS_ALTER_INFO has correct basic properties
    mu_check(TS_ALTER_INFO.version == REDISMODULE_COMMAND_INFO_VERSION);
    mu_check(TS_ALTER_INFO.arity == -2); // At least 2 arguments: TS.ALTER key
    mu_check(TS_ALTER_INFO.since != NULL);
    mu_check(strcmp(TS_ALTER_INFO.since, "1.0.0") == 0);
    mu_check(TS_ALTER_INFO.summary != NULL);
    mu_check(strstr(TS_ALTER_INFO.summary, "Update the retention") != NULL);
    mu_check(TS_ALTER_INFO.complexity != NULL);
    mu_check(strstr(TS_ALTER_INFO.complexity, "O(N)") != NULL);
}

// Test that TS.CREATE command info is properly structured  
MU_TEST(test_ts_create_command_info_structure) {
    // Test that TS_CREATE_INFO has correct basic properties
    mu_check(TS_CREATE_INFO.version == REDISMODULE_COMMAND_INFO_VERSION);
    mu_check(TS_CREATE_INFO.arity == -2); // At least 2 arguments: TS.CREATE key
    mu_check(TS_CREATE_INFO.since != NULL);
    mu_check(strcmp(TS_CREATE_INFO.since, "1.0.0") == 0);
    mu_check(TS_CREATE_INFO.summary != NULL);
    mu_check(strstr(TS_CREATE_INFO.summary, "Create a new time series") != NULL);
    mu_check(TS_CREATE_INFO.complexity != NULL);
    mu_check(strstr(TS_CREATE_INFO.complexity, "O(1)") != NULL);
}

// Test that TS.CREATERULE command info is properly structured  
MU_TEST(test_ts_createrule_command_info_structure) {
    // Test that TS_CREATERULE_INFO has correct basic properties
    mu_check(TS_CREATERULE_INFO.version == REDISMODULE_COMMAND_INFO_VERSION);
    mu_check(TS_CREATERULE_INFO.arity == -5); // At least 5 arguments: TS.CREATERULE sourceKey destKey AGGREGATION aggregator bucketDuration
    mu_check(TS_CREATERULE_INFO.since != NULL);
    mu_check(strcmp(TS_CREATERULE_INFO.since, "1.0.0") == 0);
    mu_check(TS_CREATERULE_INFO.summary != NULL);
    mu_check(strstr(TS_CREATERULE_INFO.summary, "Create a compaction rule") != NULL);
    mu_check(TS_CREATERULE_INFO.complexity != NULL);
    mu_check(strstr(TS_CREATERULE_INFO.complexity, "O(1)") != NULL);
}

// Test that TS.INCRBY command info is properly structured  
MU_TEST(test_ts_incrby_command_info_structure) {
    // Test that TS_INCRBY_INFO has correct basic properties
    mu_check(TS_INCRBY_INFO.version == REDISMODULE_COMMAND_INFO_VERSION);
    mu_check(TS_INCRBY_INFO.arity == -3); // At least 3 arguments: TS.INCRBY key addend
    mu_check(TS_INCRBY_INFO.since != NULL);
    mu_check(strcmp(TS_INCRBY_INFO.since, "1.0.0") == 0);
    mu_check(TS_INCRBY_INFO.summary != NULL);
    mu_check(strstr(TS_INCRBY_INFO.summary, "Increase the value") != NULL);
    mu_check(TS_INCRBY_INFO.complexity != NULL);
    mu_check(strstr(TS_INCRBY_INFO.complexity, "O(M)") != NULL);
}

// Test that TS.DECRBY command info is properly structured  
MU_TEST(test_ts_decrby_command_info_structure) {
    // Test that TS_DECRBY_INFO has correct basic properties
    mu_check(TS_DECRBY_INFO.version == REDISMODULE_COMMAND_INFO_VERSION);
    mu_check(TS_DECRBY_INFO.arity == -3); // At least 3 arguments: TS.DECRBY key subtrahend
    mu_check(TS_DECRBY_INFO.since != NULL);
    mu_check(strcmp(TS_DECRBY_INFO.since, "1.0.0") == 0);
    mu_check(TS_DECRBY_INFO.summary != NULL);
    mu_check(strstr(TS_DECRBY_INFO.summary, "Decrease the value") != NULL);
    mu_check(TS_DECRBY_INFO.complexity != NULL);
    mu_check(strstr(TS_DECRBY_INFO.complexity, "O(M)") != NULL);
}

// Test that key specifications are correct
MU_TEST(test_command_key_specs) {
    // TS.ADD should have RW and INSERT flags
    mu_check(TS_ADD_KEYSPECS[0].flags & REDISMODULE_CMD_KEY_RW);
    mu_check(TS_ADD_KEYSPECS[0].flags & REDISMODULE_CMD_KEY_INSERT);
    mu_check(TS_ADD_KEYSPECS[0].begin_search_type == REDISMODULE_KSPEC_BS_INDEX);
    mu_check(TS_ADD_KEYSPECS[0].bs.index.pos == 1);
    
    // TS.ALTER should have RW flag but not INSERT (modifies existing)
    mu_check(TS_ALTER_KEYSPECS[0].flags & REDISMODULE_CMD_KEY_RW);
    mu_check(!(TS_ALTER_KEYSPECS[0].flags & REDISMODULE_CMD_KEY_INSERT));
    mu_check(TS_ALTER_KEYSPECS[0].begin_search_type == REDISMODULE_KSPEC_BS_INDEX);
    mu_check(TS_ALTER_KEYSPECS[0].bs.index.pos == 1);
    
    // TS.CREATE should have RW and INSERT flags (creates new time series)
    mu_check(TS_CREATE_KEYSPECS[0].flags & REDISMODULE_CMD_KEY_RW);
    mu_check(TS_CREATE_KEYSPECS[0].flags & REDISMODULE_CMD_KEY_INSERT);
    mu_check(TS_CREATE_KEYSPECS[0].begin_search_type == REDISMODULE_KSPEC_BS_INDEX);
    mu_check(TS_CREATE_KEYSPECS[0].bs.index.pos == 1);
    
    // TS.CREATERULE should have two keys: source (RO) and dest (RW)
    mu_check(TS_CREATERULE_KEYSPECS[0].flags & REDISMODULE_CMD_KEY_RO); // source key
    mu_check(!(TS_CREATERULE_KEYSPECS[0].flags & REDISMODULE_CMD_KEY_INSERT));
    mu_check(TS_CREATERULE_KEYSPECS[0].begin_search_type == REDISMODULE_KSPEC_BS_INDEX);
    mu_check(TS_CREATERULE_KEYSPECS[0].bs.index.pos == 1);
    
    mu_check(TS_CREATERULE_KEYSPECS[1].flags & REDISMODULE_CMD_KEY_RW); // dest key
    mu_check(!(TS_CREATERULE_KEYSPECS[1].flags & REDISMODULE_CMD_KEY_INSERT));
    mu_check(TS_CREATERULE_KEYSPECS[1].begin_search_type == REDISMODULE_KSPEC_BS_INDEX);
    mu_check(TS_CREATERULE_KEYSPECS[1].bs.index.pos == 2);
    
    // TS.INCRBY should have RW flag (can modify existing or create new)
    mu_check(TS_INCRBY_KEYSPECS[0].flags & REDISMODULE_CMD_KEY_RW);
    mu_check(TS_INCRBY_KEYSPECS[0].begin_search_type == REDISMODULE_KSPEC_BS_INDEX);
    mu_check(TS_INCRBY_KEYSPECS[0].bs.index.pos == 1);
    
    // TS.DECRBY should have RW flag (can modify existing or create new)
    mu_check(TS_DECRBY_KEYSPECS[0].flags & REDISMODULE_CMD_KEY_RW);
    mu_check(TS_DECRBY_KEYSPECS[0].begin_search_type == REDISMODULE_KSPEC_BS_INDEX);
    mu_check(TS_DECRBY_KEYSPECS[0].bs.index.pos == 1);
    
    // TS.RANGE should have RO flag (read-only operation)
    mu_check(TS_RANGE_KEYSPECS[0].flags & REDISMODULE_CMD_KEY_RO);
    mu_check(TS_RANGE_KEYSPECS[0].begin_search_type == REDISMODULE_KSPEC_BS_INDEX);
    mu_check(TS_RANGE_KEYSPECS[0].bs.index.pos == 1);
    
    // TS.REVRANGE should have RO flag (read-only operation)
    mu_check(TS_REVRANGE_KEYSPECS[0].flags & REDISMODULE_CMD_KEY_RO);
    mu_check(TS_REVRANGE_KEYSPECS[0].begin_search_type == REDISMODULE_KSPEC_BS_INDEX);
    mu_check(TS_REVRANGE_KEYSPECS[0].bs.index.pos == 1);
}

// Test that arguments are properly defined
MU_TEST(test_command_arguments) {
    // Test TS.ADD has required arguments
    mu_check(TS_ADD_ARGS[0].type == REDISMODULE_ARG_TYPE_KEY); // key
    mu_check(TS_ADD_ARGS[1].type == REDISMODULE_ARG_TYPE_STRING); // timestamp  
    mu_check(TS_ADD_ARGS[2].type == REDISMODULE_ARG_TYPE_DOUBLE); // value
    
    // Test TS.ALTER has key argument
    mu_check(TS_ALTER_ARGS[0].type == REDISMODULE_ARG_TYPE_KEY); // key
    
    // Test TS.CREATE has key argument
    mu_check(TS_CREATE_ARGS[0].type == REDISMODULE_ARG_TYPE_KEY); // key
    
    // Test TS.INCRBY has required arguments
    mu_check(TS_INCRBY_ARGS[0].type == REDISMODULE_ARG_TYPE_KEY); // key
    mu_check(TS_INCRBY_ARGS[1].type == REDISMODULE_ARG_TYPE_DOUBLE); // addend
    
    // Test TS.DECRBY has required arguments
    mu_check(TS_DECRBY_ARGS[0].type == REDISMODULE_ARG_TYPE_KEY); // key
    mu_check(TS_DECRBY_ARGS[1].type == REDISMODULE_ARG_TYPE_DOUBLE); // subtrahend
    
    // Test TS.RANGE has required arguments
    mu_check(TS_RANGE_ARGS[0].type == REDISMODULE_ARG_TYPE_KEY); // key
    mu_check(TS_RANGE_ARGS[1].type == REDISMODULE_ARG_TYPE_STRING); // fromTimestamp
    mu_check(TS_RANGE_ARGS[2].type == REDISMODULE_ARG_TYPE_STRING); // toTimestamp
    
    // Test TS.REVRANGE has required arguments
    mu_check(TS_REVRANGE_ARGS[0].type == REDISMODULE_ARG_TYPE_KEY); // key
    mu_check(TS_REVRANGE_ARGS[1].type == REDISMODULE_ARG_TYPE_STRING); // fromTimestamp
    mu_check(TS_REVRANGE_ARGS[2].type == REDISMODULE_ARG_TYPE_STRING); // toTimestamp
    
    // Test TS.QUERYINDEX has required arguments
    mu_check(TS_QUERYINDEX_ARGS[0].type == REDISMODULE_ARG_TYPE_STRING); // filterExpr
    mu_check(TS_QUERYINDEX_ARGS[0].flags & REDISMODULE_CMD_ARG_MULTIPLE); // multiple filter expressions allowed
    
    // Test that optional arguments are marked as optional
    int found_optional_retention = 0;
    for (int i = 0; TS_ADD_ARGS[i].name != NULL; i++) {
        if (strcmp(TS_ADD_ARGS[i].name, "RETENTION") == 0) {
            mu_check(TS_ADD_ARGS[i].flags & REDISMODULE_CMD_ARG_OPTIONAL);
            found_optional_retention = 1;
            break;
        }
    }
    mu_check(found_optional_retention);
    
    // Test that TS.CREATE also has optional RETENTION
    int found_create_retention = 0;
    for (int i = 0; TS_CREATE_ARGS[i].name != NULL; i++) {
        if (strcmp(TS_CREATE_ARGS[i].name, "RETENTION") == 0) {
            mu_check(TS_CREATE_ARGS[i].flags & REDISMODULE_CMD_ARG_OPTIONAL);
            found_create_retention = 1;
            break;
        }
    }
    mu_check(found_create_retention);
    
    // Test that TS.INCRBY also has optional TIMESTAMP
    int found_incrby_timestamp = 0;
    for (int i = 0; TS_INCRBY_ARGS[i].name != NULL; i++) {
        if (strcmp(TS_INCRBY_ARGS[i].name, "timestamp_block") == 0) {
            mu_check(TS_INCRBY_ARGS[i].flags & REDISMODULE_CMD_ARG_OPTIONAL);
            found_incrby_timestamp = 1;
            break;
        }
    }
    mu_check(found_incrby_timestamp);
    
    // Test that TS.DECRBY also has optional TIMESTAMP
    int found_decrby_timestamp = 0;
    for (int i = 0; TS_DECRBY_ARGS[i].name != NULL; i++) {
        if (strcmp(TS_DECRBY_ARGS[i].name, "timestamp_block") == 0) {
            mu_check(TS_DECRBY_ARGS[i].flags & REDISMODULE_CMD_ARG_OPTIONAL);
            found_decrby_timestamp = 1;
            break;
        }
    }
    mu_check(found_decrby_timestamp);
}

// Test that duplicate policy options are correctly defined
MU_TEST(test_duplicate_policy_options) {
    // Find DUPLICATE_POLICY argument in TS.ADD
    int found_duplicate_policy = 0;
    for (int i = 0; TS_ADD_ARGS[i].name != NULL; i++) {
        if (strcmp(TS_ADD_ARGS[i].name, "DUPLICATE_POLICY") == 0) {
            found_duplicate_policy = 1;
            mu_check(TS_ADD_ARGS[i].flags & REDISMODULE_CMD_ARG_OPTIONAL);
            mu_check(TS_ADD_ARGS[i].type == REDISMODULE_ARG_TYPE_BLOCK);
            
            // Check that policy options are available
            const RedisModuleCommandArg *subargs = TS_ADD_ARGS[i].subargs;
            mu_check(subargs != NULL);
            
            // Find the policy oneof argument
            int found_policy_oneof = 0;
            for (int j = 0; subargs[j].name != NULL; j++) {
                if (strcmp(subargs[j].name, "policy") == 0) {
                    found_policy_oneof = 1;
                    mu_check(subargs[j].type == REDISMODULE_ARG_TYPE_ONEOF);
                    
                    // Check that BLOCK, FIRST, LAST, MIN, MAX, SUM options exist
                    const RedisModuleCommandArg *policy_options = subargs[j].subargs;
                    mu_check(policy_options != NULL);
                    
                    int found_block = 0, found_first = 0, found_last = 0;
                    int found_min = 0, found_max = 0, found_sum = 0;
                    
                    for (int k = 0; policy_options[k].name != NULL; k++) {
                        if (strcmp(policy_options[k].name, "BLOCK") == 0) found_block = 1;
                        if (strcmp(policy_options[k].name, "FIRST") == 0) found_first = 1;
                        if (strcmp(policy_options[k].name, "LAST") == 0) found_last = 1;
                        if (strcmp(policy_options[k].name, "MIN") == 0) found_min = 1;
                        if (strcmp(policy_options[k].name, "MAX") == 0) found_max = 1;
                        if (strcmp(policy_options[k].name, "SUM") == 0) found_sum = 1;
                    }
                    
                    mu_check(found_block && found_first && found_last && 
                             found_min && found_max && found_sum);
                    break;
                }
            }
            mu_check(found_policy_oneof);
            break;
        }
    }
    mu_check(found_duplicate_policy);
}

// Test that aggregation options are correctly defined for TS.CREATERULE
MU_TEST(test_createrule_aggregation_options) {
    // Find AGGREGATION argument in TS.CREATERULE
    int found_aggregation = 0;
    for (int i = 0; TS_CREATERULE_ARGS[i].name != NULL; i++) {
        if (strcmp(TS_CREATERULE_ARGS[i].name, "AGGREGATION") == 0) {
            found_aggregation = 1;
            mu_check(TS_CREATERULE_ARGS[i].type == REDISMODULE_ARG_TYPE_BLOCK);
            
            // Check that aggregation options are available
            const RedisModuleCommandArg *subargs = TS_CREATERULE_ARGS[i].subargs;
            mu_check(subargs != NULL);
            
            // Find the aggregator oneof argument
            int found_aggregator_oneof = 0;
            for (int j = 0; subargs[j].name != NULL; j++) {
                if (strcmp(subargs[j].name, "aggregator") == 0) {
                    found_aggregator_oneof = 1;
                    mu_check(subargs[j].type == REDISMODULE_ARG_TYPE_ONEOF);
                    
                    // Check that all aggregation types exist
                    const RedisModuleCommandArg *agg_options = subargs[j].subargs;
                    mu_check(agg_options != NULL);
                    
                    int found_avg = 0, found_sum = 0, found_min = 0, found_max = 0;
                    int found_range = 0, found_count = 0, found_first = 0, found_last = 0;
                    int found_std_p = 0, found_std_s = 0, found_var_p = 0, found_var_s = 0, found_twa = 0;
                    
                    for (int k = 0; agg_options[k].name != NULL; k++) {
                        if (strcmp(agg_options[k].name, "avg") == 0) found_avg = 1;
                        if (strcmp(agg_options[k].name, "sum") == 0) found_sum = 1;
                        if (strcmp(agg_options[k].name, "min") == 0) found_min = 1;
                        if (strcmp(agg_options[k].name, "max") == 0) found_max = 1;
                        if (strcmp(agg_options[k].name, "range") == 0) found_range = 1;
                        if (strcmp(agg_options[k].name, "count") == 0) found_count = 1;
                        if (strcmp(agg_options[k].name, "first") == 0) found_first = 1;
                        if (strcmp(agg_options[k].name, "last") == 0) found_last = 1;
                        if (strcmp(agg_options[k].name, "std.p") == 0) found_std_p = 1;
                        if (strcmp(agg_options[k].name, "std.s") == 0) found_std_s = 1;
                        if (strcmp(agg_options[k].name, "var.p") == 0) found_var_p = 1;
                        if (strcmp(agg_options[k].name, "var.s") == 0) found_var_s = 1;
                        if (strcmp(agg_options[k].name, "twa") == 0) found_twa = 1;
                    }
                    
                    // Verify all 13 aggregation types are present
                    mu_check(found_avg && found_sum && found_min && found_max && found_range && 
                             found_count && found_first && found_last && found_std_p && found_std_s && 
                             found_var_p && found_var_s && found_twa);
                    break;
                }
            }
            mu_check(found_aggregator_oneof);
            break;
        }
    }
    mu_check(found_aggregation);
}

// Test that RegisterTSCommandInfos function exists and has correct signature
MU_TEST(test_register_function_exists) {
    // Just verify the function exists by checking it's not NULL
    // We can't easily test the actual registration without a full Redis context
    mu_check(RegisterTSCommandInfos != NULL);
}

// Test that LABELS argument supports multiple label-value pairs
MU_TEST(test_labels_argument_structure) {
    // Find LABELS argument in TS.ADD
    int found_labels = 0;
    for (int i = 0; TS_ADD_ARGS[i].name != NULL; i++) {
        if (strcmp(TS_ADD_ARGS[i].name, "LABELS") == 0) {
            found_labels = 1;
            mu_check(TS_ADD_ARGS[i].flags & REDISMODULE_CMD_ARG_OPTIONAL);
            mu_check(TS_ADD_ARGS[i].type == REDISMODULE_ARG_TYPE_BLOCK);
            
            const RedisModuleCommandArg *subargs = TS_ADD_ARGS[i].subargs;
            mu_check(subargs != NULL);
            
            // Should have LABELS token and label_value_pairs
            int found_token = 0, found_pairs = 0;
            for (int j = 0; subargs[j].name != NULL; j++) {
                if (strcmp(subargs[j].name, "LABELS") == 0) {
                    found_token = 1;
                    mu_check(subargs[j].type == REDISMODULE_ARG_TYPE_PURE_TOKEN);
                }
                if (strcmp(subargs[j].name, "label_value_pairs") == 0) {
                    found_pairs = 1;
                    mu_check(subargs[j].flags & REDISMODULE_CMD_ARG_MULTIPLE);
                    mu_check(subargs[j].flags & REDISMODULE_CMD_ARG_OPTIONAL);
                }
            }
            mu_check(found_token && found_pairs);
            break;
        }
    }
    mu_check(found_labels);
}

// Test that TS.MRANGE command info is properly structured
MU_TEST(test_ts_mrange_command_info_structure) {
    // Test that TS_MRANGE_INFO has correct basic properties
    mu_check(TS_MRANGE_INFO.version == REDISMODULE_COMMAND_INFO_VERSION);
    mu_check(TS_MRANGE_INFO.arity == -4); // At least 4 arguments: TS.MRANGE fromTimestamp toTimestamp FILTER filterExpr
    mu_check(TS_MRANGE_INFO.since != NULL);
    mu_check(strcmp(TS_MRANGE_INFO.since, "1.0.0") == 0);
    mu_check(TS_MRANGE_INFO.summary != NULL);
    mu_check(strstr(TS_MRANGE_INFO.summary, "Query a range across multiple time series") != NULL);
    mu_check(strstr(TS_MRANGE_INFO.summary, "forward direction") != NULL);
    mu_check(TS_MRANGE_INFO.complexity != NULL);
    mu_check(strstr(TS_MRANGE_INFO.complexity, "O(n/m+k)") != NULL);
}

// Test that TS.RANGE command info is properly structured
MU_TEST(test_ts_range_command_info_structure) {
    // Test that TS_RANGE_INFO has correct basic properties
    mu_check(TS_RANGE_INFO.version == REDISMODULE_COMMAND_INFO_VERSION);
    mu_check(TS_RANGE_INFO.arity == -4); // At least 4 arguments: TS.RANGE key fromTimestamp toTimestamp
    mu_check(TS_RANGE_INFO.since != NULL);
    mu_check(strcmp(TS_RANGE_INFO.since, "1.0.0") == 0);
    mu_check(TS_RANGE_INFO.summary != NULL);
    mu_check(strstr(TS_RANGE_INFO.summary, "Query a range") != NULL);
    mu_check(strstr(TS_RANGE_INFO.summary, "forward direction") != NULL);
    mu_check(TS_RANGE_INFO.complexity != NULL);
    mu_check(strstr(TS_RANGE_INFO.complexity, "O(n/m+k)") != NULL);
}

// Test that TS.REVRANGE command info is properly structured
MU_TEST(test_ts_revrange_command_info_structure) {
    // Test that TS_REVRANGE_INFO has correct basic properties
    mu_check(TS_REVRANGE_INFO.version == REDISMODULE_COMMAND_INFO_VERSION);
    mu_check(TS_REVRANGE_INFO.arity == -4); // At least 4 arguments: TS.REVRANGE key fromTimestamp toTimestamp
    mu_check(TS_REVRANGE_INFO.since != NULL);
    mu_check(strcmp(TS_REVRANGE_INFO.since, "1.4.0") == 0);
    mu_check(TS_REVRANGE_INFO.summary != NULL);
    mu_check(strstr(TS_REVRANGE_INFO.summary, "Query a range") != NULL);
    mu_check(strstr(TS_REVRANGE_INFO.summary, "reverse direction") != NULL);
    mu_check(TS_REVRANGE_INFO.complexity != NULL);
    mu_check(strstr(TS_REVRANGE_INFO.complexity, "O(n/m+k)") != NULL);
}

// Test that TS.QUERYINDEX command info is properly structured
MU_TEST(test_ts_queryindex_command_info_structure) {
    // Test that TS_QUERYINDEX_INFO has correct basic properties
    mu_check(TS_QUERYINDEX_INFO.version == REDISMODULE_COMMAND_INFO_VERSION);
    mu_check(TS_QUERYINDEX_INFO.arity == -2); // At least 2 arguments: TS.QUERYINDEX filterExpr
    mu_check(TS_QUERYINDEX_INFO.since != NULL);
    mu_check(strcmp(TS_QUERYINDEX_INFO.since, "1.0.0") == 0);
    mu_check(TS_QUERYINDEX_INFO.summary != NULL);
    mu_check(strstr(TS_QUERYINDEX_INFO.summary, "Get all time series keys") != NULL);
    mu_check(strstr(TS_QUERYINDEX_INFO.summary, "matching a filter list") != NULL);
    mu_check(TS_QUERYINDEX_INFO.complexity != NULL);
    mu_check(strstr(TS_QUERYINDEX_INFO.complexity, "O(n)") != NULL);
    mu_check(strstr(TS_QUERYINDEX_INFO.complexity, "time-series that match") != NULL);
}

// Test that TS.MREVRANGE command info is properly structured
MU_TEST(test_ts_mrevrange_command_info_structure) {
    // Test that TS_MREVRANGE_INFO has correct basic properties
    mu_check(TS_MREVRANGE_INFO.version == REDISMODULE_COMMAND_INFO_VERSION);
    mu_check(TS_MREVRANGE_INFO.arity == -4); // At least 4 arguments: TS.MREVRANGE fromTimestamp toTimestamp FILTER filterExpr
    mu_check(TS_MREVRANGE_INFO.since != NULL);
    mu_check(strcmp(TS_MREVRANGE_INFO.since, "1.4.0") == 0);
    mu_check(TS_MREVRANGE_INFO.summary != NULL);
    mu_check(strstr(TS_MREVRANGE_INFO.summary, "Query a range across multiple time series") != NULL);
    mu_check(strstr(TS_MREVRANGE_INFO.summary, "reverse direction") != NULL);
    mu_check(TS_MREVRANGE_INFO.complexity != NULL);
    mu_check(strstr(TS_MREVRANGE_INFO.complexity, "O(n/m+k)") != NULL);
}

// Test that TS.MRANGE has required arguments
MU_TEST(test_mrange_command_arguments) {
    // Test TS.MRANGE has required arguments
    mu_check(TS_MRANGE_ARGS[0].type == REDISMODULE_ARG_TYPE_STRING); // fromTimestamp
    mu_check(strcmp(TS_MRANGE_ARGS[0].name, "fromTimestamp") == 0);
    mu_check(TS_MRANGE_ARGS[1].type == REDISMODULE_ARG_TYPE_STRING); // toTimestamp
    mu_check(strcmp(TS_MRANGE_ARGS[1].name, "toTimestamp") == 0);
    
    // Find FILTER argument which is required
    int found_filter = 0;
    for (int i = 0; TS_MRANGE_ARGS[i].name != NULL; i++) {
        if (strcmp(TS_MRANGE_ARGS[i].name, "FILTER") == 0) {
            found_filter = 1;
            mu_check(TS_MRANGE_ARGS[i].type == REDISMODULE_ARG_TYPE_BLOCK);
            mu_check(!(TS_MRANGE_ARGS[i].flags & REDISMODULE_CMD_ARG_OPTIONAL)); // FILTER is required
            
            // Check that filterExpr supports multiple expressions
            const RedisModuleCommandArg *subargs = TS_MRANGE_ARGS[i].subargs;
            mu_check(subargs != NULL);
            
            int found_filter_expr = 0;
            for (int j = 0; subargs[j].name != NULL; j++) {
                if (strcmp(subargs[j].name, "filterExpr") == 0) {
                    found_filter_expr = 1;
                    mu_check(subargs[j].type == REDISMODULE_ARG_TYPE_STRING);
                    mu_check(subargs[j].flags & REDISMODULE_CMD_ARG_MULTIPLE);
                    break;
                }
            }
            mu_check(found_filter_expr);
            break;
        }
    }
    mu_check(found_filter);
}

// Test that TS.MRANGE has optional arguments like LATEST, FILTER_BY_TS, etc.
MU_TEST(test_mrange_optional_arguments) {
    // Check LATEST is optional
    int found_latest = 0;
    for (int i = 0; TS_MRANGE_ARGS[i].name != NULL; i++) {
        if (strcmp(TS_MRANGE_ARGS[i].name, "LATEST") == 0) {
            found_latest = 1;
            mu_check(TS_MRANGE_ARGS[i].type == REDISMODULE_ARG_TYPE_PURE_TOKEN);
            mu_check(TS_MRANGE_ARGS[i].flags & REDISMODULE_CMD_ARG_OPTIONAL);
            break;
        }
    }
    mu_check(found_latest);
    
    // Check FILTER_BY_VALUE is optional
    int found_filter_by_value = 0;
    for (int i = 0; TS_MRANGE_ARGS[i].name != NULL; i++) {
        if (strcmp(TS_MRANGE_ARGS[i].name, "FILTER_BY_VALUE") == 0) {
            found_filter_by_value = 1;
            mu_check(TS_MRANGE_ARGS[i].type == REDISMODULE_ARG_TYPE_BLOCK);
            mu_check(TS_MRANGE_ARGS[i].flags & REDISMODULE_CMD_ARG_OPTIONAL);
            
            // Should have min and max arguments
            const RedisModuleCommandArg *subargs = TS_MRANGE_ARGS[i].subargs;
            mu_check(subargs != NULL);
            
            int found_min = 0, found_max = 0;
            for (int j = 0; subargs[j].name != NULL; j++) {
                if (strcmp(subargs[j].name, "min") == 0) {
                    found_min = 1;
                    mu_check(subargs[j].type == REDISMODULE_ARG_TYPE_DOUBLE);
                }
                if (strcmp(subargs[j].name, "max") == 0) {
                    found_max = 1;
                    mu_check(subargs[j].type == REDISMODULE_ARG_TYPE_DOUBLE);
                }
            }
            mu_check(found_min && found_max);
            break;
        }
    }
    mu_check(found_filter_by_value);
    
    // Check GROUPBY is optional
    int found_groupby = 0;
    for (int i = 0; TS_MRANGE_ARGS[i].name != NULL; i++) {
        if (strcmp(TS_MRANGE_ARGS[i].name, "GROUPBY") == 0) {
            found_groupby = 1;
            mu_check(TS_MRANGE_ARGS[i].type == REDISMODULE_ARG_TYPE_BLOCK);
            mu_check(TS_MRANGE_ARGS[i].flags & REDISMODULE_CMD_ARG_OPTIONAL);
            break;
        }
    }
    mu_check(found_groupby);
}

// Test that aggregation options are properly defined in MRANGE
MU_TEST(test_mrange_aggregation_options) {
    // Find aggregation argument in TS.MRANGE
    int found_aggregation = 0;
    for (int i = 0; TS_MRANGE_ARGS[i].name != NULL; i++) {
        if (strcmp(TS_MRANGE_ARGS[i].name, "aggregation") == 0) {
            found_aggregation = 1;
            mu_check(TS_MRANGE_ARGS[i].type == REDISMODULE_ARG_TYPE_BLOCK);
            mu_check(TS_MRANGE_ARGS[i].flags & REDISMODULE_CMD_ARG_OPTIONAL);
            
            const RedisModuleCommandArg *subargs = TS_MRANGE_ARGS[i].subargs;
            mu_check(subargs != NULL);
            
            // Check for AGGREGATION subcommand
            int found_agg_block = 0;
            for (int j = 0; subargs[j].name != NULL; j++) {
                if (strcmp(subargs[j].name, "AGGREGATION") == 0) {
                    found_agg_block = 1;
                    mu_check(subargs[j].type == REDISMODULE_ARG_TYPE_BLOCK);
                    
                    // Check for aggregator oneof
                    const RedisModuleCommandArg *agg_subargs = subargs[j].subargs;
                    mu_check(agg_subargs != NULL);
                    
                    int found_aggregator = 0;
                    for (int k = 0; agg_subargs[k].name != NULL; k++) {
                        if (strcmp(agg_subargs[k].name, "aggregator") == 0) {
                            found_aggregator = 1;
                            mu_check(agg_subargs[k].type == REDISMODULE_ARG_TYPE_ONEOF);
                            
                            // Check some aggregation types exist
                            const RedisModuleCommandArg *agg_options = agg_subargs[k].subargs;
                            mu_check(agg_options != NULL);
                            
                            int found_avg = 0, found_sum = 0;
                            for (int l = 0; agg_options[l].name != NULL; l++) {
                                if (strcmp(agg_options[l].name, "avg") == 0) found_avg = 1;
                                if (strcmp(agg_options[l].name, "sum") == 0) found_sum = 1;
                            }
                            mu_check(found_avg && found_sum);
                            break;
                        }
                    }
                    mu_check(found_aggregator);
                    break;
                }
            }
            mu_check(found_agg_block);
            break;
        }
    }
    mu_check(found_aggregation);
}

MU_TEST_SUITE(command_info_test_suite) {
    MU_RUN_TEST(test_ts_add_command_info_structure);
    MU_RUN_TEST(test_ts_alter_command_info_structure);
    MU_RUN_TEST(test_ts_create_command_info_structure);
    MU_RUN_TEST(test_ts_createrule_command_info_structure);
    MU_RUN_TEST(test_ts_incrby_command_info_structure);
    MU_RUN_TEST(test_ts_decrby_command_info_structure);
    MU_RUN_TEST(test_ts_range_command_info_structure);
    MU_RUN_TEST(test_ts_revrange_command_info_structure);
    MU_RUN_TEST(test_ts_queryindex_command_info_structure);
    MU_RUN_TEST(test_ts_mrange_command_info_structure);
    MU_RUN_TEST(test_ts_mrevrange_command_info_structure);
    MU_RUN_TEST(test_command_key_specs);
    MU_RUN_TEST(test_command_arguments);
    MU_RUN_TEST(test_mrange_command_arguments);
    MU_RUN_TEST(test_mrange_optional_arguments);
    MU_RUN_TEST(test_mrange_aggregation_options);
    MU_RUN_TEST(test_duplicate_policy_options);
    MU_RUN_TEST(test_createrule_aggregation_options);
    MU_RUN_TEST(test_register_function_exists);
    MU_RUN_TEST(test_labels_argument_structure);
}
