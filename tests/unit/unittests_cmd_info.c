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

MU_TEST_SUITE(command_info_test_suite) {
    MU_RUN_TEST(test_ts_add_command_info_structure);
    MU_RUN_TEST(test_ts_alter_command_info_structure);
    MU_RUN_TEST(test_ts_create_command_info_structure);
    MU_RUN_TEST(test_ts_createrule_command_info_structure);
    MU_RUN_TEST(test_command_key_specs);
    MU_RUN_TEST(test_command_arguments);
    MU_RUN_TEST(test_duplicate_policy_options);
    MU_RUN_TEST(test_createrule_aggregation_options);
    MU_RUN_TEST(test_register_function_exists);
    MU_RUN_TEST(test_labels_argument_structure);
}
