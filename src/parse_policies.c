/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "parse_policies.h"

#include "compaction.h"
#include "consts.h"

#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include "rmutil/util.h"
#include <rmutil/alloc.h>

static const timestamp_t lookup_intervals[] = { ['m'] = 1,
                                                ['s'] = 1000,
                                                ['M'] = 1000 * 60,
                                                ['h'] = 1000 * 60 * 60,
                                                ['d'] = 1000 * 60 * 60 * 24 };

static int parse_string_to_millisecs(const char *timeStr, timestamp_t *out) {
    char should_be_empty;
    unsigned char interval_type;
    timestamp_t timeSize;
    if (sscanf(timeStr, "%" SCNu64 "%c%c", &timeSize, &interval_type, &should_be_empty) != 2) {
        return FALSE;
    }
    timestamp_t interval_in_millisecs = lookup_intervals[interval_type];
    if (interval_in_millisecs == 0) {
        return FALSE;
    }
    *out = interval_in_millisecs * timeSize;
    return TRUE;
}

static int parse_interval_policy(char *policy, SimpleCompactionRule *rule) {
    char *token;
    char *token_iter_ptr;
    char agg_type[20];

    token = strtok_r(policy, ":", &token_iter_ptr);
    for (int i = 0; i < 3; i++) {
        if (token == NULL) {
            return FALSE;
        }

        if (i == 0) { // first param its the aggregation type
            strcpy(agg_type, token);
        } else if (i == 1) { // the 2nd param is the bucket
            if (parse_string_to_millisecs(token, &rule->timeBucket) == FALSE) {
                return FALSE;
            }
        } else if (i == 2) {
            if (parse_string_to_millisecs(token, &rule->retentionSizeMillisec) == FALSE) {
                return FALSE;
            }
        } else {
            return FALSE;
        }

        token = strtok_r(NULL, ":", &token_iter_ptr);
    }
    int agg_type_index = StringAggTypeToEnum(agg_type);
    if (agg_type_index == TS_AGG_INVALID) {
        return FALSE;
    }
    rule->aggType = agg_type_index;

    return TRUE;
}

static size_t count_char_in_str(const char *string, size_t len, char lookup) {
    size_t count = 0;
    for (size_t i = 0; i < len; i++) {
        if (string[i] == lookup) {
            count++;
        }
    }
    return count;
}

// parse compaction policies in the following format: "max:1m;min:10s;avg:2h;avg:3d"
// the format is AGGREGATION_FUNCTION:\d[s|m|h|d];
int ParseCompactionPolicy(const char *policy_string,
                          SimpleCompactionRule **parsed_rules_out,
                          uint64_t *rules_count) {
    char *token;
    char *token_iter_ptr;
    size_t len = strlen(policy_string);
    char *rest = malloc(len + 1);
    memcpy(rest, policy_string, len + 1);
    *rules_count = 0;

    // the ';' is a separator so we need to add +1 for the policy count
    uint64_t policies_count = count_char_in_str(policy_string, len, ';') + 1;
    *parsed_rules_out = malloc(sizeof(SimpleCompactionRule) * policies_count);
    SimpleCompactionRule *parsed_rules_runner = *parsed_rules_out;

    token = strtok_r(rest, ";", &token_iter_ptr);
    int success = TRUE;
    while (token != NULL) {
        int result = parse_interval_policy(token, parsed_rules_runner);
        if (result == FALSE) {
            success = FALSE;
            break;
        }
        token = strtok_r(NULL, ";", &token_iter_ptr);
        parsed_rules_runner++;
        *rules_count = *rules_count + 1;
    }

    free(rest);
    if (success == FALSE) {
        // all or nothing, don't allow partial parsing
        *rules_count = 0;
    }
    return success;
}
