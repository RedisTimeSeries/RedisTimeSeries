/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#include <string.h>
#include <stdio.h>
#include <rmutil/alloc.h>
#include "consts.h"
#include "compaction.h"
#include "parse_policies.h"

static const int lookup_intervals[] = {
   ['m'] = 1,
   ['s'] = 1000,
   ['M'] = 1000*60,
   ['h'] = 1000*60*60,
   ['d'] = 1000*60*60*24
};

int parse_string_to_millisecs(const char *timeStr, int *out){
    char should_be_empty;
    unsigned char interval_type;
    int timeSize;
    if (sscanf(timeStr, "%d%c%c", &timeSize, &interval_type, &should_be_empty) != 2) {
        return FALSE;
    }
    int interval_in_millisecs = lookup_intervals[interval_type];
    if (interval_in_millisecs == 0) {
        return FALSE;
    }
    *out = interval_in_millisecs * timeSize;
    return TRUE;
}

int parse_interval_policy(char *policy, SimpleCompactionRule *rule) {
    char *token;
    char *token_iter_ptr;
    char agg_type[20];

    token = strtok_r(policy, ":", &token_iter_ptr);
    for (int i=0; i<3; i++)
    {
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

        token = strtok_r (NULL, ":", &token_iter_ptr);
    }
    int agg_type_index = StringAggTypeToEnum(agg_type);
    if (agg_type_index == TS_AGG_INVALID) {
        return FALSE;
    }
    rule->aggType = agg_type_index;

    return 1;
}

int count_char_in_str(const char * string, int len, char lookup) {
    int count = 0;
    for (int i=0; i<len; i++) {
        if (string[i] == lookup) {
            count++;
        }
    }
    return count;
}

// parse compaction policies in the following format: "max:1m;min:10s;avg:2h;avg:3d"
// the format is AGGREGATION_FUNCTION:\d[s|m|h|d];
int ParseCompactionPolicy(const char * policy_string, SimpleCompactionRule **parsed_rules_out,
            size_t *rules_count) {
    char *token;
    char *token_iter_ptr;
    size_t len = strlen(policy_string);
    char *rest = malloc(len + 1);
    memcpy(rest, policy_string, len+1);
    *rules_count = 0;

    // the ';' is a separator so we need to add +1 for the policy count
    int policies_count = count_char_in_str(policy_string, len, ';') + 1;
    SimpleCompactionRule *parsed_rules = malloc(sizeof(SimpleCompactionRule) * policies_count);
    *parsed_rules_out = parsed_rules;

    token = strtok_r (rest, ";", &token_iter_ptr);
    int success = TRUE;
    while (token != NULL)
    {
        int result = parse_interval_policy(token, parsed_rules);
        if (result == FALSE ) {
            success = FALSE;
            break;
        }
        token = strtok_r (NULL, ";", &token_iter_ptr);
        parsed_rules++;
        *rules_count = *rules_count + 1;
    }

    free(rest);
    if (success == FALSE) {
        // all or nothing, don't allow partial parsing
        *rules_count = 0;
    }
    return success;
}
