/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */
#include "parse_policies.h"

#include "compaction.h"
#include "consts.h"

#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include "rmutil/util.h"
#include <rmutil/alloc.h>

#define SINGLE_RULE_ITEM_STRING_LENGTH 32

static const timestamp_t lookup_intervals[] = {
    ['m'] = 1, ['s'] = 1000, ['M'] = 1000 * 60, ['h'] = 1000 * 60 * 60, ['d'] = 1000 * 60 * 60 * 24,
};

static bool parse_string_to_millisecs(const char *timeStr, timestamp_t *out, bool canBeZero) {
    if (!isdigit(*timeStr)) {
        return false;
    }

    char should_be_empty;
    unsigned char interval_type;
    timestamp_t timeSize;
    int ret = sscanf(timeStr, "%" SCNu64 "%c%c", &timeSize, &interval_type, &should_be_empty);
    bool valid_state = (ret == 2) || (ret == 1 && timeSize == 0);
    if (!valid_state) {
        return false;
    }

    if (timeSize == 0) {
        if (canBeZero) {
            *out = 0;
            return true;
        }
        return false;
    }

    timestamp_t interval_in_millisecs = lookup_intervals[interval_type];
    if (interval_in_millisecs == 0) {
        return false;
    }
    *out = interval_in_millisecs * timeSize;
    return true;
}

static bool parse_interval_policy(char *policy, SimpleCompactionRule *rule) {
    char *saveptr;
    rule->timestampAlignment = 0; // the default alignment is 0

    int num_tokens = 0;
    for (char *token = strtok_r(policy, ":", &saveptr); token != NULL;
         token = strtok_r(NULL, ":", &saveptr)) {
        switch (num_tokens++) {
            case 0: // agg type
                if ((rule->aggType = StringAggTypeToEnum(token)) == TS_AGG_INVALID)
                    return false;
                break;
            case 1: // bucket duration
                if (!parse_string_to_millisecs(token, &rule->bucketDuration, false))
                    return false;
                break;
            case 2: // retention size
                if (!parse_string_to_millisecs(token, &rule->retentionSizeMillisec, true))
                    return false;
                break;
            case 3: // timestamp alignment (optional)
                if (!parse_string_to_millisecs(token, &rule->timestampAlignment, true))
                    return false;
                break;
            default:
                return false;
        }
    }

    // we expect 3 or 4 tokens. the tokens are:
    // aggType:bucketDuration:retentionSize:timestampAlignment (optional)
    return num_tokens == 3 || num_tokens == 4;
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
bool ParseCompactionPolicy(const char *policy_string,
                           size_t len,
                           SimpleCompactionRule **parsed_rules_out,
                           uint64_t *rules_count) {
    char *token;
    char *token_iter_ptr;
    char *rest = malloc(len + 1);
    memcpy(rest, policy_string, len + 1);
    *parsed_rules_out = NULL;
    *rules_count = 0;

    // the ';' is a separator so we need to add +1 for the policy count
    uint64_t policies_count = count_char_in_str(policy_string, len, ';') + 1;
    *parsed_rules_out = malloc(policies_count * sizeof **parsed_rules_out);
    SimpleCompactionRule *parsed_rules_runner = *parsed_rules_out;

    token = strtok_r(rest, ";", &token_iter_ptr);
    bool success = true;
    while (token != NULL) {
        bool result = parse_interval_policy(token, parsed_rules_runner);
        if (!result) {
            success = false;
            break;
        }
        token = strtok_r(NULL, ";", &token_iter_ptr);
        parsed_rules_runner++;
        *rules_count = *rules_count + 1;
    }

    free(rest);
    if (!success) {
        // all or nothing, don't allow partial parsing
        *rules_count = 0;
        if (*parsed_rules_out) {
            free(*parsed_rules_out);
            *parsed_rules_out = NULL;
        }
    }
    return success;
}

// Helper function to convert milliseconds to a time string
static inline void MillisecondsToTimeString(uint64_t millis, char *out, const size_t outLength) {
    if (millis % (1000 * 60 * 60 * 24) == 0) {
        snprintf(out, outLength, "%" PRIu64 "d", millis / (1000 * 60 * 60 * 24));
    } else if (millis % (1000 * 60 * 60) == 0) {
        snprintf(out, outLength, "%" PRIu64 "h", millis / (1000 * 60 * 60));
    } else if (millis % (1000 * 60) == 0) {
        snprintf(out, outLength, "%" PRIu64 "M", millis / (1000 * 60));
    } else if (millis % 1000 == 0) {
        snprintf(out, outLength, "%" PRIu64 "s", millis / 1000);
    } else {
        snprintf(out, outLength, "%" PRIu64 "m", millis);
    }
}

char *CompactionRulesToString(const SimpleCompactionRule *compactionRules,
                              const uint64_t compactionRulesCount) {
    if (compactionRules == NULL || compactionRulesCount == 0) {
        return NULL;
    }

    // Estimate a sufficiently large buffer size
    size_t buffer_size = 256 * compactionRulesCount;
    char *result = malloc(buffer_size);
    if (!result) {
        return NULL;
    }
    result[0] = '\0'; // Initialize the string

    for (uint64_t i = 0; i < compactionRulesCount; ++i) {
        const SimpleCompactionRule *rule = &compactionRules[i];

        // Convert bucket duration and retention size to strings
        char bucket_duration[SINGLE_RULE_ITEM_STRING_LENGTH] = { 0 };
        char retention[SINGLE_RULE_ITEM_STRING_LENGTH] = { 0 };
        char alignment[SINGLE_RULE_ITEM_STRING_LENGTH] = { 0 };

        MillisecondsToTimeString(
            rule->bucketDuration, bucket_duration, SINGLE_RULE_ITEM_STRING_LENGTH);
        MillisecondsToTimeString(
            rule->retentionSizeMillisec, retention, SINGLE_RULE_ITEM_STRING_LENGTH);
        if (rule->timestampAlignment > 0) {
            MillisecondsToTimeString(
                rule->timestampAlignment, alignment, SINGLE_RULE_ITEM_STRING_LENGTH);
        }

        // Get aggregation type as string
        const char *aggTypeStr = AggTypeEnumToStringLowerCase(rule->aggType);
        if (!aggTypeStr) {
            free(result);
            return NULL; // Invalid aggregation type
        }

        // Append the rule to the result string
        if (rule->timestampAlignment > 0) {
            snprintf(result + strlen(result),
                     buffer_size - strlen(result),
                     "%s:%s:%s:%s;",
                     aggTypeStr,
                     bucket_duration,
                     retention,
                     alignment);
        } else {
            snprintf(result + strlen(result),
                     buffer_size - strlen(result),
                     "%s:%s:%s;",
                     aggTypeStr,
                     bucket_duration,
                     retention);
        }
    }

    // Remove the trailing semicolon
    size_t len = strlen(result);
    if (len > 0 && result[len - 1] == ';') {
        result[len - 1] = '\0';
    }

    return result;
}
