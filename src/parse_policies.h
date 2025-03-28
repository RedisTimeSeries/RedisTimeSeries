/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */
#ifndef PARSE_POLICIES_H
#define PARSE_POLICIES_H

#include "consts.h"
#include <stdint.h>
#include <stdint.h>

typedef struct SimpleCompactionRule
{
    uint64_t bucketDuration;
    uint64_t retentionSizeMillisec;
    int aggType;
    timestamp_t timestampAlignment;
} SimpleCompactionRule;

bool ParseCompactionPolicy(const char *policy_string,
                           size_t len,
                           SimpleCompactionRule **parsed_rules,
                           uint64_t *count_rules);

/* Converts the compaction rules back to a string. The returned string
must be deallocated by the user using free(). Note that it might use the
redis allocator. */
char *CompactionRulesToString(const SimpleCompactionRule *compactionRules,
                              uint64_t compactionRulesCount);
#endif
