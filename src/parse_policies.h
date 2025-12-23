/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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
