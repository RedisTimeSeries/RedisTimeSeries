/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */
#ifndef PARSE_POLICIES_H
#define PARSE_POLICIES_H

#include "consts.h"
#include <stdint.h>
#include <sys/types.h>

typedef struct SimpleCompactionRule
{
    uint64_t bucketDuration;
    uint64_t retentionSizeMillisec;
    int aggType;
    timestamp_t timestampAlignment;
} SimpleCompactionRule;

int ParseCompactionPolicy(const char *policy_string,
                          SimpleCompactionRule **parsed_rules,
                          uint64_t *count_rules);
#endif
