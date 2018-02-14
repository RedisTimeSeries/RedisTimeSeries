#ifndef PARSE_POLICIES_H
#define PARSE_POLICIES_H

#include <sys/types.h>

typedef struct SimpleCompactionRule {
    int32_t bucketSizeSec;
    int32_t retentionSizeSec;
    int aggType;
} SimpleCompactionRule;

int ParseCompactionPolicy(const char * policy_string, SimpleCompactionRule **parsed_rules, size_t *count_rules);
#endif
