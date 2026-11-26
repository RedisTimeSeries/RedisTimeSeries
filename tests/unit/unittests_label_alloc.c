/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include <strings.h>
#include "minunit.h"
#include "query_language.h"

// Exercise the parser's allocation failure without needing Redis string objects.
static int labels_oom_arg_index(const char *arg, RedisModuleString **argv, int argc) {
    return 0;
}

#define RMUtil_ArgIndex labels_oom_arg_index
#define parseLabelsFromArgs labels_oom_parse
#include "query_language.c"
#undef parseLabelsFromArgs
#undef RMUtil_ArgIndex

static void *labels_oom_calloc(size_t count, size_t size) {
    return NULL;
}

MU_TEST(test_label_array_oom_returns_error) {
    void *(*saved)(size_t, size_t) = RedisModule_TryCalloc;
    RedisModuleString *argv[3] = { NULL };
    for (int allow_null = 0; allow_null <= 1; allow_null++) {
        Label *labels = (Label *)0xDEAD;
        size_t count = 99;
        RedisModule_TryCalloc = labels_oom_calloc;
        int rc = labels_oom_parse(argv, 3, &count, &labels, allow_null);
        RedisModule_TryCalloc = saved;
        mu_assert_int_eq(REDISMODULE_ERR, rc);
        mu_assert(labels == NULL, "OOM must clear the label array");
        mu_assert(count == 0, "OOM must clear the label count");
    }
}

MU_TEST_SUITE(label_alloc_test_suite) {
    MU_RUN_TEST(test_label_array_oom_returns_error);
}
