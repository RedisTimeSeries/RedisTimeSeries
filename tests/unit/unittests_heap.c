/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/*
 * Tests for the binary heap (src/utils/heap.c) and for the multi-series sample
 * iterator built on it (src/multiseries_sample_iterator.c).
 *
 * Heap items are pointers to ints. cmp_int() makes it a max-heap (the biggest
 * int is the root) and returns 0 for equal ints, which the membership and
 * root-callback functions rely on. check_order() verifies
 * the heap through the public API only: it polls every item, checks they come
 * out biggest first and match the expected contents, then offers them back.
 */

#include "minunit.h"
#include "multiseries_sample_iterator.h"
#include "utils/heap.h"

#include <stdlib.h>
#include <string.h>

static int cmp_int(const void *a, const void *b, const void *udata) {
    (void)udata;
    const int x = *(const int *)a, y = *(const int *)b;
    return (x > y) - (x < y);
}

static int cmp_desc(const void *a, const void *b) {
    return cmp_int(b, a, NULL);
}

// expected: the values the heap must hold, any order. Leaves the heap with the same items.
static void check_order(heap_t **h, const int *expected, int n) {
    int *want = malloc(sizeof(int) * (n ? n : 1));
    for (int i = 0; i < n; i++) {
        want[i] = expected[i];
    }
    qsort(want, n, sizeof(int), cmp_desc);

    mu_assert_int_eq(n, heap_count(*h));
    mu_check(n == 0 ? heap_peek(*h) == NULL : *(int *)heap_peek(*h) == want[0]);

    int **polled = malloc(sizeof(int *) * (n ? n : 1));
    for (int i = 0; i < n; i++) {
        polled[i] = heap_poll(*h);
        mu_check(polled[i] != NULL);
        mu_assert_int_eq(want[i], *polled[i]);
    }
    mu_check(heap_poll(*h) == NULL);
    for (int i = 0; i < n; i++) {
        mu_assert_int_eq(0, heap_offer(h, polled[i]));
    }
    free(polled);
    free(want);
}

MU_TEST(test_heap_empty) {
    heap_t *h = heap_new(cmp_int, NULL);
    int x = 1;
    mu_assert_int_eq(0, heap_count(h));
    mu_assert_int_eq(13, heap_size(h)); // DEFAULT_CAPACITY
    mu_check(heap_peek(h) == NULL);
    mu_check(heap_poll(h) == NULL);
    mu_check(!heap_contains_item(h, &x));
    heap_clear(h);
    mu_assert_int_eq(0, heap_count(h));
    check_order(&h, NULL, 0);
    heap_free(h);
}

MU_TEST(test_heap_singleton) {
    heap_t *h = heap_new(cmp_int, NULL);
    int x = 7, other = 8;
    mu_assert_int_eq(0, heap_offer(&h, &x));
    mu_check(heap_peek(h) == &x);
    mu_check(heap_contains_item(h, &x));
    mu_check(!heap_contains_item(h, &other));
    check_order(&h, (int[]){ 7 }, 1);

    mu_check(heap_poll(h) == &x);
    check_order(&h, NULL, 0);
    heap_free(h);
}

// Offering past DEFAULT_CAPACITY grows the array; peek tracks the max after every offer.
MU_TEST(test_heap_offer_grows_and_poll_drains_in_order) {
    enum { N = 40 };
    int v[N];
    heap_t *h = heap_new(cmp_int, NULL);
    for (int i = 0; i < N; i++) {
        v[i] = (i * 17) % N; // a permutation of 0..39, interleaved
        mu_assert_int_eq(0, heap_offer(&h, &v[i]));
        mu_assert_int_eq(i + 1, heap_count(h));
        int max = 0;
        for (int j = 0; j <= i; j++) {
            max = v[j] > max ? v[j] : max;
        }
        mu_assert_int_eq(max, *(int *)heap_peek(h));
    }
    mu_check(heap_size(h) >= N);
    mu_assert_int_eq(52, heap_size(h)); // 13 -> 26 -> 52
    check_order(&h, v, N);
    for (int i = N - 1; i >= 0; i--) {
        mu_assert_int_eq(i, *(int *)heap_poll(h));
    }
    mu_check(heap_poll(h) == NULL);
    heap_free(h);
}

// Equal items all come out, next to each other.
MU_TEST(test_heap_duplicates) {
    int v[] = { 5, 3, 5, 1, 5, 3 };
    heap_t *h = heap_new(cmp_int, NULL);
    for (int i = 0; i < 6; i++) {
        mu_assert_int_eq(0, heap_offer(&h, &v[i]));
        check_order(&h, v, i + 1);
    }
    heap_free(h);
}

// heap_init() on caller memory: heap_offerx() never grows and fails once full.
MU_TEST(test_heap_init_offerx_fixed_capacity) {
    enum { CAP = 4 };
    heap_t *h = malloc(heap_sizeof(CAP));
    heap_init(h, cmp_int, NULL, CAP);
    mu_assert_int_eq(CAP, heap_size(h));
    int v[] = { 2, 9, 4, 7, 100 };
    for (int i = 0; i < CAP; i++) {
        mu_assert_int_eq(0, heap_offerx(h, &v[i]));
        check_order(&h, v, i + 1);
    }
    mu_assert_int_eq(-1, heap_offerx(h, &v[CAP]));
    mu_assert_int_eq(CAP, heap_count(h));
    mu_assert_int_eq(CAP, heap_size(h));
    check_order(&h, v, CAP); // 100 was not added
    heap_free(h);
}

// heap_replace() swaps the root and sifts the new item down to its place.
MU_TEST(test_heap_replace_root) {
    int v[] = { 10, 8, 6, 4, 2 };
    int small = 1, big = 50, mid = 5;
    heap_t *h = heap_new(cmp_int, NULL);
    for (int i = 0; i < 5; i++) {
        heap_offer(&h, &v[i]);
    }

    heap_replace(h, &small); // 10 out, 1 in: goes to the bottom
    check_order(&h, (int[]){ 8, 6, 4, 2, 1 }, 5);

    heap_replace(h, &big); // 8 out, 50 in: stays at the root
    mu_check(heap_peek(h) == &big);
    check_order(&h, (int[]){ 50, 6, 4, 2, 1 }, 5);

    heap_replace(h, &mid); // 50 out, 5 in: lands in the middle
    check_order(&h, (int[]){ 6, 5, 4, 2, 1 }, 5);
    heap_free(h);
}

MU_TEST(test_heap_clear_keeps_items_and_heap) {
    int v[] = { 3, 1, 2 };
    heap_t *h = heap_new(cmp_int, NULL);
    for (int i = 0; i < 3; i++) {
        heap_offer(&h, &v[i]);
    }
    heap_clear(h);
    check_order(&h, NULL, 0);
    mu_assert_int_eq(13, heap_size(h));

    // The heap is reusable after a clear.
    heap_offer(&h, &v[1]);
    heap_offer(&h, &v[0]);
    check_order(&h, (int[]){ 3, 1 }, 2);
    heap_free(h);
}

static int freed_count;
static void count_and_free(void *p) {
    freed_count++;
    free(p);
}

// heap_clear_free_items() frees every item; ASan/LSan catch a miss or a double free.
MU_TEST(test_heap_clear_free_items) {
    heap_t *h = heap_new(cmp_int, NULL);
    for (int i = 0; i < 20; i++) {
        int *p = malloc(sizeof(int));
        *p = i % 7;
        heap_offer(&h, p);
    }
    freed_count = 0;
    heap_clear_free_items(h, count_and_free);
    mu_assert_int_eq(20, freed_count);
    check_order(&h, NULL, 0);

    freed_count = 0;
    heap_clear_free_items(h, count_and_free); // empty: no calls
    mu_assert_int_eq(0, freed_count);
    heap_free(h);
}

MU_TEST(test_heap_contains_item) {
    int v[] = { 4, 9, 1, 7, 7 };
    int absent = 5, equal_to_nine = 9;
    heap_t *h = heap_new(cmp_int, NULL);
    for (int i = 0; i < 5; i++) {
        heap_offer(&h, &v[i]);
    }
    for (int i = 0; i < 5; i++) {
        mu_check(heap_contains_item(h, &v[i]));
    }
    mu_check(!heap_contains_item(h, &absent));
    mu_check(heap_contains_item(h, &equal_to_nine)); // membership is by cmp(), not by pointer
    check_order(&h, v, 5);
    heap_free(h);
}

typedef struct
{
    int n;
    int *seen[16];
} CbCtx;

static void collect(void *dst, void *src) {
    CbCtx *ctx = dst;
    ctx->seen[ctx->n++] = src;
}

// heap_cb_root() calls back once for the root and once for every item equal to it.
MU_TEST(test_heap_cb_root) {
    heap_t *h = heap_new(cmp_int, NULL);
    CbCtx ctx = { 0 };
    heap_cb_root(h, collect, &ctx); // empty heap: no calls
    mu_assert_int_eq(0, ctx.n);

    int v[] = { 3, 9, 1, 9, 9, 2, 9, 4, 8 };
    for (int i = 0; i < 9; i++) {
        heap_offer(&h, &v[i]);
    }
    heap_cb_root(h, collect, &ctx);
    mu_assert_int_eq(4, ctx.n);
    for (int i = 0; i < ctx.n; i++) {
        mu_assert_int_eq(9, *ctx.seen[i]);
        for (int j = 0; j < i; j++) {
            mu_check(ctx.seen[i] != ctx.seen[j]); // each 9 exactly once
        }
    }
    check_order(&h, v, 9); // read-only

    int one = 1;
    heap_t *single = heap_new(cmp_int, NULL);
    heap_offer(&single, &one);
    ctx.n = 0;
    heap_cb_root(single, collect, &ctx);
    mu_assert_int_eq(1, ctx.n);
    mu_check(ctx.seen[0] == &one);
    heap_free(single);
    heap_free(h);
}

/* ---- multi-series sample iterator ---- */

typedef struct
{
    AbstractSampleIterator base;
    const Sample *samples;
    size_t n, pos;
    int closed;
} FakeSeriesIter;

static ChunkResult fake_get_next(AbstractSampleIterator *base, Sample *sample) {
    FakeSeriesIter *it = (FakeSeriesIter *)base;
    if (it->pos == it->n) {
        return CR_END;
    }
    *sample = it->samples[it->pos++];
    return CR_OK;
}

static void fake_close(AbstractSampleIterator *base) {
    ((FakeSeriesIter *)base)->closed++;
}

static void fake_init(FakeSeriesIter *it, const Sample *samples, size_t n) {
    *it = (FakeSeriesIter){ .base = { .GetNext = fake_get_next, .Close = fake_close },
                            .samples = samples,
                            .n = n };
}

// Series a/b/c have interleaved timestamps, 30 is in all three and 20 in a and c; d is
// empty. A sample's value is its timestamp + series/10, so it names the series it came from.
static const Sample ser_a[] = { { 10, 10.1 }, { 20, 20.1 }, { 30, 30.1 }, { 60, 60.1 } };
static const Sample ser_b[] = { { 15, 15.2 }, { 30, 30.2 }, { 45, 45.2 } };
static const Sample ser_c[] = { { 5, 5.3 }, { 20, 20.3 }, { 30, 30.3 }, { 70, 70.3 } };

static void reversed(const Sample *in, size_t n, Sample *out) {
    for (size_t i = 0; i < n; i++) {
        out[i] = in[n - 1 - i];
    }
}

static int cmp_double_desc(const void *a, const void *b) {
    const double x = *(const double *)a, y = *(const double *)b;
    return (x < y) - (x > y);
}

// Drains the merged stream and checks the timestamp order exactly. Samples with the same
// timestamp come out next to each other in an unspecified order, so each such run is
// compared as a set.
static void check_merge(bool reverse) {
    static const timestamp_t asc_ts[] = { 5, 10, 15, 20, 20, 30, 30, 30, 45, 60, 70 };
    static const double asc_val[] = { 5.3,  10.1, 15.2, 20.3, 20.1, 30.3,
                                      30.2, 30.1, 45.2, 60.1, 70.3 };
    enum { N = 11 };
    Sample a[4], b[3], c[4];
    FakeSeriesIter it[4];
    if (reverse) { // each input is in the same direction as the merge
        reversed(ser_a, 4, a);
        reversed(ser_b, 3, b);
        reversed(ser_c, 4, c);
    } else {
        memcpy(a, ser_a, sizeof(a));
        memcpy(b, ser_b, sizeof(b));
        memcpy(c, ser_c, sizeof(c));
    }
    fake_init(&it[0], a, 4);
    fake_init(&it[1], NULL, 0);
    fake_init(&it[2], b, 3);
    fake_init(&it[3], c, 4);
    AbstractSampleIterator *inputs[] = { &it[0].base, &it[1].base, &it[2].base, &it[3].base };

    AbstractMultiSeriesSampleIterator *ms =
        (AbstractMultiSeriesSampleIterator *)MultiSeriesSampleIterator_New(inputs, 4, reverse);
    Sample out[N + 1];
    size_t n = 0;
    while (n <= N && ms->GetNext(ms, &out[n]) == CR_OK) {
        n++;
    }
    mu_assert_int_eq(N, n);
    mu_check(ms->GetNext(ms, &out[N]) == CR_END); // stays at the end

    for (size_t i = 0; i < N;) {
        const size_t e = reverse ? N - 1 - i : i; // expected index
        size_t run = 1;
        while (i + run < N && out[i + run].timestamp == out[i].timestamp) {
            run++;
        }
        double got[3], want[3];
        mu_check(run <= 3);
        for (size_t k = 0; k < run; k++) {
            const size_t ek = reverse ? e - k : e + k;
            mu_assert_int_eq(asc_ts[ek], out[i + k].timestamp);
            got[k] = out[i + k].value;
            want[k] = asc_val[ek];
        }
        qsort(got, run, sizeof(double), cmp_double_desc);
        qsort(want, run, sizeof(double), cmp_double_desc);
        for (size_t k = 0; k < run; k++) {
            mu_assert_double_eq(want[k], got[k]);
        }
        i += run;
    }

    ms->Close(ms);
    for (int i = 0; i < 4; i++) {
        mu_assert_int_eq(1, it[i].closed);
    }
}

MU_TEST(test_multiseries_merge_forward) {
    check_merge(false);
}

// MREVRANGE GROUPBY reduces forward (MultiSeriesReduce passes reverse=false), so the
// reverse comparator is reachable only from here.
MU_TEST(test_multiseries_merge_reverse) {
    check_merge(true);
}

// Closing before the end frees the samples still waiting in the heap (LSan checks that).
MU_TEST(test_multiseries_close_early) {
    Sample a[4], c[4];
    reversed(ser_a, 4, a);
    reversed(ser_c, 4, c);
    FakeSeriesIter it[2];
    fake_init(&it[0], a, 4);
    fake_init(&it[1], c, 4);
    AbstractSampleIterator *inputs[] = { &it[0].base, &it[1].base };
    AbstractMultiSeriesSampleIterator *ms =
        (AbstractMultiSeriesSampleIterator *)MultiSeriesSampleIterator_New(inputs, 2, true);
    Sample s;
    mu_check(ms->GetNext(ms, &s) == CR_OK);
    mu_assert_int_eq(70, s.timestamp);
    mu_check(ms->GetNext(ms, &s) == CR_OK);
    mu_assert_int_eq(60, s.timestamp);
    ms->Close(ms);
    mu_assert_int_eq(1, it[0].closed);
    mu_assert_int_eq(1, it[1].closed);
}

MU_TEST(test_multiseries_all_empty) {
    FakeSeriesIter it[2];
    fake_init(&it[0], NULL, 0);
    fake_init(&it[1], NULL, 0);
    AbstractSampleIterator *inputs[] = { &it[0].base, &it[1].base };
    AbstractMultiSeriesSampleIterator *ms =
        (AbstractMultiSeriesSampleIterator *)MultiSeriesSampleIterator_New(inputs, 2, true);
    Sample s;
    mu_check(ms->GetNext(ms, &s) == CR_END);
    ms->Close(ms);
    mu_assert_int_eq(1, it[0].closed);
    mu_assert_int_eq(1, it[1].closed);
}

MU_TEST_SUITE(heap_test_suite) {
    MU_RUN_TEST(test_heap_empty);
    MU_RUN_TEST(test_heap_singleton);
    MU_RUN_TEST(test_heap_offer_grows_and_poll_drains_in_order);
    MU_RUN_TEST(test_heap_duplicates);
    MU_RUN_TEST(test_heap_init_offerx_fixed_capacity);
    MU_RUN_TEST(test_heap_replace_root);
    MU_RUN_TEST(test_heap_clear_keeps_items_and_heap);
    MU_RUN_TEST(test_heap_clear_free_items);
    MU_RUN_TEST(test_heap_contains_item);
    MU_RUN_TEST(test_heap_cb_root);
    MU_RUN_TEST(test_multiseries_merge_forward);
    MU_RUN_TEST(test_multiseries_merge_reverse);
    MU_RUN_TEST(test_multiseries_close_early);
    MU_RUN_TEST(test_multiseries_all_empty);
}
