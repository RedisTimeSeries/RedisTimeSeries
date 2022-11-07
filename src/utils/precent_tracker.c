/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "redismodule.h"
#include "precent_tracker.h"
#include "heap.h"
#include "../consts.h"
#include "../load_io_error_macros.h"

typedef struct precentTracker_s
{
    heap_t *left;  // max heap
    heap_t *right; // min heap
    double median;
} precentTracker_s;

// implements min heap
static int heap_cmp_func_min(const void *val1, const void *val2, __unused const void *udata) {
    if (*((double *)val1) == *((double *)val2)) {
        return 0;
    }
    return (*((double *)val1) < *((double *)val2)) ? 1 : -1;
}

// implements max heap
static int heap_cmp_func_max(const void *val1, const void *val2, __unused const void *udata) {
    if (*((double *)val1) == *((double *)val2)) {
        return 0;
    }
    return (*((double *)val1) > *((double *)val2)) ? 1 : -1;
}

precentTracker_t *precent_tracker_new(size_t cap) {
    precentTracker_t *pt = malloc(sizeof(precentTracker_t));
    if (!pt) {
        return NULL;
    }

    pt->median = 0;

    cap *= sizeof(double);
    pt->left = heap_new(heap_cmp_func_max, NULL, &cap);
    if (!pt->left) {
        return NULL;
    }

    pt->right = heap_new(heap_cmp_func_min, NULL, &cap);
    if (!pt->right) {
        return NULL;
    }

    return pt;
}

int precent_tracker_add(precentTracker_t *pt, double val) {
    int n_left = heap_count(pt->left);
    int n_right = heap_count(pt->right);
    double prev_m;
    double *val_copy = (double *)malloc(sizeof(val));
    *val_copy = val;

    if (n_left != n_right) { // heaps are of different size (odd number of items)
        double *top_item;
        heap_t **larger_heap, **smaller_heap;
        if (n_left > n_right) {
            larger_heap = &pt->left;
            smaller_heap = &pt->right;
        } else {
            larger_heap = &pt->right;
            smaller_heap = &pt->left;
        }
        top_item = heap_peek(*larger_heap);
        RedisModule_Assert(top_item != NULL);
        prev_m = *top_item;

        if (((n_left > n_right) && (val < prev_m)) || ((n_left < n_right) && (val > prev_m))) {
            // val fits the larger heap
            heap_offer(smaller_heap, heap_remove_item(*larger_heap, top_item));
            heap_offer(larger_heap, val_copy);
        } else { // val fits the smaller heap
            heap_offer(smaller_heap, val_copy);
        }

        pt->median =
            (*(double *)heap_peek(*larger_heap) + *(double *)heap_peek(*smaller_heap)) / 2.0;
    } else { // n_left == n_right
        double *min, *max, *med;
        if (n_left != 0) {
            max = heap_peek(pt->left);
            min = heap_peek(pt->right);
            prev_m = (*max + *min) / 2.0;
            if (val < prev_m) { // val fits the left heap
                heap_offer(&pt->left, val_copy);
                med = heap_peek(pt->left);
            } else { // val fits the right heap
                heap_offer(&pt->right, val_copy);
                med = heap_peek(pt->right);
            }
            pt->median = *med;
        } else {                             // n_left == n_right == 0
            heap_offer(&pt->left, val_copy); // arbitrarly add to the left heap
            pt->median = val;
        }
    }

    return 0;
}

double precent_tracker_getMedian(precentTracker_t *pt) {
    return ((precentTracker_s *)pt)->median;
}

void precent_tracker_delete(precentTracker_t *pt, double val) {
    int n_left = heap_count(pt->left);
    int n_right = heap_count(pt->right);
    heap_t **larger_heap, **smaller_heap;
    double *removed_item;

    if (n_left == n_right) { // both heaps the same size
        // try to remove from the left heap
        if ((removed_item = heap_remove_item(pt->left, &val)) != NULL) {
            free(removed_item);
            pt->median = *((double *)heap_peek(pt->right));
        } else if ((removed_item = heap_remove_item(pt->left, &val)) != NULL) {
            // try to remove from the right heap
            free(removed_item);
            pt->median = *((double *)heap_peek(pt->left));
        }
        return;
    } else { // n_left != n_right
        if (n_left > n_right) {
            larger_heap = &pt->left;
            smaller_heap = &pt->right;
        } else {
            larger_heap = &pt->right;
            smaller_heap = &pt->left;
        }

        // try to remove first from the larger heap
        if ((removed_item = heap_remove_item(*larger_heap, &val)) != NULL) {
            free(removed_item);
        } else if ((removed_item = heap_remove_item(*smaller_heap, &val)) != NULL) {
            // try to remove from the smaller heap
            // move the top item of the larger heap to the smaller one
            free(removed_item);
            double *top_item = heap_peek(*larger_heap);
            heap_offer(smaller_heap, top_item);
            heap_remove_item(*larger_heap, top_item);
        } else {
            return;
        }

        pt->median = (*((double *)heap_peek(pt->right)) + *((double *)heap_peek(pt->left))) / 2.0;
        return;
    }
}

void precent_tracker_RDBWrite(precentTracker_t *pt, RedisModuleIO *io) {
    heapRDBWrite(pt->left, io, RedisModule_SaveDouble);
    heapRDBWrite(pt->right, io, RedisModule_SaveDouble);
    RedisModule_SaveDouble(io, pt->median);
}

int precent_tracker_RDBRead(precentTracker_t *pt, RedisModuleIO *io) {
    heapRDBRead(pt->left, io, LoadDouble_IOError, goto err);
    heapRDBRead(pt->right, io, LoadDouble_IOError, goto err);
    pt->median = LoadDouble_IOError(io, goto err);
    return TSDB_OK;
err:
    return TSDB_ERROR;
}
