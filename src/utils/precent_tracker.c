/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "heap.h"
#include "redismodule.h"
#include "precent_tracker.h"

typedef struct precentTracker_s
{
    heap_t *left;               // max heap
    heap_t *right;              // min heap
    double median;
} precentTracker_s;

// implements min heap
static int heap_cmp_func_min(const void *val1, const void *val2, __unused const void *udata) {
    return (*((double *)val1) < *((double *)val2)) ? 1 : -1;
}

// implements max heap
static int heap_cmp_func_max(const void *val1, const void *val2, __unused const void *udata) {
    return (*((double *)val1) > *((double *)val2)) ? 1 : -1;
}

precentTracker_t * precent_tracker_new(size_t cap) {
    precentTracker_t *pt = malloc(sizeof(precentTracker_t));
    if(!pt) {
        return NULL;
    }

    pt->median = 0;

    pt->left = heap_new(heap_cmp_func_max, NULL, cap*sizeof(double));
    if(!pt->left) {
        return NULL;
    }

    pt->right = heap_new(heap_cmp_func_min, NULL, cap*sizeof(double));
    if(!pt->right) {
        return NULL;
    }

    return pt;
}

int precent_tracker_add(precentTracker_t *pt, double *val) {
    precentTracker_s *_pt = pt;
    int n_left = heap_count(_pt->left);
    int n_right = heap_count(_pt->right);
    double prev_m;

    if(n_left != n_right) { // heaps are of different size (odd number of items)
        double *top_item;
        heap_t **larger_heap, **smaller_heap;
        if(n_left > n_right) {
            larger_heap = &_pt->left;
            smaller_heap = &_pt->right;
        } else {
            larger_heap = &_pt->right;
            smaller_heap = &_pt->left;
        }
        top_item = heap_peek(*larger_heap);
        RedisModule_Assert(top_item != NULL);
        prev_m = *top_item;

        if(((n_left > n_right) && (*val < prev_m)) || ((n_left < n_right) && (*val > prev_m))) { 
            // val fits the larger heap
            heap_offer(smaller_heap, heap_remove_item(*larger_heap, top_item));
            heap_offer(larger_heap, val);
        } else { // val fits the smaller heap
            heap_offer(smaller_heap, val);
        }

        _pt->median = (*top_item + *val)/2.0;
    } else { // n_left == n_right
        double *min, *max;
        max = heap_peek(_pt->left);
        min = heap_peek(_pt->right);
        prev_m = (*max + *min)/2.0;
        if(*val < prev_m) { // val fits the left heap
            heap_offer(&_pt->left, val);
        } else { // val fits the right heap
            heap_offer(&_pt->right, val);
        }

        _pt->median = *val;
    }

    return 0;
}

double getMedian(precentTracker_t *pt) {
    return ((precentTracker_s *)pt)->median;
}

void precent_tracker_delete(precentTracker_t *pt, double *val) {
    precentTracker_s *_pt = pt;
    int n_left = heap_count(_pt->left);
    int n_right = heap_count(_pt->right);
    heap_t *larger_heap, *smaller_heap;
    if(n_left == n_right) { // both heaps the same size
        // try to remove from the left heap
        if(heap_remove_item(_pt->left, val) != NULL) {
            _pt->median = *((double *)heap_peek(_pt->right));
        } else if(heap_remove_item(_pt->left, val) != NULL) {
            // try to remove from the right heap
            _pt->median = *((double *)heap_peek(_pt->left));
        }
        return;
    } else { // n_left != n_right
        if(n_left > n_right) {
            larger_heap = _pt->left;
            smaller_heap = _pt->right;
        } else {
            larger_heap = _pt->right;
            smaller_heap = _pt->left;
        }

        // try to remove first from the larger heap
        if(heap_remove_item(larger_heap, val) != NULL) {
        } else if(heap_remove_item(smaller_heap, val) != NULL) {
            // try to remove from the smaller heap
            // move the top item of the larger heap to the smaller one
            double *top_item = heap_peek(larger_heap);
            heap_offer(smaller_heap, top_item);
            heap_remove_item(larger_heap, top_item);
        } else {
            return;
        }

        _pt->median = (*((double *)heap_peek(_pt->right)) + *((double *)heap_peek(_pt->left)))/2.0;
        return;
    }
}
