/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#ifndef UTIL_CARR_H_
#define UTIL_CARR_H_
/* circular_arr.h - simple, easy to use circular dynamic array with fast pointers,
 * to allow native access to members. It can accept pointers, struct literals and scalars.
 *
 * Example usage:
 *
 *  int *arr = carray_new(int, 8);
 *  // Add elements to the array
 *  for (int i = 0; i < 100; i++) {
 *   array_append(arr, i);
 *  }
 *
 *  // read individual elements
 *  for (int i = 0; i < array_len(arr); i++) {
 *    printf("%d\n", arr[i]);
 *  }
 *
 *  array_free(arr);
 *
 *
 *  */
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <assert.h>

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Definition of malloc & friedns that can be overridden before including arr.h.
 * Alternatively you can include arr_rm_alloc.h, which wraps arr.h and sets the allcoation functions
 * to those of the RM_ family
 */
#ifndef carray_alloc_fn
#define carray_alloc_fn malloc
#define carray_realloc_fn realloc
#define carray_free_fn free
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"

// In the beginning end == start, end always point to the place of the next insertion.
// When (end + 1)%cap == start the capacity of the buffer will increase (the last element is never used).
typedef struct {
  uint32_t start; // start index
  uint32_t end;   // end index
	uint32_t cap;
	uint32_t elem_sz;
	char buf[];
} carray_s;

typedef carray_s carray_t;
/* Internal - calculate the array size for allocations */
#define carray_sizeof(carr) (sizeof(carray_s) + (uint64_t)carr->cap * carr->elem_sz)

static inline uint32_t carray_len(carray_t *arr) {
    return (arr->end >= arr->start) ? arr->end - arr->start : arr->end + arr->cap - arr->start;
}

/* Initialize a new array with a given element size and capacity. Should not be used directly - use
 * carray_new instead */
static carray_t *carray_new_sz(uint32_t elem_sz, uint32_t cap, uint32_t len) {
  cap += 1; // +1 cause the last place in the array is never used
	uint64_t array_size = (uint64_t)cap * (uint64_t)elem_sz;
	carray_s *arr = (carray_s *)carray_alloc_fn(sizeof(carray_s) + array_size);
  _log_if(len >= cap, "Trying to init circular array with len >= cap");
	arr->cap = cap;
	arr->elem_sz = elem_sz;
  arr->start = 0;
  arr->end = len;
	return arr;
}

/* Functions declared as symbols for use in debugger */
void carray_debug(void *pp);

/* Initialize an carray for a given type T with a given capacity and zero length.
 *  */
#define carray_new(T, cap) (carray_new_sz(sizeof(T), cap, 0))

/* Initialize an carray for a given type T with a given length. The capacity allocated is identical
 * to the length
 *  */
#define carray_newlen(T, len) (carray_new_sz(sizeof(T), len, len))

#define carray_tail_index(arr) (((arr)->end > 0) ? ((arr)->end - 1) : ((arr)->cap - 1))

/* get the last element in the array */
#define carray_tail(arr) ((arr)->buf[(arr)->elem_sz*carray_tail_index(arr)])

#define carray_end_addres(arr) &((arr)->buf[(arr)->end*(arr)->elem_sz])

#define carray_front(arr) ((arr)->buf[(arr)->elem_sz*(arr)->start])

#define carray_full(arr) (((arr)->end + 1) % (arr)->cap == (arr)->start)

#define carray_empty(arr) ((arr)->end == (arr)->start)

#define carray_push_back(arr, x) do {                                                 \
    _log_if(carray_full(arr), "circular array overflow");                           \
    typeof(x) _x = (x); \
    memcpy(carray_end_addres(arr), &(_x), (arr)->elem_sz);                           \
    (arr)->end = ((arr)->end + 1)%(arr)->cap;                                       \
  } while(0)

/* Remove element from the beginning of the array if not empty, reduce the size and return it */
#define carray_pop_front(arr, T)                           \
__extension__({                                           \
    _log_if(carray_empty(arr), "Trying to pop element from empty array");                  \
    typeof((arr)->start) old_start = (arr)->start;                  \
    (arr)->start = ((arr)->start + 1)%((arr)->cap);         \
    *(((T *)((arr)->buf)) + old_start);            \
})

#define array_RDBWrite(arr, io, saveItem_cb)            \
  __extension__({                                                \
    size_t len = carray_len(arr);                                   \
    RedisModule_SaveUnsigned(io, len);  \
    for (size_t i = (arr)->start; i < len; i = (i + 1)%(arr)->cap) { \
      saveItem_cb(io, (arr)->buf[i*(arr)->elem_sz]);                          \
    }                                                            \
  })

#define array_RDBRead(arr, io, loadItem_cb, cleanup_exp)             \
  __extension__({                                                \
    size_t len = LoadDouble_IOError((io), cleanup_exp);                                \
    (arr)->start = 0;    \
    _log_if(len >= (arr)->cap, "trying to load array with len bigger than capacity"); \
    (arr)->end = len; \
    for (size_t i = (arr)->start; i < len; i = (i + 1)%(arr)->cap) { \
      typeof(loadItem_cb((io), cleanup_exp)) ret = loadItem_cb((io), cleanup_exp); \
      carray_push_back((arr), ret);  \
    }                                                            \
  })

#if 0 // Untested functions: most of them not working properly

static void carray_ensure_cap(carray_t **arr, uint32_t cap) {
  carray_s *carr = (carray_s *)*arr;
	if(cap > carr->cap) {
    uint32_t old_cap = carr->cap;
		carr->cap = MAX(carr->cap * 2, cap);
		*arr = carr = (carray_s *)carray_realloc_fn(carr, carray_sizeof(carr));

    if(carr->start > carr->end) {
        uint32_t diff = cap - old_cap;
        memmove(carr->buf + (carr->start+diff)*carr->elem_sz, carr->buf + (carr->start*carr->elem_sz), (old_cap - carr->start)*carr->elem_sz);
        carr->start += diff;
    }
	}

	return;
}

/* Ensure capacity for the array to grow by n */
static inline void carray_grow(carray_t **arr, size_t n) {
    carray_s *carr = (carray_s *)*arr;
    uint32_t new_len = carray_len(*arr) + n;
	  carray_ensure_cap(arr, new_len);
    carr->end = (carr->end + n)%carr->cap;
    return;
}

// the following function isn't working: TODO: the arr->end should be incremented after the memcpy
#define carray_push_back_safe(arrp, x) do {                                                 \
    if(carray_full(arrp)) {                                   \
        carray_grow(arrp, 1);                                                        \
    } else {                                                                      \
        (*arrp)->end = ((*arrp)->end + 1)%carr->cap;                                       \
    }                             \
    memcpy(&carray_tail(*arrp), &(x), (*arrp)->elem_sz);                           \
  } while(0)

static inline void carray_ensure_len(carray_t **arr, size_t len) {
	if(len <= carray_len(*arr)) {
		return;
	}
	len -= carray_len(*arr);
	carray_grow(arr, len);
}

/* Ensures that array_tail will always point to a valid element. */
#define carray_ensure_tail(arrpp, T)            \
  ({                                           \
    if (!*(arrpp)) {                           \
      *(arrpp) = carray_newlen(T, 1);           \
    } else {                                   \
      carray_grow((arrpp), 1);                  \
    }                                          \
    &(carray_tail(*(arrpp)));                   \
  })


/**
 * Appends elements to the end of the array, creating the array if it does
 * not exist
 * @param carr carray. Can be NULL
 * @param src array (i.e. C array) of elements to append
 * @param n length of sec
 * @param T type of the array (for sizeof)
 * @return the array
 */
#define carray_ensure_append(arrpp, src, n, T) do {                \
    uint32_t old_start = 0;                                       \
    uint32_t old_end = 0;                                         \
    carray_s *carr;                                               \
    if (!arr) {                                                   \
        *(arrpp) = carr = carray_newlen(T, n);                     \
    } else {                                                      \
        carr = (carray_s *)&arrpp;                                \
        old_start = carr->start;                                  \
        old_end = carr->end;                                      \
        carray_grow(arrpp, n);                                     \
    }                                                             \
    if(old_start <= old_end) {                                    \
        uint32_t n_end = MIN(n, carr->cap - old_end);             \
        memcpy((T *)(carr->buf) + old_end, (src), n_end*sizeof(T));      \
        memcpy((T *)(carr->buf), (src) + n_end, ((n) - n_end)*sizeof(T));    \
    } else {                                                      \
        memcpy((T *)(carr->buf) + old_end, (src), (n)*sizeof(T));            \
    }                                                             \
  } while(0)

/**
 * Does the same thing as ensure_append, but the added elements are
 * at the _beginning_ of the array
 */
#define carray_ensure_prepend(arr, src, n, T) do {                                          \
    uint32_t old_start = 0;                                                                \
    uint32_t old_end = 0;                                                                  \
    carray_s *carr = (carray_s *)arr;                                                     \
    if (!arr) {                                                                            \
        carr = (carray_s *)carray_newlen(T, n);                                             \
    } else {                                                                               \
        old_start = carr->start;                                                           \
        old_end = carr->end;                                                               \
        carray_grow(arr, n);                                                                \
    }                                                                                      \
    if(old_start <= old_end) {                                                             \
        uint32_t n_start = MIN(n, old_start);                                              \
        uint32_t n_end = (n) - n_start;                                                    \
        memcpy((T *)(carr->buf) + old_start - n_start, (src) + (n) - n_start, n_start * sizeof(T));   \
        memcpy((T *)(carr->buf) + carr->cap - n_end, (src), n_end*sizeof(T));                          \
    } else {                                                                               \
        memcpy((T *)(carr->buf) + old_end, src, (n)*sizeof(T));                                       \
    }                                                                                      \
  } while(0)

/*
 * This macro is useful for sparse arrays. It ensures that `*arrpp` will
 * point to a valid index in the array, growing the array to fit.
 *
 * If the array needs to be expanded in order to contain the index, then
 * the unused portion of the array (i.e. the space between the previously
 * last-valid element and the new index) is zero'd
 *
 * @param arrpp a pointer to the array (e.g. `T**`)
 * @param pos the index that should be considered valid
 * @param T the type of the array (in case it must be created)
 * @return A pointer of T at the requested index
 */
#define carray_ensure_at(arrpp, pos, T)                                    \
  ({                                                                      \
    if (!(*(arrpp))) {                                                    \
      *(arrpp) = carray_new(T, 1);                                         \
    }                                                                     \
    carray_s carr = (carray_s)*(arrpp);                                   \
    int32_t old_len = carray_len(carr);                                   \
    if (old_len <= (pos)) {                                               \
        uint32_t old_start = carr.start;                                 \
        uint32_t old_end = carr.end;                                     \
        carray_grow(*(arrpp), (pos) + 1);                                  \
        if(old_start <= old_end) {                                        \
            uint32_t n_added = (((pos) + 1) - old_len);                       \
            uint32_t n_end = MIN(n_added, carr.cap - old_end);                \
            memset((T *)(carr.buf) + old_end, 0, n_end*sizeof(T));       \
            memset((T *)(carr.buf), 0, (n_added - n_end)*sizeof(T));     \
        } else {                                                              \
            memset((T *)(carr.buf) + old_end, 0, sizeof(T) * n_added);  \
        }                                                                     \
    }                                                                       \
    (T *)(carr->buf) + carr->start + (pos);                                   \
  })

/* get the element at index x, assumes that the array has len which is larger than x */
#define carray_at(arr, x, T)                                                     \
({                                                                              \
    carray_s *carr = (carray_s *)arr;                                           \
    *((T *)carr->buf + ((carr->start + x)%carr->cap));                          \
})

/* get pointer to the element at index x, assumes that the array has len which is larger than x */
#define carray_at_ptr(arr, x, T)                                                 \
({                                                                              \
    carray_s *carr = (carray_s *)arr;                                           \
    (T *)carr->buf + ((carr->start + x)%carr->cap);                            \
})

/* Get the length of the array */
static inline uint32_t carray_len(carray_t *arr) {
    if(!arr) {
        return 0;
    }
    carray_s *carr = (carray_s *)arr;
    if(carr->end >= carr->start) {
        return carr->end - carr->start;
    } else {
        return (carr->cap - carr->start) + carr->end;
    }
}

/* Free the array, without dealing with individual elements */
static void carray_free(carray_t *arr) {
	if(arr != NULL) {
		// like free(), shouldn't explode if NULL
		carray_free_fn(arr);
	}
}

#define carray_clear(arr) do {                        \
    carray_s *carr = (carray_s *)arr;                 \
    carr->start = carr->end = 0;                      \
  } while(0)


/* Free the array, free individual element using callback */
#define carray_free_cb(arr, cb)                                         \
  ({                                                                   \
    if (arr) {                                                         \
        array_hdr_t *hdr = carray_hdr(arr);                             \
        uint32_t len = carray_len(arr);                                 \
        for (uint32_t i = hdr->start; i < len; i = (i + 1)%hdr->cap) { \
            { cb(arr[i]); }                                            \
        }                                                              \
      carray_free(arr);                                                 \
    }                                                                  \
  })

/* Pop the last element from the end of the array, reduce the size and return it */
#define carray_pop_back(arr)                                                    \
  __extension__({                                                               \
    array_hdr_t *hdr = carray_hdr(arr);                                          \
    assert(hdr->start != hdr->end);                                             \
    uint32_t old_end = hdr->end;                                                \
    if(old_end == 0) {                                                          \
        hdr->end = hdr->cap - 1;                                                \
    } else {                                                                    \
        hdr->end = hdr->end - 1;                                                \
    }                                                                           \
    hdr->end = (hdr->end + 1)%hdr->cap;                                         \
    *((arr) + old_start)                                                        \
  })

/* Remove a specified element from the array */
#define carray_del(arr, ix)                                                        \
  __extension__({                                                                              \
    assert(carray_len(arr) > ix);                                                  \
    if (carray_len(arr) - 1 > ix) {                                                \
      memcpy(arr + ix, arr + ix + 1, sizeof(*arr) * (carray_len(arr) - (ix + 1))); \
    }                                                                             \
    --carray_hdr(arr)->len;                                                        \
    arr;                                                                          \
  })

/* Remove a specified element from the array, but does not preserve order */
#define carray_del_fast(arr, ix)                \
  __extension__({                                           \
    if (carray_len((arr)) > 1) {                \
      (arr)[ix] = (arr)[carray_len((arr)) - 1]; \
    }                                          \
    --carray_hdr((arr))->len;                   \
    arr;                                       \
  })

/* Duplicate the array to the pointer dest. */
#define carray_clone(dest, arr)                            \
  __extension__({                                                      \
   dest = carray_newlen(typeof(*arr), carray_len(arr));     \
   memcpy(dest, arr, sizeof(*arr) * (carray_len(arr)));    \
  })

#endif

#pragma GCC diagnostic pop

#ifdef __cplusplus
}
#endif
#endif
