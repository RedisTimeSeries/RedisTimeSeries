/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#ifndef UTIL_ARR_H_
#define UTIL_ARR_H_
/* circular_arr.h - simple, easy to use circular dynamic array with fast pointers,
 * to allow native access to members. It can accept pointers, struct literals and scalars.
 *
 * Example usage:
 *
 *  int *arr = array_new(int, 8);
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

#ifdef __cplusplus
extern "C" {
#endif

/* Definition of malloc & friedns that can be overridden before including arr.h.
 * Alternatively you can include arr_rm_alloc.h, which wraps arr.h and sets the allcoation functions
 * to those of the RM_ family
 */
#ifndef array_alloc_fn
#define array_alloc_fn malloc
#define array_realloc_fn realloc
#define array_free_fn free
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"

typedef struct {
    uint32_t start; // start index
    uint32_t end;   // end index
	uint32_t cap;
	uint32_t elem_sz;
	char buf[];
} carray_s;

typedef carray_s carray_t;
/* Internal - calculate the array size for allocations */
#define array_sizeof(carr) (sizeof(carray_s) + (uint64_t)carr->cap * carr->elem_sz)

static inline uint32_t array_len(carray_t *arr);

/* Initialize a new array with a given element size and capacity. Should not be used directly - use
 * array_new instead */
static carray_t *array_new_sz(uint32_t elem_sz, uint32_t cap, uint32_t len) {
	uint64_t array_size = (uint64_t)cap * elem_sz;
	carray_s *arr = (carray_s *)array_alloc_fn(sizeof(carray_s) + array_size);
	arr->cap = cap;
	arr->elem_sz = elem_sz;
    arr->start = 0;
    arr->end = MAX((int32_t)len - (int32_t)1, (int32_t)0);
	return arr;
}

/* Functions declared as symbols for use in debugger */
void carray_debug(void *pp);

/* Initialize an carray for a given type T with a given capacity and zero length.
 *  */
#define array_new(T, cap) (array_new_sz(sizeof(T), cap, 0))

/* Initialize an carray for a given type T with a given length. The capacity allocated is identical
 * to the length
 *  */
#define array_newlen(T, len) (array_new_sz(sizeof(T), len, len))

static inline void array_ensure_cap(carray_t **arr, uint32_t cap) {
    carray_s *carr = (carray_s *)*arr;
	if(cap > carr->cap) {
        uint32_t old_cap = carr->cap;
		carr->cap = MAX(carr->cap * 2, cap);
		*arr = carr = (carray_s *)array_realloc_fn(carr, array_sizeof(carr));

        if(carr->start > carr->end) {
            uint32_t diff = cap - old_cap;
            memmove(carr->buf + (carr->start+diff)*carr->elem_sz, carr->buf + (carr->start*carr->elem_sz), (old_cap - carr->start)*carr->elem_sz); \
            carr->start += diff;
        }
	}

	return;
}

/* Ensure capacity for the array to grow by one */
static inline void array_grow(carray_t **arr, size_t n) {
    carray_s *carr = (carray_s *)*arr;
    uint32_t new_len = array_len(*arr) + n;
	array_ensure_cap(arr, new_len);
    carr->end = (carr->end + n)%carr->cap;
    return;
}

static inline void array_ensure_len(carray_t **arr, size_t len) {
	if(len <= array_len(*arr)) {
		return;
	}
	len -= array_len(*arr);
	array_grow(arr, len);
}

/* Ensures that array_tail will always point to a valid element. */
#define array_ensure_tail(arrpp, T)            \
  ({                                           \
    if (!*(arrpp)) {                           \
      *(arrpp) = array_newlen(T, 1);           \
    } else {                                   \
      array_grow((arrpp), 1);                  \
    }                                          \
    &(array_tail(*(arrpp)));                   \
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
#define array_ensure_append(arrpp, src, n, T) do {                \
    uint32_t old_start = 0;                                       \
    uint32_t old_end = 0;                                         \
    carray_s *carr;                                               \
    if (!arr) {                                                   \
        *(arrpp) = carr = array_newlen(T, n);                     \
    } else {                                                      \
        carr = (carray_s *)&arrpp;                                \
        old_start = carr->start;                                  \
        old_end = carr->end;                                      \
        array_grow(arrpp, n);                                     \
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
#define array_ensure_prepend(arr, src, n, T) do {                                          \
    uint32_t old_start = 0;                                                                \
    uint32_t old_end = 0;                                                                  \
    carray_s *carr = (carray_s *)arr;                                                     \
    if (!arr) {                                                                            \
        carr = (carray_s *)array_newlen(T, n);                                             \
    } else {                                                                               \
        old_start = carr->start;                                                           \
        old_end = carr->end;                                                               \
        array_grow(arr, n);                                                                \
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
#define array_ensure_at(arrpp, pos, T)                                    \
  ({                                                                      \
    if (!(*(arrpp))) {                                                    \
      *(arrpp) = array_new(T, 1);                                         \
    }                                                                     \
    carray_s carr = (carray_s)*(arrpp);                                   \
    int32_t old_len = array_len(carr));                                   \
    if (old_len <= (pos)) {                                               \
        uint32_t old_start = carr.start;                                 \
        uint32_t old_end = carr.end;                                     \
        array_grow(*(arrpp), (pos) + 1);                                  \
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

/* get the last element in the array */
#define array_tail(arr) ((carray_s *)(arr)->buf[(carray_s *)(arr)->end - 1])

/* Append an element to the array, returning the array which may have been reallocated */
#define array_append(arr, x) do {                                                 \
    carray_s *carr = (carray_s *)arr;                                             \
    if((carr->end + 1)%carr->cap == carr->start) {                                   \
        array_grow((arr), 1);                                                        \
        carr = (carray_s *)arr;                                             \
    } else {                                                                      \
        carr->end = (carr->end + 1)%carr->cap;                                       \
    }                                                                             \
    array_tail((arr)) = (x);                                                      \
  } while(0)

/* get the element at index x, assumes that the array has len which is larger than x */
#define array_at(arr, x, T)                                                     \
({                                                                              \
    carray_s *carr = (carray_s *)arr;                                           \
    *((T *)carr->buf + ((carr->start + x)%carr->cap));                          \
})

/* get pointer to the element at index x, assumes that the array has len which is larger than x */
#define array_at_ptr(arr, x, T)                                                 \
({                                                                              \
    carray_s *carr = (carray_s *)arr;                                           \
    (T *)carr->buf + ((carr->start + x)%carr->cap));                            \
})

/* Get the length of the array */
static inline uint32_t array_len(carray_t *arr) {
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
static void array_free(carray_t arr) {
	if(arr != NULL) {
		// like free(), shouldn't explode if NULL
		array_free_fn(arr);
	}
}

#define array_clear(arr) array_hdr(arr)->start = array_hdr(arr)->end = 0

/* Repeate the code in "blk" for each element in the array, and give it the name of "as".
 * e.g:
 *  int *arr = array_new(int, 10);
 *  array_append(arr, 1);
 *  array_foreach(arr, i, printf("%d\n", i));
 */
#define array_foreach(arr, as, blk)                                \
  ({                                                               \
    array_hdr_t *hdr = array_hdr(arr);                             \
    uint32_t len = array_len(arr);                                 \
    for (uint32_t i = hdr->start; i < len; i = (i + 1)%hdr->cap) { \
      __typeof__(*arr) as = arr[i];                                \
      blk;                                                         \
    }                                                              \
  })

/* Free the array, free individual element using callback */
#define array_free_cb(arr, cb)                                         \
  ({                                                                   \
    if (arr) {                                                         \
        array_hdr_t *hdr = array_hdr(arr);                             \
        uint32_t len = array_len(arr);                                 \
        for (uint32_t i = hdr->start; i < len; i = (i + 1)%hdr->cap) { \
            { cb(arr[i]); }                                            \
        }                                                              \
      }                                                                \
      array_free(arr);                                                 \
    }                                                                  \
  })

/* Pop the top element from the end of the array, reduce the size and return it */
#define array_pop_front(arr)                                                    \
    array_hdr_t *hdr = array_hdr(arr);                                          \
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

/* Remove element from the beginning of the array if not empty, reduce the size and return it */
#define array_pop_back(arr)                                                     \
({                                                                              \
    array_hdr_t *hdr = array_hdr(arr);                                          \
    assert(hdr->start != hdr->end);                                             \
    uint32_t old_start = hdr->start;                                            \
    hdr->start = (hdr->start + 1)%hdr->cap;                                     \
    *((arr) + old_start)                                                        \
})

/* Remove a specified element from the array */
#define array_del(arr, ix)                                                        \
  __extension__({                                                                              \
    assert(array_len(arr) > ix);                                                  \
    if (array_len(arr) - 1 > ix) {                                                \
      memcpy(arr + ix, arr + ix + 1, sizeof(*arr) * (array_len(arr) - (ix + 1))); \
    }                                                                             \
    --array_hdr(arr)->len;                                                        \
    arr;                                                                          \
  })

/* Remove a specified element from the array, but does not preserve order */
#define array_del_fast(arr, ix)                \
  __extension__({                                           \
    if (array_len((arr)) > 1) {                \
      (arr)[ix] = (arr)[array_len((arr)) - 1]; \
    }                                          \
    --array_hdr((arr))->len;                   \
    arr;                                       \
  })

/* Duplicate the array to the pointer dest. */
#define array_clone(dest, arr)                            \
  __extension__({                                                      \
   dest = array_newlen(typeof(*arr), array_len(arr));     \
   memcpy(dest, arr, sizeof(*arr) * (array_len(arr)));    \
  })

#pragma GCC diagnostic pop

#ifdef __cplusplus
}
#endif
#endif
