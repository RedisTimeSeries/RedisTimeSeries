/*
 * strmap.h - a tiny string->void* hash map for internal module use.
 *
 * We use this to keep a module-owned label index that can be accessed from
 * worker threads without calling RedisModule APIs (and therefore without
 * RedisModule_ThreadSafeContextLock()).
 */
#ifndef RTS_STRMAP_H
#define RTS_STRMAP_H

#include <stddef.h>

typedef struct StrMapEnt {
    char *key;         /* owned */
    void *val;
    unsigned hash;
    unsigned char state; /* 0=empty,1=filled,2=tomb */
} StrMapEnt;

typedef struct StrMap {
    StrMapEnt *ents;
    size_t cap;   /* power-of-two */
    size_t len;   /* filled entries */
    size_t tomb;  /* tombstones */
} StrMap;

typedef struct StrMapIter {
    size_t idx;
} StrMapIter;

void StrMap_Init(StrMap *m);
void StrMap_Free(StrMap *m, void (*free_val)(void *));
size_t StrMap_Len(const StrMap *m);

/* Returns 1 if found, 0 otherwise. */
int StrMap_Get(const StrMap *m, const char *key, void **out_val);

/* Insert or replace. Copies key. Returns 0 on success. */
int StrMap_Set(StrMap *m, const char *key, void *val, void (*free_old_val)(void *));

/* Delete. Returns 1 if deleted, 0 if not found. */
int StrMap_Del(StrMap *m, const char *key, void (*free_val)(void *));

/* Iterate over filled entries. Returns key or NULL when done. */
const char *StrMapIter_Next(StrMap *m, StrMapIter *it, void **out_val);

#endif /* RTS_STRMAP_H */


