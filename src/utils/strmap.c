/*
 * strmap.c - a tiny string->void* hash map for internal module use.
 *
 * Open addressing + linear probing. Keys are owned (malloc'd).
 */
#include "strmap.h"

#include <stdlib.h>
#include <string.h>

#define STRMAP_EMPTY 0
#define STRMAP_FILLED 1
#define STRMAP_TOMB 2

static unsigned strhash_fnv1a(const char *s) {
    unsigned h = 2166136261u;
    for (const unsigned char *p = (const unsigned char *)s; *p; ++p) {
        h ^= (unsigned)(*p);
        h *= 16777619u;
    }
    return h ? h : 1u;
}

static size_t next_pow2(size_t x) {
    size_t p = 1;
    while (p < x) {
        p <<= 1;
    }
    return p;
}

static int strmap_rehash(StrMap *m, size_t new_cap) {
    StrMapEnt *old = m->ents;
    size_t old_cap = m->cap;

    StrMapEnt *ents = calloc(new_cap, sizeof(*ents));
    if (!ents) {
        return -1;
    }

    m->ents = ents;
    m->cap = new_cap;
    m->len = 0;
    m->tomb = 0;

    for (size_t i = 0; i < old_cap; ++i) {
        if (old[i].state != STRMAP_FILLED) {
            continue;
        }
        /* Reinsert without copying key. */
        const char *key = old[i].key;
        void *val = old[i].val;
        unsigned h = old[i].hash;

        size_t mask = new_cap - 1;
        size_t idx = (size_t)h & mask;
        while (m->ents[idx].state == STRMAP_FILLED) {
            idx = (idx + 1) & mask;
        }
        m->ents[idx].key = (char *)key;
        m->ents[idx].val = val;
        m->ents[idx].hash = h;
        m->ents[idx].state = STRMAP_FILLED;
        m->len++;
    }

    free(old);
    return 0;
}

void StrMap_Init(StrMap *m) {
    m->ents = NULL;
    m->cap = 0;
    m->len = 0;
    m->tomb = 0;
}

void StrMap_Free(StrMap *m, void (*free_val)(void *)) {
    if (!m->ents) {
        return;
    }
    for (size_t i = 0; i < m->cap; ++i) {
        if (m->ents[i].state != STRMAP_FILLED) {
            continue;
        }
        free(m->ents[i].key);
        if (free_val) {
            free_val(m->ents[i].val);
        }
    }
    free(m->ents);
    m->ents = NULL;
    m->cap = 0;
    m->len = 0;
    m->tomb = 0;
}

size_t StrMap_Len(const StrMap *m) {
    return m->len;
}

static int strmap_ensure(StrMap *m) {
    if (!m->ents) {
        size_t cap = 256;
        m->ents = calloc(cap, sizeof(*m->ents));
        if (!m->ents) {
            return -1;
        }
        m->cap = cap;
        return 0;
    }
    /* keep load factor under ~0.7 including tombstones */
    if ((m->len + m->tomb) * 10 >= m->cap * 7) {
        size_t new_cap = m->cap ? (m->cap * 2) : 256;
        new_cap = next_pow2(new_cap);
        return strmap_rehash(m, new_cap);
    }
    return 0;
}

int StrMap_Get(const StrMap *m, const char *key, void **out_val) {
    if (!m->ents || m->cap == 0) {
        return 0;
    }
    unsigned h = strhash_fnv1a(key);
    size_t mask = m->cap - 1;
    size_t idx = (size_t)h & mask;
    for (size_t n = 0; n < m->cap; ++n) {
        StrMapEnt *e = &m->ents[idx];
        if (e->state == STRMAP_EMPTY) {
            return 0;
        }
        if (e->state == STRMAP_FILLED && e->hash == h && strcmp(e->key, key) == 0) {
            if (out_val) {
                *out_val = e->val;
            }
            return 1;
        }
        idx = (idx + 1) & mask;
    }
    return 0;
}

int StrMap_Set(StrMap *m, const char *key, void *val, void (*free_old_val)(void *)) {
    if (strmap_ensure(m) != 0) {
        return -1;
    }
    unsigned h = strhash_fnv1a(key);
    size_t mask = m->cap - 1;
    size_t idx = (size_t)h & mask;
    ssize_t tomb_idx = -1;

    for (size_t n = 0; n < m->cap; ++n) {
        StrMapEnt *e = &m->ents[idx];
        if (e->state == STRMAP_EMPTY) {
            if (tomb_idx >= 0) {
                e = &m->ents[(size_t)tomb_idx];
                m->tomb--;
            }
            e->key = strdup(key);
            e->val = val;
            e->hash = h;
            e->state = STRMAP_FILLED;
            m->len++;
            return 0;
        }
        if (e->state == STRMAP_TOMB) {
            if (tomb_idx < 0) {
                tomb_idx = (ssize_t)idx;
            }
        } else if (e->hash == h && strcmp(e->key, key) == 0) {
            if (free_old_val) {
                free_old_val(e->val);
            }
            e->val = val;
            return 0;
        }
        idx = (idx + 1) & mask;
    }
    return -1;
}

int StrMap_Del(StrMap *m, const char *key, void (*free_val)(void *)) {
    if (!m->ents || m->cap == 0) {
        return 0;
    }
    unsigned h = strhash_fnv1a(key);
    size_t mask = m->cap - 1;
    size_t idx = (size_t)h & mask;
    for (size_t n = 0; n < m->cap; ++n) {
        StrMapEnt *e = &m->ents[idx];
        if (e->state == STRMAP_EMPTY) {
            return 0;
        }
        if (e->state == STRMAP_FILLED && e->hash == h && strcmp(e->key, key) == 0) {
            free(e->key);
            if (free_val) {
                free_val(e->val);
            }
            e->key = NULL;
            e->val = NULL;
            e->hash = 0;
            e->state = STRMAP_TOMB;
            m->len--;
            m->tomb++;
            return 1;
        }
        idx = (idx + 1) & mask;
    }
    return 0;
}

const char *StrMapIter_Next(StrMap *m, StrMapIter *it, void **out_val) {
    if (!m->ents) {
        return NULL;
    }
    while (it->idx < m->cap) {
        StrMapEnt *e = &m->ents[it->idx++];
        if (e->state == STRMAP_FILLED) {
            if (out_val) {
                *out_val = e->val;
            }
            return e->key;
        }
    }
    return NULL;
}


