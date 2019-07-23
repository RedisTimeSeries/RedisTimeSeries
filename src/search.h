/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "../RediSearch/src/redisearch_api.h"
#include "../RediSearch/src/field_spec.h"
#include "indexer.h"

typedef struct {
    //  char *label;                // Indexed label.
    char **fields;              // Indexed fields.
    //  Attribute_ID *fields_ids;   // Indexed field IDs.
    uint32_t fields_count;          // Number of fields.
    RSIndex *idx;               // RediSearch full-text index.
} FullTextIndex;

typedef struct {
    uint32_t fieldlen;
    uint32_t strlen;
    char *field;
    char *str;
    double dbl;
    FieldType RSFieldType;
} RSLabels;

typedef uint32_t count_t;

/***** Modification functions *****/
int AddDoc(FullTextIndex *, char *key, uint32_t keylen, RSLabels *, count_t);

int AlterKey(FullTextIndex *, char *key, uint32_t keylen, RSLabels *, count_t);

int DeleteKey(FullTextIndex *, char *key, uint32_t keylen, RSLabels *, count_t);

/***** Modification functions *****/
char **QueryLabels(FullTextIndex *, RSLabels *, count_t);
