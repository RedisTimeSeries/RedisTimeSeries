#include <stdbool.h>
#include <string.h>
#include <assert.h>

#include "search.h"

/***** Static functions *****/
static bool CheckFieldExist(FullTextIndex *fti, const char *field, uint32_t fieldlen) {
    assert(fti);
    assert(field);

    for(uint32_t i = 0; i < fti->fields_count; ++i) {
        if (strncmp(field, fti->fields[i], fieldlen) == 0) {
            return true;
        }
    }
    return false;
} 

static void VerifyAddField(FullTextIndex *fti, char *field, uint32_t fieldlen) {
    if (CheckFieldExist(fti, field, fieldlen) == false) {
        if (fti->fields_count % 64 == 0) {
            if (fti->fields_count == 0) {
                fti->fields = calloc(64, sizeof(char *));
            } else {
                fti->fields = realloc(fti->fields, sizeof(char *) * (fti->fields_count + 10));
            }
        }
        RediSearch_CreateField(fti->idx, field, 
            RSFLDTYPE_FULLTEXT | RSFLDTYPE_NUMERIC, RSFLDOPT_NONE);   
        fti->fields[fti->fields_count++] = RedisModule_Strdup(field);
        // Ensure retention of string
    }
}

/***** Modification functions *****/
int AddDoc(FullTextIndex *fti, char *key, uint32_t keylen, RSLabels *labels, count_t count) {
    RSDoc *doc = RediSearch_CreateDocument(key, keylen, 1, 0);

    for(count_t i = 0; i < count; ++i) {
        VerifyAddField(fti, labels[i].field, labels[i].fieldlen);
            
        if (labels[i].RSFieldType == INDEXFLD_T_NUMERIC) {            
            RediSearch_DocumentAddFieldNumber(doc, labels[i].field,
                                labels[i].dbl, RSFLDTYPE_NUMERIC);
        } else if (labels[i].RSFieldType == INDEXFLD_T_FULLTEXT) {
            RediSearch_DocumentAddFieldString(doc, labels[i].field,
                                labels[i].str, labels[i].strlen, RSFLDTYPE_FULLTEXT);
        } else {
            return REDISMODULE_ERR; // TODO error
        }
        RediSearch_SpecAddDocument(fti->idx, doc); // Always use ADD_REPLACE for simplicity
    }
}

int DeleteLabels(FullTextIndex *fti, char *key, uint32_t keylen, RSLabels *labels, count_t count) {
    RSDoc *doc = RediSearch_CreateDocument(key, keylen, 1, 0);
    for(count_t i = 0; i < count; ++i) {
        if (labels[i].RSFieldType == INDEXFLD_T_NUMERIC) {            
            RediSearch_DocumentAddFieldNumber(doc, labels[i].field,
                                labels[i].dbl, RSFLDTYPE_NUMERIC);
        } else if (labels[i].RSFieldType == INDEXFLD_T_FULLTEXT) {
            RediSearch_DocumentAddFieldString(doc, labels[i].field,
                                labels[i].str, labels[i].strlen, RSFLDTYPE_FULLTEXT);
        } else {
            return REDISMODULE_ERR; // TODO error
        }
    }
    RediSearch_DeleteDocument(fti->idx, key, keylen);
    return 0;
}

/***** Modification functions *****/
char **QueryLabels(FullTextIndex *fti, RSLabels *labels, count_t count) {

}
