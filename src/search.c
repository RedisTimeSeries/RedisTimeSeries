#include <stdbool.h>
#include <string.h>
#include <assert.h>

#include "search.h"
//#include "geo_index.h"

/***** Static functions *****/
static bool CheckFieldExist(RSLiteIndex *fti, const char *field, uint32_t fieldlen) {
    assert(fti);
    assert(field);

    for(uint32_t i = 0; i < fti->fields_count; ++i) {
        if (strncmp(field, fti->fields[i], fieldlen) == 0) {
            return true;
        }
    }
    return false;
} 

// Checks whether a field exists and if not, creates it
static void VerifyAddField(RSLiteIndex *fti, char *field, uint32_t fieldlen) {
    // TODO : change to AVL
    if (CheckFieldExist(fti, field, fieldlen) == false) {
        if (fti->fields_count % 64 == 0) {
            if (fti->fields_count == 0) {
                fti->fields = calloc(64, sizeof(char *));
            } else {
                fti->fields = realloc(fti->fields, sizeof(char *) * (fti->fields_count + 10));
            }
        }
        RediSearch_CreateField (fti->idx, field, 
                                RSFLDTYPE_FULLTEXT | RSFLDTYPE_NUMERIC | RSFLDTYPE_TAG,
                                RSFLDOPT_NONE);   
        fti->fields[fti->fields_count++] = RedisModule_Strdup(field);
        // Ensure retention of string
    }
}

/***** Modification functions *****/
RSLiteIndex *RSLiteCreate(const char *name) {
    RSLiteIndex *fti = (RSLiteIndex *)calloc(1, sizeof(RSLiteIndex));

    fti->fields = NULL; // TODO will probably changed once AVL is added
    fti->fields_count = 0;
    fti->idx = RediSearch_CreateIndex(name, NULL);

    return fti;
}

int AddDoc (RSLiteIndex *fti, char *item, uint32_t itemlen, 
            RSLabel *labels, count_t count) {
    RSDoc *doc = RediSearch_CreateDocument(item, itemlen, 1, 0);

    for(count_t i = 0; i < count; ++i) {
        VerifyAddField(fti, labels[i].fieldStr, labels[i].fieldLen);
            
        if (labels[i].RSFieldType == INDEXFLD_T_NUMERIC) {            
            RediSearch_DocumentAddFieldNumber(doc, labels[i].fieldStr,
                                labels[i].dbl, RSFLDTYPE_NUMERIC);
        } else if (labels[i].RSFieldType == INDEXFLD_T_FULLTEXT) {
            RediSearch_DocumentAddFieldString(doc, labels[i].fieldStr,
                                labels[i].valueStr, labels[i].valueLen, RSFLDTYPE_FULLTEXT);
        } else if (labels[i].RSFieldType == INDEXFLD_T_TAG) {
            RediSearch_DocumentAddFieldString(doc, labels[i].fieldStr,
                                labels[i].valueStr, labels[i].valueLen, RSFLDTYPE_TAG);
        } else if (labels[i].RSFieldType == INDEXFLD_T_GEO) {
            //  INDEXFLD_T_GEO = 0x04
            return REDISMODULE_ERR; // TODO error
        } else {
            return REDISMODULE_ERR; // TODO error
        }
    }
    RediSearch_SpecAddDocument(fti->idx, doc); // Always use ADD_REPLACE for simplicity
}

int DeleteLabels(RSLiteIndex *fti, char *item, uint32_t itemlen, 
                 RSLabel *labels, count_t count) {
    return RediSearch_DeleteDocument(fti->idx, item, itemlen);
}

RSResultsIterator *QueryString(RSLiteIndex *fti, const char *s, size_t n, char **err) {
  return RediSearch_IterateQuery(fti->idx, s, n, err);
}

void FreeRSLabels(RSLabel *labels, size_t count) {
  for(size_t i = 0; i < count; ++i) {
    free(labels[i].fieldStr);
    free(labels[i].valueStr);
  }
  
  free(labels);
}