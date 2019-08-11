#include <stdbool.h>
#include <string.h>
#include <assert.h>

#include "search.h"
#include "config.h"
#include "indexer.h"
#include "rmutil/alloc.h"
#include "rmutil/util.h"

//#include "geo_index.h"

#define max(a,b) \
   ({ __typeof__ (a) _a = (a); \
      __typeof__ (b) _b = (b); \
      _a > _b ? _a : _b; })

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
static void VerifyAddField(RSLiteIndex *fti, const char *field, uint32_t fieldlen) {
    // TODO : change to AVL
    if (CheckFieldExist(fti, field, fieldlen) == false) {
        if (fti->fields_count % 64 == 0) {
            if (fti->fields_count == 0) {
                fti->fields = calloc(64, sizeof(char *));
            } else {
                fti->fields = realloc(fti->fields, sizeof(char *) * (fti->fields_count + 64));
            }
        }
        RSField *rsf = RediSearch_CreateField (fti->idx, field, 
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

int RSL_Index(RSLiteIndex *fti, const char *item, uint32_t itemlen, 
              RSLabel *labels, count_t count) {
    RSDoc *doc = RediSearch_CreateDocument(item, itemlen, 1, 0);

    for(count_t i = 0; i < count; ++i) {
        size_t fieldLen = 0;
        size_t valueLen = 0;
        const char *fieldStr = RedisModule_StringPtrLen(labels[i].RTS_Label.key, &fieldLen);
        const char *valueStr = RedisModule_StringPtrLen(labels[i].RTS_Label.value, &valueLen);
        VerifyAddField(fti, fieldStr, fieldLen);
            
        if (labels[i].RSFieldType == RSFLDTYPE_NUMERIC) {            
            RediSearch_DocumentAddFieldNumber(doc, fieldStr,
                                labels[i].value.dbl, RSFLDTYPE_NUMERIC);
            printf("NUMERIC\n");
        } else if (labels[i].RSFieldType == RSFLDTYPE_FULLTEXT) {
            RediSearch_DocumentAddFieldString(doc, fieldStr,
                                valueStr, valueLen, RSFLDTYPE_FULLTEXT);
            printf("FULL\n");
        } else if (labels[i].RSFieldType == RSFLDTYPE_TAG) {
            RediSearch_DocumentAddFieldString(doc, fieldStr,
                                valueStr, valueLen, RSFLDTYPE_TAG);
            printf("TAG\n");
        } else if (labels[i].RSFieldType == RSFLDTYPE_GEO) {
            printf("GEO\n");
            return REDISMODULE_ERR; // TODO error
        } else {
            printf("ELSE\n");
            return REDISMODULE_ERR; // TODO error
        }
    }
    RediSearch_SpecAddDocument(fti->idx, doc); // Always use ADD_REPLACE for simplicity
    return REDISMODULE_OK;
}

int RSL_Remove(RSLiteIndex *fti, const char *item, uint32_t itemlen) {
    return RediSearch_DeleteDocument(fti->idx, item, itemlen);
}

RSResultsIterator *RSL_GetQueryFromString(RSLiteIndex *fti, const char *s, size_t n, char **err) {
    printf("Query string : %s\n", s);
  return RediSearch_IterateQuery(fti->idx, s, n, err);
}

RSResultsIterator *RSL_GetQueryFromNode(RSLiteIndex *fti, RSQNode *queryNode) {
  return RediSearch_GetResultsIterator(queryNode, fti->idx);
}

const char *RSL_IterateResults(RSResultsIterator *iter, size_t *len) {
    return RediSearch_ResultsIteratorNext(iter, TSGlobalConfig.globalRSIndex->idx, len);
}

int RSL_AppendTerm(RedisModuleString *RM_item, char **query, size_t *queryLoc) {
    // field=v1         @field:v1
    // field=(v1,v2)    @field:(v1|v2)
    // field!=v1       -@field:v1
    // field!=(v1,v2)  -@field:(v1|v2)
    // field=          -@field:*
    // field!=          @field:*
    size_t itemLen = 0;
    bool addAsterisk = false;
    const char *item = RedisModule_StringPtrLen(RM_item, &itemLen);

    if ((*queryLoc + itemLen + 1 + 1) / QUERY_EXP == 1) {
        *query = realloc(*query, ((*queryLoc / QUERY_EXP) + 1) * QUERY_EXP);
    }  

    char *equalPtr = strstr(item, "=");
    if(equalPtr == NULL) { return REDISMODULE_ERR; }

    if(*(equalPtr + 1) == '\0') { 
        addAsterisk = true;
    }

    if ((*(equalPtr - 1) == '!' && *(equalPtr + 1) != '\0') ||
        (*(equalPtr - 1) != '!' && *(equalPtr + 1) == '\0')) {
        (*query)[(*queryLoc)++] = '-';        
    }

    (*query)[(*queryLoc)++] = '@';

    for (size_t i = 0; i < itemLen; ++i) {
        switch (item[i])
        {
        case '!':
            break;
        case '=':
            (*query)[(*queryLoc)++] = ':';
            break;
        case ',':
            (*query)[(*queryLoc)++] = '|';
            break;        
        default:
            (*query)[(*queryLoc)++] = item[i];
            break;
        }
    }
    if (addAsterisk) {
        (*query)[(*queryLoc)++] = '*';
    }
    (*query)[(*queryLoc)++] = ' ';
    return REDISMODULE_OK;
}

int RSL_RSQueryFromTSQuery(RedisModuleString **argv, int start, 
                        char **queryStr, size_t *queryLen, int query_count) {
    for(size_t i = start; i < query_count + start; ++i) {
        if (RSL_AppendTerm(argv[i], queryStr, queryLen) != REDISMODULE_OK) {
            return REDISMODULE_ERR;
        }
    }
    (*queryStr)[--(*queryLen)] = '\0';
    return REDISMODULE_OK;
}
/*
int RSL_CreateQuery(RedisModuleCtx *ctx, RedisModuleString **argv, int start,
                                            int query_count, RSQNode **tree) {
    QueryPredicate *predList = RedisModule_PoolAlloc(ctx, sizeof(QueryPredicate) * query_count);
    if (parseLabelListFromArgs(ctx, argv, start, query_count, predList) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    RSQNode *negativeNode = NULL;

    *tree = RediSearch_CreateIntersectNode(TSGlobalConfig.globalRSIndex, 0);
    for(size_t i = 0; i < query_count; ++i) {
        if (predList[i].valueListCount == 1) {
            size_t fieldLen = 0, tokenLen = 0;
            char *field = RedisModule_StringPtrLen(predList[i].key, &fieldLen);
            char *token = RedisModule_StringPtrLen(predList[i].valuesList[0], &tokenLen);
            RSQNode *temp = RediSearch_CreateTokenNode(TSGlobalConfig.globalRSIndex, field, token);
            RediSearch_QueryNodeAddChild(tree, temp);
        } else {
            for(size_t j = 0; j < predList[i].valueListCount; ++j) {
            }
        }
    }

    if (query_count == 1) {
        if (predList->valueListCount == 1) { 
            size_t fieldLen = 0, tokenLen = 0;
            char *field = RedisModule_StringPtrLen(predList->key, &fieldLen);
            char *token = RedisModule_StringPtrLen(predList->valuesList, &tokenLen);
            *tree = RediSearch_CreateTokenNode(TSGlobalConfig.globalRSIndex, field, token);
            return REDISMODULE_OK;
        } else {
            for(si)
        }
    }

}*/

/*
RSResultsIterator *RSL_GetQueryIter(RedisModuleCtx *ctx, RSLiteIndex *fti, RedisModuleString **argv, int start, int query_count) {
    size_t itemLen = 0;
    const char *curItem = RedisModule_StringPtrLen(argv[start], itemLen);
    if (strstr(curItem, "@") != NULL) {
        return RSL_GetQueryFromString(fti, curItem, itemLen, NULL);
    }

    QueryPredicate *query = NULL; 
    
    parsePredicate(ctx, argv[start], query, )

    RSQNode *head = NULL; 
    if (query_count == 1) {
        double dbl;
        if (RedisModule_StringToDouble(argv[start], &dbl) == REDISMODULE_OK) {
            head = RediSearch_CreateNumericNode(fti->idx, )
        }
    }


}*/




/*
int parsePredicate(RedisModuleCtx *ctx, RedisModuleString *label, QueryPredicate *retQuery, const char *separator) {


    char *token;
    char *iter_ptr;
    size_t _s;
    const char *labelRaw = RedisModule_StringPtrLen(label, &_s);
    char *labelstr = RedisModule_PoolAlloc(ctx, _s + 1);
    labelstr[_s] = '\0';
    strncpy(labelstr, labelRaw, _s);

    // Extract key
    token = strtok_r(labelstr, separator, &iter_ptr);
    if (token == NULL) {
        return TSDB_ERROR;
    }
    retQuery->key = RedisModule_CreateString(ctx, token, strlen(token));

    // Extract value
    token = strtok_r(NULL, separator, &iter_ptr);
    if (strstr(separator, "=(") != NULL) {
        if (token == NULL) {
            return TSDB_ERROR;
        }
        size_t token_len = strlen(token);

        if (token[token_len - 1] == ')') {
            token[token_len - 1] = '\0'; // remove closing parentheses
        } else {
            return TSDB_ERROR;
        }

        int filterCount = 0;
        for (int i=0; token[i]!='\0'; i++) {
            if (token[i] == ',') {
                filterCount++;
            }
        }
        if (token_len <= 1) {
            // when the token is <=1 it means that we have an empty list
            retQuery->valueListCount = 0;
        } else {
            retQuery->valueListCount = filterCount  + 1;
        }
        retQuery->valuesList = RedisModule_PoolAlloc(ctx, retQuery->valueListCount * sizeof(RedisModuleString*));

        char* subToken = strtok_r(token, ",", &iter_ptr);
        for (int i=0; i < retQuery->valueListCount; i++) {
            if (subToken == NULL) {
                continue;
            }
            retQuery->valuesList[i] = RedisModule_CreateStringPrintf(ctx, subToken);
            subToken = strtok_r(NULL, ",", &iter_ptr);
        }
    } else if (token != NULL) {
        retQuery->valueListCount = 1;
        retQuery->valuesList = RedisModule_PoolAlloc(ctx, sizeof(RedisModuleString*));
        retQuery->valuesList[0] = RedisModule_CreateString(ctx, token, strlen(token));
    } else {
        retQuery->valuesList = NULL;
        retQuery->valueListCount = 0;
    }
    return TSDB_OK;
}
*/

/****** Helper functions ******/
void FreeRSLabels(RSLabel *labels, size_t count, bool freeRMString) {
  for(size_t i = 0; i < count; ++i) {
    if (freeRMString)
    {
        RedisModule_FreeString(NULL, labels[i].RTS_Label.key);
        RedisModule_FreeString(NULL, labels[i].RTS_Label.value);
    }    
  }
  
  free(labels);
}

Label *RSLabelToLabels(RSLabel *labels, size_t count) {
    Label *newLabels = (Label *)calloc(count, sizeof(Label));
    for(size_t i = 0; i < count; ++i) {
        newLabels[i].key = labels[i].RTS_Label.key;
        newLabels[i].value = labels[i].RTS_Label.value;
    }
    return newLabels;
}

static int parseFieldType(RedisModuleString *str, FieldType *type) {
    size_t strLen = 0;
    const char *typeStr = RedisModule_StringPtrLen(str, &strLen);
    if (strLen != 3) { return REDISMODULE_ERR; }

    if (strncasecmp(typeStr, "TXT", 3)) {
        *type = RSFLDTYPE_FULLTEXT;
    } else if(strncasecmp(typeStr, "NUM", 3)) {
        *type = RSFLDTYPE_NUMERIC;
    } else if(strncasecmp(typeStr, "TAG", 3)) {
        *type = RSFLDTYPE_TAG;
    } else if(strncasecmp(typeStr, "GEO", 3)) {
        *type = RSFLDTYPE_GEO;
    } else {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

static int parseGeo(RedisModuleString *str, GeoFilter *geo) {
    // TODO
    return REDISMODULE_OK;
}


/*
 * Received in format [FIELDS field type value ...]
 */
int parseFieldsFromArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, size_t *label_count, RSLabel **rsLabels) {
    int pos = RMUtil_ArgIndex("FIELDS", argv, argc);
    if (pos < 0) { return REDISMODULE_OK; }

    RSLabel *labelsResult = NULL;
    int first_label_pos = pos + 1;
    // TODO : how to proceed with GEOs
    // Leaning towards "lon&lat&radius&m|km|mi|ft"
    *label_count = (size_t)(max(0, (argc - first_label_pos) / 3 ));
    if (label_count > 0) {
    	labelsResult = malloc(sizeof(RSLabel) * (*label_count));
        for (int i = 0; i < *label_count; i++) {
        	size_t *keyLen = 0;
            size_t *valueLen = 0;
        	RedisModuleString *key = argv[first_label_pos + i * 3];
        	RedisModuleString *type = argv[first_label_pos + i * 3 + 1];
        	RedisModuleString *value = argv[first_label_pos + i * 3 + 2];

            if(RedisModule_StringPtrLen(key, keyLen) == NULL || *keyLen == 0) {
                goto exitOnError; 
            }
            
            if(parseFieldType(type, &labelsResult[i].RSFieldType) != REDISMODULE_OK) {
        		goto exitOnError;
            }

            const char *ptr = NULL; // Can't declare after switch label.
            switch (labelsResult[i].RSFieldType) {
            case RSFLDTYPE_FULLTEXT:
            case RSFLDTYPE_TAG:
                ptr = RedisModule_StringPtrLen(value, valueLen);
                if(*valueLen == 0 || strpbrk(ptr, "(),=!")) {
                    goto exitOnError;
                }
                break;
            case RSFLDTYPE_NUMERIC:
                if (RedisModule_StringToDouble(value, &labelsResult[i].value.dbl) != REDISMODULE_OK) {
                    goto exitOnError;
                }                
                break;
            case RSFLDTYPE_GEO:
                if (parseGeo(value, labelsResult[i].value.geo) != REDISMODULE_OK) {
                    goto exitOnError;
                }                
                break;
            default:
                return REDISMODULE_ERR;
                break;
            }

            // These RM_strings are for Series->labels
            labelsResult[i].RTS_Label.key = 
                RedisModule_CreateStringFromString(NULL, key);
        	labelsResult[i].RTS_Label.value =
                RedisModule_CreateStringFromString(NULL, value);
        }
    }
    *rsLabels = labelsResult;
    return REDISMODULE_OK;

exitOnError:
    FreeRSLabels(labelsResult, *label_count, true); // need to release prior key values too
    labelsResult = NULL;
    return REDISMODULE_ERR;  
}
