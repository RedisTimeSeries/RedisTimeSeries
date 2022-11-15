/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */

#include "indexer.h"

extern RedisModuleDict *labelsIndex;  // maps label to it's ts keys.
extern RedisModuleDict *tsLabelIndex; // maps ts_key to it's dict in labelsIndex

RedisModuleDict *labelsIndex_bkup;  // backup of labelsIndex
RedisModuleDict *tsLabelIndex_bkup; // backup of tsLabelIndex

void Backup_Globals() {
    labelsIndex_bkup = labelsIndex;
    tsLabelIndex_bkup = tsLabelIndex;

    IndexInit();
}

void Restore_Globals() {
    RemoveAllIndexedMetrics();

    RedisModule_FreeDict(NULL, labelsIndex);
    labelsIndex = labelsIndex_bkup;
    labelsIndex_bkup = NULL;

    RedisModule_FreeDict(NULL, tsLabelIndex);
    tsLabelIndex = tsLabelIndex_bkup;
    tsLabelIndex_bkup = NULL;
}

void Discard_Globals_Backup() {
    RemoveAllIndexedMetrics_generic(labelsIndex_bkup, &tsLabelIndex_bkup);

    RedisModule_FreeDict(NULL, labelsIndex_bkup);
    labelsIndex_bkup = NULL;

    RedisModule_FreeDict(NULL, tsLabelIndex_bkup);
    tsLabelIndex_bkup = NULL;
}
