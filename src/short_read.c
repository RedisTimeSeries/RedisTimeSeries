/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "indexer.h"
#include "rts_lock.h"

extern RedisModuleDict *labelsIndex;  // maps label to it's ts keys.
extern RedisModuleDict *tsLabelIndex; // maps ts_key to it's dict in labelsIndex

RedisModuleDict *labelsIndex_bkup;  // backup of labelsIndex
RedisModuleDict *tsLabelIndex_bkup; // backup of tsLabelIndex

void Backup_Globals() {
    RTS_LockWriteRecursive();
    labelsIndex_bkup = labelsIndex;
    tsLabelIndex_bkup = tsLabelIndex;

    IndexInit();
    RTS_UnlockWrite();
}

void Restore_Globals() {
    RTS_LockWriteRecursive();
    RemoveAllIndexedMetrics();

    RedisModule_FreeDict(NULL, labelsIndex);
    labelsIndex = labelsIndex_bkup;
    labelsIndex_bkup = NULL;

    RedisModule_FreeDict(NULL, tsLabelIndex);
    tsLabelIndex = tsLabelIndex_bkup;
    tsLabelIndex_bkup = NULL;
    RTS_UnlockWrite();
}

void Discard_Globals_Backup() {
    RemoveAllIndexedMetrics_generic(labelsIndex_bkup, &tsLabelIndex_bkup);

    RedisModule_FreeDict(NULL, labelsIndex_bkup);
    labelsIndex_bkup = NULL;

    RedisModule_FreeDict(NULL, tsLabelIndex_bkup);
    tsLabelIndex_bkup = NULL;
}
