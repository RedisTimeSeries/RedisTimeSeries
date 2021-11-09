/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#ifndef SHORT_READ_H
#define SHORT_READ_H

void Backup_Globals();

void Restore_Globals();

void Discard_Globals_Backup();

#endif /* SHORT_READ_H */
