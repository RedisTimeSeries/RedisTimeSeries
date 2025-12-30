/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef SHORT_READ_H
#define SHORT_READ_H

void Backup_Globals();

void Restore_Globals();

void Discard_Globals_Backup();

#endif /* SHORT_READ_H */
