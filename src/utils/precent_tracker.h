/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#ifndef PRECENT_TRACKER_H
#define PRECENT_TRACKER_H

typedef struct precentTracker_s precentTracker_t;

precentTracker_t *precent_tracker_new(size_t cap);
int precent_tracker_add(precentTracker_t *pt, double val);
void precent_tracker_delete(precentTracker_t *pt, double val);
double precent_tracker_getMedian(precentTracker_t *pt);
void precent_tracker_RDBWrite(precentTracker_t *pt, RedisModuleIO *io);
int precent_tracker_RDBRead(precentTracker_t *pt, RedisModuleIO *io);

#endif /* PRECENT_TRACKER_H */
