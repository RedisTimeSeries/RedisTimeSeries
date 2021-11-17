#ifndef REDIS_TIMESERIES_RTS_LOCK_H
#define REDIS_TIMESERIES_RTS_LOCK_H

#include <pthread.h>

int IsMRCluster();

typedef struct {
    pthread_rwlock_t rwlock;
    int writer_count;
} rts_rwlock;


extern rts_rwlock rts_rwlock_g;

// We are using recursive because writes are serialized and synchronized by redis (libmr support only reads).
// We can implement this cheaply, the need for the is load_rdb which can be called when
// the lock is already taken on replication scenario when the Backup_Globals() is being called before.
static inline void RTS_LockWriteRecursive() {
    // Only on cluster mode we have multithreaded
    if(!IsMRCluster() || rts_rwlock_g.writer_count > 0) {
        return;
    }

    pthread_rwlock_wrlock(&rts_rwlock_g.rwlock);
    rts_rwlock_g.writer_count++;
}

static inline void RTS_UnlockWrite() {
    if(!IsMRCluster()) {     // Only on cluster mode we have multithreaded
        return;
    }

    rts_rwlock_g.writer_count--;
    if(rts_rwlock_g.writer_count == 0) {
        pthread_rwlock_unlock(&rts_rwlock_g.rwlock);
    }
}

static inline void RTS_LockRead() {
    if(!IsMRCluster()) {     // Only on cluster mode we have multithreaded
        return;
    }

    pthread_rwlock_rdlock(&rts_rwlock_g.rwlock);
}

static inline void RTS_UnlockRead() {
    if(!IsMRCluster()) {     // Only on cluster mode we have multithreaded
        return;
    }
    pthread_rwlock_unlock(&rts_rwlock_g.rwlock);
}

int init_rts_rwlock_g();

#endif // REDIS_TIMESERIES_RTS_LOCK_H