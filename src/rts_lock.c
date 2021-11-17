
#include "rts_lock.h"

rts_rwlock rts_rwlock_g;

int init_rts_rwlock_g() {
    rts_rwlock_g.writer_count = 0;
    return pthread_rwlock_init(&rts_rwlock_g.rwlock, NULL);
}