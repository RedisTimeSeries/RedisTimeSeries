#ifndef REDIS_TIMESERIES_LOAD_IO_ERROR_MACROS_H
#define REDIS_TIMESERIES_LOAD_IO_ERROR_MACROS_H


#define LoadDouble_IOError(rdb, cleanup_exp)                                                       \
    __extension__({                                                                                \
        double res = RedisModule_LoadDouble((rdb));                                                \
        if (RedisModule_IsIOError(rdb)) {                                                          \
            cleanup_exp;                                                                           \
        }                                                                                          \
        (res);                                                                                     \
    })

#define LoadUnsigned_IOError(rdb, cleanup_exp)                                                     \
    __extension__({                                                                                \
        uint64_t res = RedisModule_LoadUnsigned((rdb));                                            \
        if (RedisModule_IsIOError(rdb)) {                                                          \
            cleanup_exp;                                                                           \
        }                                                                                          \
        (res);                                                                                     \
    })

#define LoadString_IOError(rdb, cleanup_exp)                                                       \
    __extension__({                                                                                \
        RedisModuleString *res = RedisModule_LoadString((rdb));                                    \
        if (RedisModule_IsIOError(rdb)) {                                                          \
            cleanup_exp;                                                                           \
        }                                                                                          \
        (res);                                                                                     \
    })

#define LoadStringBuffer_IOError(rdb, str, cleanup_exp)                                            \
    __extension__({                                                                                \
        char *res = RedisModule_LoadStringBuffer((rdb), (str));                                    \
        if (RedisModule_IsIOError(rdb)) {                                                          \
            cleanup_exp;                                                                           \
        }                                                                                          \
        (res);                                                                                     \
    })


#endif //REDIS_TIMESERIES_LOAD_IO_ERROR_MACROS_H