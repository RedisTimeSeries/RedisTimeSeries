#ifndef REDIS_TIMESERIES_LOAD_IO_ERROR_MACROS_H
#define REDIS_TIMESERIES_LOAD_IO_ERROR_MACROS_H

#define DEFER_(F, V)                                                                               \
    auto inline __attribute__((__always_inline__)) void F(int *);                                  \
    int V __attribute__((__cleanup__(F)));                                                         \
    void F(__attribute__((__unused__)) int *_)
#define DEFERER(F, L, C) DEFER_(DEFER_FUNC_##F##L##C, DEFER_VAR_##F##L##C)
#define DEFERER_(F, L, C) DEFERER(F, L, C)
#define defer DEFERER_(__FILE__, __LINE__, __COUNTER__)
#define errdefer(err, ...)                                                                         \
    defer {                                                                                        \
        if (unlikely(err))                                                                         \
            __VA_ARGS__;                                                                           \
    }

#define LoadDouble_IOError(rdb, is_err, ret)                                                       \
    __extension__({                                                                                \
        double res = RedisModule_LoadDouble((rdb));                                                \
        if (RedisModule_IsIOError(rdb)) {                                                          \
            is_err = true;                                                                         \
            return ret;                                                                            \
        }                                                                                          \
        res;                                                                                       \
    })

#define LoadUnsigned_IOError(rdb, is_err, ret)                                                     \
    __extension__({                                                                                \
        uint64_t res = RedisModule_LoadUnsigned((rdb));                                            \
        if (RedisModule_IsIOError(rdb)) {                                                          \
            is_err = true;                                                                         \
            return ret;                                                                            \
        }                                                                                          \
        res;                                                                                       \
    })

#define LoadString_IOError(rdb, is_err, ret)                                                       \
    __extension__({                                                                                \
        RedisModuleString *res = RedisModule_LoadString((rdb));                                    \
        if (RedisModule_IsIOError(rdb)) {                                                          \
            is_err = true;                                                                         \
            return ret;                                                                            \
        }                                                                                          \
        res;                                                                                       \
    })

#define LoadStringBuffer_IOError(rdb, len, is_err, ret)                                            \
    __extension__({                                                                                \
        char *res = RedisModule_LoadStringBuffer((rdb), (len));                                    \
        if (RedisModule_IsIOError(rdb)) {                                                          \
            is_err = true;                                                                         \
            return ret;                                                                            \
        }                                                                                          \
        res;                                                                                       \
    })

#endif // REDIS_TIMESERIES_LOAD_IO_ERROR_MACROS_H
