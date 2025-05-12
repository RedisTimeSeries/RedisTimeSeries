#ifndef REDIS_TIMESERIES_LOAD_IO_ERROR_MACROS_H
#define REDIS_TIMESERIES_LOAD_IO_ERROR_MACROS_H

#define DEFER_(F, V)                                                                               \
    auto inline __attribute__((always_inline)) void F(int *);                                      \
    int V __attribute__((cleanup(F)));                                                             \
    void F(__attribute__((unused)) int *_)
#define DEFERER(N, M) DEFER_(DEFER_FUNC_##N##M, DEFER_VAR_##N##M)
#define DEFERER_(N, M) DEFERER(N, M)
#define defer DEFERER_(__LINE__, __COUNTER__)
#define errdefer(err, ...)                                                                         \
    defer {                                                                                        \
        if (err)                                                                                   \
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
