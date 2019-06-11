# BUILD redisfab/redistimeseries-${ARCH}-${OSNICK}:M.m.b

ARG OSNICK=bionic
# ARG ARCH=x64|arm64|arm7l [no need to specify: using multi-arch]

#----------------------------------------------------------------------------------------------
FROM redislabs/redis-${OSNICK}:5.0.5 AS builder

ADD ./ /build
WORKDIR /build

RUN ./deps/readies/bin/getpy2
RUN python ./system-setup.py

RUN make -C RedisModulesSDK/rmutil
RUN make -C src

#----------------------------------------------------------------------------------------------
FROM redislab/redis-${OSNICK}:5.0.5

ENV LIBDIR /usr/lib/redis/modules
WORKDIR /data
RUN mkdir -p "$LIBDIR"

COPY --from=builder /build/src/redistimeseries.so "$LIBDIR"

EXPOSE 6379
CMD ["redis-server", "--loadmodule", "/usr/lib/redis/modules/redistimeseries.so"]
