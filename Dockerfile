# BUILD redisfab/redistimeseries-${ARCH}-${OSNICK}:M.m.b

# stretch|bionic
ARG OSNICK=stretch

#----------------------------------------------------------------------------------------------
# FROM redisfab/redis-${OSNICK}:5.0.5 AS builder
FROM redis:latest AS builder

ENV X_NPROC "cat /proc/cpuinfo|grep processor|wc -l"

ADD ./ /build
WORKDIR /build

RUN ./deps/readies/bin/getpy2
RUN python ./system-setup.py

RUN make -C src -j $(eval "$X_NPROC")

#----------------------------------------------------------------------------------------------
# FROM redisfab/redis-${OSNICK}:5.0.5
FROM redis:latest

ENV LIBDIR /usr/lib/redis/modules
WORKDIR /data
RUN mkdir -p "$LIBDIR"

COPY --from=builder /build/bin/redistimeseries.so "$LIBDIR"

EXPOSE 6379
CMD ["redis-server", "--loadmodule", "/usr/lib/redis/modules/redistimeseries.so"]
