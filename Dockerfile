# BUILD redisfab/redistimeseries-${OSNICK}:M.m.b-${ARCH}

# stretch|bionic|buster
ARG OSNICK=buster

# ARCH=x64|arm64v8|arm32v7
ARG ARCH=x64

#----------------------------------------------------------------------------------------------
FROM redisfab/redis:6.0-rc3-${ARCH}-${OSNICK} AS builder

ADD ./ /build
WORKDIR /build

RUN ./deps/readies/bin/getpy2
RUN ./system-setup.py
RUN make fetch

ENV X_NPROC "cat /proc/cpuinfo|grep processor|wc -l"
RUN echo nproc=$(nproc); echo NPROC=$(eval "$X_NPROC")
RUN make build

#----------------------------------------------------------------------------------------------
FROM redisfab/redis:6.0-rc3-${ARCH}-${OSNICK}

ENV LIBDIR /usr/lib/redis/modules
WORKDIR /data
RUN mkdir -p "$LIBDIR"

COPY --from=builder /build/bin/redistimeseries.so "$LIBDIR"

EXPOSE 6379
CMD ["redis-server", "--loadmodule", "/usr/lib/redis/modules/redistimeseries.so"]
