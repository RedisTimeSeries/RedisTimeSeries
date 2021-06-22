# BUILD redisfab/redistimeseries:${VERSION}-${ARCH}-${OSNICK}

ARG REDIS_VER=6.2.4

# OSNICK=focal|bionic|xenial|stretch|buster|centos8|centos7
ARG OSNICK=buster

# OS=debian:buster-slim|debian:stretch-slim|ubuntu:bionic
ARG OS=debian:buster-slim

# ARCH=x64|arm64v8|arm32v7
ARG ARCH=x64

ARG PACK=0
ARG TEST=0

#----------------------------------------------------------------------------------------------
FROM redisfab/redis:${REDIS_VER}-${ARCH}-${OSNICK} AS redis
FROM ${OS} AS builder

ARG OSNICK
ARG OS
ARG ARCH
ARG REDIS_VER
ARG PACK
ARG TEST

RUN echo "Building for ${OSNICK} (${OS}) for ${ARCH} [with Redis ${REDIS_VER}]"

WORKDIR /build
COPY --from=redis /usr/local/ /usr/local/

ADD . /build

RUN ./deps/readies/bin/getupdates
RUN ./deps/readies/bin/getpy3
RUN ./system-setup.py

RUN make fetch
RUN make build

#----------------------------------------------------------------------------------------------
FROM redisfab/redis:${REDIS_VER}-${ARCH}-${OSNICK}

ARG OSNICK
ARG OS
ARG ARCH
ARG REDIS_VER
ARG PACK

RUN set -ex ;\
    if [ -e /usr/bin/apt-get ]; then \
        apt-get update -qq; \
        apt-get upgrade -yqq; \
        rm -rf /var/cache/apt; \
    fi
RUN if [ -e /usr/bin/yum ]; then \
        yum update -y; \
        rm -rf /var/cache/yum; \
    fi

ENV LIBDIR /usr/lib/redis/modules
WORKDIR /data
RUN mkdir -p "$LIBDIR"

RUN mkdir -p /var/opt/redislabs/artifacts
RUN chown -R redis:redis /var/opt/redislabs
COPY --from=builder /build/bin/artifacts/ /var/opt/redislabs/artifacts

COPY --from=builder /build/bin/redistimeseries.so "$LIBDIR"

EXPOSE 6379
CMD ["redis-server", "--loadmodule", "/usr/lib/redis/modules/redistimeseries.so"]
