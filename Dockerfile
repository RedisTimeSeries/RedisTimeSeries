# BUILD redisfab/redistimeseries:${VERSION}-${ARCH}-${OSNICK}

ARG REDIS_VER=6.0.5

# stretch|bionic|buster
ARG OSNICK=buster

# ARCH=x64|arm64v8|arm32v7
ARG ARCH=x64

#----------------------------------------------------------------------------------------------
FROM redisfab/redis:${REDIS_VER}-${ARCH}-${OSNICK} AS builder

ARG REDIS_VER

# Add in deps. Only add this in s.t. this step is
#   cached and doesn't need to be re-done on subsequent
#   rebuilds
ADD ./deps /build/deps
WORKDIR /build
RUN ./deps/readies/bin/getpy2

# Set up the system. Need just system-setup.py and the
#   test requirement file.
ADD ./system-setup.py .
ADD ./tests/requirements.txt ./tests/requirements.txt
RUN ./system-setup.py

# Now, add in the source for the build.
ADD . /build
RUN make fetch
RUN make build

#----------------------------------------------------------------------------------------------
FROM redisfab/redis:${REDIS_VER}-${ARCH}-${OSNICK}

ARG REDIS_VER

ENV LIBDIR /usr/lib/redis/modules
WORKDIR /data
RUN mkdir -p "$LIBDIR"

COPY --from=builder /build/bin/redistimeseries.so "$LIBDIR"

EXPOSE 6379
CMD ["redis-server", "--loadmodule", "/usr/lib/redis/modules/redistimeseries.so"]
