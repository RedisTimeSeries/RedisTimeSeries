
#----------------------------------------------------------------------------------------------
FROM redis:bullseye AS redis
FROM debian:bullseye-slim AS builder

SHELL ["/bin/bash", "-l", "-c"]

WORKDIR /build
COPY --from=redis /usr/local/ /usr/local/

ADD . /build

RUN ./deps/readies/bin/getupdates
RUN ./deps/readies/bin/getpy3
RUN ./system-setup.py
RUN make fetch build

#----------------------------------------------------------------------------------------------
FROM redis:bullseye

WORKDIR /data
RUN mkdir -p /usr/lib/redis/modules

COPY --from=builder /build/bin/redistimeseries.so /usr/lib/redis/modules

EXPOSE 6379
CMD ["redis-server", "--loadmodule", "/usr/lib/redis/modules/redistimeseries.so"]
