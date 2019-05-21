#----------------------------------------------------------------------------------------------
# FROM redislabs/redis-arm:arm64-bionic as builder
FROM raffapen/redis-arm:arm64-bionic

RUN set -ex;\
    apt-get update;\
    apt-get install -y python
    
ADD ./ /redis-timeseries
WORKDIR /redis-timeseries

RUN python ./system-setup.py

RUN set -ex;\
	cd RedisModulesSDK/rmutil;\
	make clean;\
	make

RUN set -ex;\
	cd src;\
    make clean; \
    make

#----------------------------------------------------------------------------------------------
# FROM redislabs/redis-arm:arm64-bionic
FROM raffapen/redis-arm:arm64-bionic

ENV LIBDIR /usr/lib/redis/modules
WORKDIR /data
RUN set -ex;\
    mkdir -p "$LIBDIR"

COPY --from=builder /redis-timeseries/src/redistimeseries.so "$LIBDIR"

EXPOSE 6379
CMD ["redis-server", "--loadmodule", "/usr/lib/redis/modules/redistimeseries.so"]
