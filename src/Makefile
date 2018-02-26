#set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=../RedisModulesSDK
endif

ifndef RMUTIL_LIBDIR
	RMUTIL_LIBDIR=../RedisModulesSDK/rmutil
endif

# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared -Bsymbolic
else
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif
CFLAGS = -I$(RM_INCLUDE_DIR) -Wall -g -fPIC -lc -lm -std=gnu99  -DREDIS_MODULE_TARGET

all: rmutil redis-tsdb-module.so

rmutil:
	$(MAKE) -C $(RMUTIL_LIBDIR)

redis-tsdb-module.so: rmutil module.o tsdb.o compaction.o rdb.o chunk.o parse_policies.o config.o
	$(LD) -o $@ module.o tsdb.o rdb.o compaction.o chunk.o parse_policies.o config.o $(SHOBJ_LDFLAGS) $(LIBS) -L$(RMUTIL_LIBDIR) -lrmutil -lc

clean:
	rm -rf *.xo *.so *.o ./tests_runner

tests: redis-tsdb-module.so
	python2 -m pytest -vv .

unittests_runner: redis-tsdb-module.so tests.o
	$(CC) *.o -o unittests_runner $(LIBS) -L$(RMUTIL_LIBDIR) -lrmutil -lc -lm

unittests: unittests_runner
	./unittests_runner

docker:
	cd .. && docker build -t redis-tsdb .

package: redis-tsdb-module.so
	mkdir -p ../build
	ramp pack -m "`pwd`/../ramp_manifest.yml" -v -o "../build/redis-tsdb-module.{os}-{architecture}.latest.zip" "`pwd`/redis-tsdb-module.so"

.PHONY: package tests unittests clean all