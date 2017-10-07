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
CC=gcc

all: rmutil redis-tsdb-module.so

rmutil: FORCE
	$(MAKE) -C $(RMUTIL_LIBDIR)

redis-tsdb-module.so: rmutil module.o tsdb.o compaction.o rdb.o chunk.o
	$(LD) -o $@ module.o tsdb.o rdb.o compaction.o chunk.o $(SHOBJ_LDFLAGS) $(LIBS) -L$(RMUTIL_LIBDIR) -lrmutil -lc 

clean:
	rm -rf *.xo *.so *.o

test: redis-tsdb-module.so
	python2 -m pytest -vv .

docker:
	cd .. && docker build -t redis-tsdb .

FORCE:
