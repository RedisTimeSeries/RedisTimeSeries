PREFIX?=/usr/local
INSTALL_LIB=$(PREFIX)/lib
INSTALL=/usr/bin/install
PAKCAGE_NAME?="../build/redistimeseries.{os}-{architecture}.latest.zip"

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
CFLAGS = -I$(RM_INCLUDE_DIR) -Wall -O3 -g -fPIC -lc -lm -std=gnu99  -DREDIS_MODULE_TARGET -DREDISMODULE_EXPERIMENTAL_API

all: rmutil redistimeseries.so

rmutil:
	$(MAKE) -C $(RMUTIL_LIBDIR)

module.o: module.c version.h
	$(CC) $(CFLAGS) -c -o $@ module.c

redistimeseries.so: rmutil module.o tsdb.o compaction.o rdb.o chunk.o parse_policies.o config.o indexer.o endianconv.o version.h
	$(LD) -o $@ module.o tsdb.o rdb.o compaction.o chunk.o parse_policies.o config.o indexer.o $(SHOBJ_LDFLAGS) $(LIBS) -L$(RMUTIL_LIBDIR) -lrmutil -lc

install: all
	@mkdir -p $(INSTALL_LIB)
	$(INSTALL) redistimeseries.so $(INSTALL_LIB)

uninstall:
	rm -f $(INSTALL_LIB)/redistimeseries.so

clean:
	rm -rf *.xo *.so *.o ./tests_runner

tests: redistimeseries.so
	python2 -m pytest ${EXTRA_ARGS} -svv .

run: redistimeseries.so
	redis-server --loadmodule `pwd`/redistimeseries.so COMPACTION_POLICY "" RETNTION_POLICY 3600 MAX_SAMPLE_PER_CHUNK 1024 --dir /tmp

gdb: redistimeseries.so
	gdb --args `which redis-server` --loadmodule `pwd`/redistimeseries.so COMPACTION_POLICY "" RETNTION_POLICY 3600 MAX_SAMPLE_PER_CHUNK 1024 --dir /tmp
valgrind: redistimeseries.so
	valgrind --leak-check=full --show-leak-kinds=all -v redis-server --loadmodule `pwd`/redistimeseries.so COMPACTION_POLICY "" RETNTION_POLICY 3600 MAX_SAMPLE_PER_CHUNK 1024 --dir /tmp

unittests_runner: redistimeseries.so tests.o
	$(CC) *.o -o unittests_runner $(LIBS) -L$(RMUTIL_LIBDIR) -lrmutil -lc -lm

unittests: unittests_runner
	./unittests_runner

docker:
	cd .. && docker build -t redis-tsdb .

package: redistimeseries.so
	mkdir -p ../build
	ramp pack -m "`pwd`/../ramp.yml" -v -o ${PAKCAGE_NAME} "`pwd`/redistimeseries.so" | tee /tmp/ramp_output
	./clean_ramp.py `tail -1 /tmp/ramp_output`

.PHONY: package tests unittests clean all
