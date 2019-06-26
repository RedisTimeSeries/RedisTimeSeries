ROOT=..
include $(ROOT)/build/mk/defs

BINDIR=$(BINROOT)/src
SRCDIR=.

#----------------------------------------------------------------------------------------------  

PREFIX ?= /usr/local
INSTALL_LIB:=$(PREFIX)/lib
INSTALL:=/usr/bin/install
PAKCAGE_NAME ?= $(BINDIR)/redistimeseries.{os}-{architecture}.latest.zip

# set environment variable RM_INCLUDE_DIR to the location of redismodule.h
SDK_DIR=$(ROOT)/deps/RedisModulesSDK
RMUTIL_LIBDIR=$(BINROOT)/rmutil

#----------------------------------------------------------------------------------------------

TARGET=$(BINROOT)/redistimeseries.so

CC=gcc

CC_FLAGS = \
	-I$(SDK_DIR) \
	-Wall \
	-fPIC \
	-std=gnu99 \
	-MMD -MF $(@:.o=.d) \
	-include $(SRCDIR)/common.h \
	-DREDIS_MODULE_TARGET \
	-DREDISMODULE_EXPERIMENTAL_API

LD_FLAGS += -lc -lm -L$(RMUTIL_LIBDIR) -lrmutil

ifeq ($(OS),linux)
LD_FLAGS += -shared -Bsymbolic
endif

ifeq ($(OS),macosx) 
CC_FLAGS += -dynamic
LD_FLAGS += -bundle -undefined dynamic_lookup
endif

ifeq ($(DEBUG),1)
CC_FLAGS += -g -ggdb -O0 -DVALGRIND
LD_FLAGS += -g
else
CC_FLAGS += -O3
endif

_SOURCES = \
	chunk.c \
	compaction.c \
	config.c \
	indexer.c \
	module.c \
	parse_policies.c \
	rdb.c \
	tsdb.c

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.c, $(BINDIR)/%.d, $(SOURCES))

#----------------------------------------------------------------------------------------------

all: rmutil $(TARGET)

include $(MK)/rules

.PHONY: package tests unittests clean all install uninstall docker

rmutil:
	@echo Building $@...
	$(SHOW)$(MAKE) -C $(ROOT)/build/rmutil

clean:
	-$(SHOW)find $(BINDIR) -name '*.[oadh]' -type f -delete
	-$(SHOW)$(MAKE) -C $(ROOT)/build/rmutil clean
	-$(SHOW)rm -f $(BINDIR)/tests_runner

-include $(CC_DEPS)

$(BINDIR)/%.o: $(SRCDIR)/%.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) -c $< -o $@

$(TARGET): $(OBJECTS)
	@echo Linking $@...
	$(SHOW)$(CC) -shared -o $@ $(OBJECTS) $(LD_FLAGS)
ifneq ($(DEBUG),1)
	$(SHOW)strip $@
endif

#----------------------------------------------------------------------------------------------

tests: $(TARGET)
	$(SHOW)cd ../tests; \
	REDISTIMESERIES=$(TARGET) python2 -m pytest ${EXTRA_ARGS} -svv .

REDIS_ARGS=\
	COMPACTION_POLICY "" \
	RETNTION_POLICY 3600 \
	MAX_SAMPLE_PER_CHUNK 1024

run: $(TARGET)
	$(SHOW)redis-server --loadmodule $(TARGET) $(REDIS_ARGS) --dir /tmp

gdb: $(TARGET)
	$(SHOW)gdb --args `which redis-server` --loadmodule $(TARGET) $(REDIS_ARGS) --dir /tmp

VALGRIND_ARGS=\
	--leak-check=full \
	--show-leak-kinds=all \
	-v redis-server

valgrind: $(TARGET)
	$(SHOW)valgrind $(VALGRIND_ARGS) --loadmodule $(TARGET) $(REDIS_ARGS) --dir /tmp

unittests_runner: $(TARGET) tests.o
	$(SHOW)$(CC) *.o -o $(BINDIR)/unittests_runner -L$(RMUTIL_LIBDIR) -lrmutil -lc -lm

unittests: unittests_runner
	$(SHOW)$(BINDIR)/unittests_runner

#----------------------------------------------------------------------------------------------

install: all
	$(SHOW)mkdir -p $(INSTALL_LIB)
	$(SHOW)$(INSTALL) $(TARGET) $(INSTALL_LIB)

uninstall:
	$(SHOW)rm -f $(INSTALL_LIB)/$(notdir $(TARGET))

docker:
	$(SHOW)cd .. && docker build -t redis-tsdb .

package: $(TARGET)
	$(SHOW)mkdir -p ../build
	$(SHOW)ramp pack -m "$(ROOT)/ramp.yml" -v -o "$(PAKCAGE_NAME)" $(TARGET) | tee /tmp/ramp_output
	$(SHOW)./clean_ramp.py `tail -1 /tmp/ramp_output`
