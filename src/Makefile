
ROOT=..
MK.pyver:=3

ifeq ($(VG),1)
override VALGRIND:=1
export VALGRIND
endif

ifeq ($(VALGRIND),1)
override DEBUG:=1
export DEBUG
endif

MK_ALL_TARGETS=bindirs deps build pack

include $(ROOT)/deps/readies/mk/main

#----------------------------------------------------------------------------------------------  

export LIBMR_BINDIR=$(ROOT)/bin/$(FULL_VARIANT)/LibMR
include $(ROOT)/build/LibMR/Makefile.defs

export HIREDIS_BINDIR=$(ROOT)/bin/$(FULL_VARIANT.release)/hiredis
include $(ROOT)/build/hiredis/Makefile.defs

export LIBEVENT_BINDIR=$(ROOT)/bin/$(FULL_VARIANT.release)/libevent
include $(ROOT)/build/libevent/Makefile.defs

export DRAGONBOX_BINDIR=$(ROOT)/bin/$(FULL_VARIANT.release)/dragonbox
include $(ROOT)/build/dragonbox/Makefile.defs

export FAST_DOUBLE_PARSER_C_BINDIR=$(ROOT)/bin/$(FULL_VARIANT.release)/fast_double_parser_c
include $(ROOT)/build/fast_double_parser_c/Makefile.defs

export RMUTIL_BINDIR=$(ROOT)/bin/$(FULL_VARIANT.release)/rmutil
include $(ROOT)/build/rmutil/Makefile.defs

#----------------------------------------------------------------------------------------------  

define HELPTEXT
make setup         # install packages required for build
make fetch         # download and prepare dependant modules

make build
  DEBUG=1          # build debug variant
  VARIANT=name     # use a build variant 'name'
  PROFILE=1        # enable profiling compile flags (and debug symbols) for release type
                   # You can consider this as build type release with debug symbols and -fno-omit-frame-pointer
  DEPS=1           # also build dependant modules
  COV=1            # perform coverage analysis (implies debug build)
make clean         # remove binary files
  ALL=1            # remove binary directories
  DEPS=1           # also clean dependant modules

make deps          # build dependant modules
make all           # build all libraries and packages

make unit_tests    # run unit tests

make flow_tests    # run tests
  TEST=name        # run test matching 'name'
  TEST_ARGS="..."  # RLTest arguments
  SIMPLE=1         # shortcut for GEN=1 AOF=0 SLAVES=0 OSS_CLUSTER=0
  GEN=1            # run general tests on a standalone Redis topology
  AOF=1            # run AOF persistency tests on a standalone Redis topology
  SLAVES=1         # run replication tests on standalone Redis topology
  OSS_CLUSTER=1    # run general tests on an OSS Cluster topology
  SHARDS=num       # run OSS cluster with `num` shards (default: 3)
  RLEC=1           # flow tests on RLEC
  COV=1            # perform coverage analysis
  VALGRIND|VG=1    # run specified tests with Valgrind
  EXT=1            # run tests with existing redis-server running

make pack          # build packages (ramp & dependencies)

make benchmarks    # run all benchmarks
  BENCHMARK=file   # run benchmark specified by 'filename'
  BENCH_ARGS="..." # redisbench_admin  extra arguments

make platform      # build artifacts for given platform
  OSNICK=nick        # build for OSNICK `nick`
  TEST=1             # also run tests

endef

#----------------------------------------------------------------------------------------------  

MK_CUSTOM_CLEAN=1

BINDIR=$(BINROOT)/src
SRCDIR=.

#----------------------------------------------------------------------------------------------  

PAKCAGE_NAME ?= $(BINROOT)/redistimeseries.{os}-{architecture}.latest.zip

#----------------------------------------------------------------------------------------------

TARGET=$(BINROOT)/redistimeseries.so
UNITTESTS_RUNNER=$(BINROOT)/unit_tests/unit_tests

CC=gcc

CC_FLAGS = \
	-I$(ROOT)/deps/RedisModulesSDK \
	-I$(ROOT)/deps \
	-Wall \
	-fPIC \
	-pedantic \
	-std=gnu99 \
	-MMD -MF $(@:.o=.d) \
	-include $(SRCDIR)/common.h \
	-DREDIS_MODULE_TARGET \
	-DREDISTIMESERIES_GIT_SHA=\"$(GIT_SHA)\" \
	-DREDISMODULE_EXPERIMENTAL_API

LD_FLAGS += 
LD_LIBS += \
	  $(LIBMR) \
	  $(FAST_DOUBLE_PARSER_C) \
	  $(DRAGONBOX) \
	  $(LIBEVENT_LIBS) \
	  $(HIREDIS) \
	  $(RMUTIL) \
	  -lc -lm -lpthread -lstdc++

ifeq ($(OS),linux)
LD_LIBS += -lssl -lcrypto
SO_LD_FLAGS += -shared -Bsymbolic $(LD_FLAGS)
endif

ifeq ($(OS),macos)
LD_LIBS += -L$(LIBSSL_PREFIX)/lib -lssl -lcrypto
SO_LD_FLAGS += -bundle -undefined dynamic_lookup $(LD_FLAGS)
DYLIB_LD_FLAGS += -dynamiclib $(LD_FLAGS)
endif

ifeq ($(PROFILE),1)
CC_FLAGS += -g -ggdb -fno-omit-frame-pointer
endif

ifeq ($(DEBUG),1)
CC_FLAGS += -g -ggdb -O0 -DDEBUG -D_DEBUG
LD_FLAGS += -g

ifeq ($(VALGRIND),1)
CC_FLAGS += -D_VALGRIND
endif
else ifeq ($(PROFILE),1)
CC_FLAGS += -O2
else
CC_FLAGS += -O3
endif

CC_FLAGS += $(CC_FLAGS.coverage)
LD_FLAGS += $(LD_FLAGS.coverage)

_SOURCES=\
	chunk.c \
	compaction.c \
	compressed_chunk.c \
	config.c \
	endianconv.c \
	filter_iterator.c \
	generic_chunk.c \
	gorilla.c \
	indexer.c \
	libmr_integration.c \
	libmr_commands.c \
	module.c \
	parse_policies.c \
	query_language.c \
	reply.c \
	rdb.c \
	short_read.c \
	resultset.c \
	tsdb.c \
	series_iterator.c \
	sample_iterator.c \
	domain_chunk.c

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
HEADERS=$(patsubst $(SRCDIR)/%.c,$(SRCDIR)/%.h,$(SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.c, $(BINDIR)/%.d, $(SOURCES) $(TEST_SOURCES))

include $(MK)/defs

#----------------------------------------------------------------------------------------------

MISSING_DEPS:=
ifeq ($(wildcard $(HIREDIS)),)
MISSING_DEPS += $(HIREDIS)
endif
ifeq ($(wildcard $(LIBEVENT)),)
MISSING_DEPS += $(LIBEVENT)
endif
ifeq ($(wildcard $(FAST_DOUBLE_PARSER_C)),)
MISSING_DEPS += $(FAST_DOUBLE_PARSER_C)
endif
ifeq ($(wildcard $(DRAGONBOX)),)
MISSING_DEPS += $(DRAGONBOX)
endif
ifeq ($(wildcard $(RMUTIL)),)
MISSING_DEPS += $(RMUTIL)
endif
ifeq ($(wildcard $(LIBMR)),)
MISSING_DEPS += $(LIBMR)
endif

ifneq ($(MISSING_DEPS),)
DEPS=1
endif

DEPENDENCIES=rmutil libmr hiredis libevent fast_double_parser_c dragonbox

ifneq ($(filter all deps $(DEPENDENCIES) pack,$(MAKECMDGOALS)),)
DEPS=1
endif

.PHONY: deps $(DEPENDENCIES)

#----------------------------------------------------------------------------------------------

.PHONY: pack flow_tests unit_tests clean all install uninstall docker bindirs

all: bindirs $(TARGET)

include $(MK)/rules

#----------------------------------------------------------------------------------------------

ifeq ($(DEPS),1)

.PHONY: rmutil libmr hiredis dragonbox fast_double_parser_c

deps: $(RMUTIL) $(LIBEVENT) $(HIREDIS) $(LIBMR) $(FAST_DOUBLE_PARSER_C) $(DRAGONBOX)

rmutil: $(RMUTIL)

$(RMUTIL):
	@echo Building $@ ...
	$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/rmutil DEBUG=''

libevent: $(LIBEVENT)

$(LIBEVENT):
	@echo Building $@ ...
	$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/libevent DEBUG=''

hiredis: $(HIREDIS)

$(HIREDIS):
	@echo Building $@ ...
	$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/hiredis DEBUG=''

libmr: $(LIBMR)

$(LIBMR): $(LIBEVENT) $(HIREDIS)
	@echo Building $@ ...
	$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/LibMR

dragonbox: $(DRAGONBOX)

$(DRAGONBOX):
	@echo Building $@ ...
	$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/dragonbox DEBUG=''

fast_double_parser_c: $(FAST_DOUBLE_PARSER_C)

$(FAST_DOUBLE_PARSER_C):
	@echo Building $@ ...
	$(SHOW)$(MAKE)  --no-print-directory -C $(ROOT)/build/fast_double_parser_c DEBUG=''

#----------------------------------------------------------------------------------------------

else

deps: ;

endif # DEPS

#----------------------------------------------------------------------------------------------

clean:
ifeq ($(ALL),1)
	-$(SHOW)rm -rf $(BINDIR) $(TARGET)
else
	-$(SHOW)[ -e $(BINDIR) ] && find $(BINDIR) -name '*.[oadh]' -type f -delete
	-$(SHOW)rm -f $(TARGET)
endif
ifdef ($(DEPS),1)
	-$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/rmutil  DEBUG='' clean
	-$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/libevent DEBUG='' clean
	-$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/hiredis DEBUG='' clean
	-$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/LibMR clean
	-$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/dragonbox DEBUG='' clean
	-$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/fast_double_parser_c DEBUG='' clean
	-$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/tests/unit DEBUG='' clean
endif

-include $(CC_DEPS)

$(BINDIR)/%.o: $(SRCDIR)/%.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) -c $< -o $@

$(TARGET): $(BIN_DIRS) $(MISSING_DEPS) $(OBJECTS)
	@echo Linking $@...
	$(SHOW)$(CC) $(SO_LD_FLAGS) -o $@ $(OBJECTS) $(LD_LIBS)
ifeq ($(OS),macos)
	$(SHOW)$(CC) $(DYLIB_LD_FLAGS) -o $(patsubst %.so,%.dylib,$@) $(OBJECTS) $(LD_LIBS)
endif
	$(SHOW)cd $(BINROOT)/..; ln -sf $(FULL_VARIANT)/$(notdir $(TARGET)) $(notdir $(TARGET))

#----------------------------------------------------------------------------------------------

NO_LINT_PATTERNS=endianconv

LINT_SOURCES=$(call filter-out2,$(NO_LINT_PATTERNS),$(SOURCES) $(HEADERS))

docker_lint:
	$(SHOW)docker build -t llvm-toolset -f $(ROOT)/build/docker/llvm.Dockerfile .
	$(SHOW)docker run --rm -w /code/src -v `pwd`/..:/code llvm-toolset make lint

lint:
	$(SHOW)clang-format -Werror -n $(LINT_SOURCES)

format:
	$(SHOW)clang-format -i $(LINT_SOURCES)

#----------------------------------------------------------------------------------------------

$(UNITTESTS_RUNNER)	: $(TARGET)
	$(SHOW)$(MAKE) -C ../tests/unit

unit_tests: $(UNITTESTS_RUNNER)
	@echo Running unit tests...
	$(SHOW)$<

#----------------------------------------------------------------------------------------------

ifeq ($(SIMPLE),1)
export GEN=1
export SLAVES=0
export AOF=0
export OSS_CLUSTER=0
else
export GEN ?= 1
export SLAVES ?= 1
export AOF ?= 1
export OSS_CLUSTER ?= 1
endif

ifneq ($(RLEC),1)

flow_tests: #$(TARGET)
ifeq ($(COV),1)
	$(COVERAGE_RESET)
endif
	$(SHOW)\
	MODULE=$(realpath $(TARGET)) \
	GEN=$(GEN) AOF=$(AOF) SLAVES=$(SLAVES) OSS_CLUSTER=$(OSS_CLUSTER) \
	VALGRIND=$(VALGRIND) \
	TEST=$(TEST) \
	$(ROOT)/tests/flow/tests.sh
ifeq ($(COV),1)
	$(COVERAGE_COLLECT_REPORT)
endif

else # RLEC

flow_tests: #$(TARGET)
ifeq ($(COV),1)
	$(COVERAGE_RESET)
endif
	$(SHOW)RLEC=1 $(ROOT)/tests/flow/tests.sh
ifeq ($(COV),1)
	$(COVERAGE_COLLECT_REPORT)
endif

endif # RLEC

#----------------------------------------------------------------------------------------------

BENCHMARK_ARGS = redisbench-admin run-local

ifneq ($(REMOTE),)
	BENCHMARK_ARGS = redisbench-admin run-remote 
endif

BENCHMARK_ARGS += \
	--module_path $(realpath $(TARGET)) \
	--required-module timeseries \
	--dso $(realpath $(TARGET))

ifneq ($(BENCHMARK),)
BENCHMARK_ARGS += --test $(BENCHMARK)
endif

ifneq ($(BENCH_ARGS),)
BENCHMARK_ARGS += $(BENCH_ARGS)
endif

benchmark: $(TARGET)
	$(SHOW)set -e; cd $(ROOT)/tests/benchmarks; $(BENCHMARK_ARGS)

#----------------------------------------------------------------------------------------------

REDIS_ARGS=\
	COMPACTION_POLICY "" \
	RETNTION_POLICY 3600 \
	MAX_SAMPLE_PER_CHUNK 1024

run: $(TARGET)
	$(SHOW)redis-server --loadmodule $(realpath $(TARGET)) --dir /tmp

run_dev: $(TARGET)
	$(SHOW)redis-server --loadmodule $(realpath $(TARGET)) $(REDIS_ARGS) --dir /tmp

gdb: $(TARGET)
	$(SHOW)gdb --args `command -v redis-server` --loadmodule $(realpath $(TARGET)) --dir /tmp

#----------------------------------------------------------------------------------------------
# To see more kinds of leaks add --show-leak-kinds=all to args
# A good way to search for relevant leaks is greping for "TSDB"
# For greacefull exit from redis use the cli: FLUSHDB SYNC; shutdown NOSAVE;

VALGRIND_ARGS=\
	--leak-check=full \
	--keep-debuginfo=yes \
	--show-reachable=no \
	--show-possibly-lost=no \
	--track-origins=yes \
	--suppressions=$(ROOT)/tests/redis_valgrind.sup \
	-v redis-server

valgrind: $(TARGET)
	$(SHOW)valgrind $(VALGRIND_ARGS) --loadmodule $(realpath $(TARGET)) $(REDIS_ARGS) --dir /tmp

CALLGRIND_ARGS=\
	--tool=callgrind \
	--dump-instr=yes \
	--simulate-cache=no \
	--collect-jumps=yes \
	--collect-atstart=yes \
	--instr-atstart=yes \
	-v redis-server --protected-mode no --save "" --appendonly no

callgrind: $(TARGET)
	$(SHOW)valgrind $(CALLGRIND_ARGS) --loadmodule $(realpath $(TARGET)) $(REDIS_ARGS) --dir /tmp

#----------------------------------------------------------------------------------------------

install: all
	$(SHOW)mkdir -p $(INSTALL_LIB)
	$(SHOW)$(INSTALL) $(TARGET) $(INSTALL_LIB)

uninstall:
	$(SHOW)rm -f $(INSTALL_LIB)/$(notdir $(TARGET))

docker:
	$(SHOW)cd .. && docker build -t redis-tsdb .

coverage:
	$(SHOW)DEBUG=1 COVERAGE=1 make unit_tests
	$(SHOW)DEBUG=1 COVERAGE=1 make flow_tests
	$(SHOW)mkdir -p tmp/lcov
	$(SHOW)lcov -d ../bin/linux-x64-debug/src -c -o tmp/lcov/hiredis.info
	$(SHOW)lcov -l tmp/lcov/hiredis.info
	$(SHOW)genhtml --legend -o tmp/lcov/report tmp/lcov/hiredis.info > /dev/null 2>&1

#----------------------------------------------------------------------------------------------

pack: $(TARGET)
	@echo Creating packages...
	$(SHOW)MODULE=$(realpath $(TARGET)) BINDIR=$(BINDIR) $(ROOT)/sbin/pack.sh

#----------------------------------------------------------------------------------------------
