
ifeq ($(NOPAR),1)
.NOTPARALLEL: ;
endif

ROOT=.

MK_ALL_TARGETS=bindirs deps build pack

include $(ROOT)/deps/readies/mk/main

#----------------------------------------------------------------------------------------------  

export LIBMR_BINDIR=$(ROOT)/bin/$(FULL_VARIANT)/LibMR
include $(ROOT)/build/LibMR/Makefile.defs

export HIREDIS_BINDIR=$(ROOT)/bin/$(FULL_VARIANT)/hiredis
include $(ROOT)/build/hiredis/Makefile.defs

export LIBEVENT_BINDIR=$(ROOT)/bin/$(FULL_VARIANT.release)/libevent
include $(ROOT)/build/libevent/Makefile.defs

export DRAGONBOX_BINDIR=$(ROOT)/bin/$(FULL_VARIANT.release)/dragonbox
include $(ROOT)/build/dragonbox/Makefile.defs

export FAST_DOUBLE_PARSER_C_BINDIR=$(ROOT)/bin/$(FULL_VARIANT.release)/fast_double_parser_c
include $(ROOT)/build/fast_double_parser_c/Makefile.defs

export RMUTIL_BINDIR=$(ROOT)/bin/$(FULL_VARIANT)/rmutil
include $(ROOT)/build/rmutil/Makefile.defs

export CPU_FEATURES_BINDIR=$(ROOT)/bin/$(FULL_VARIANT.release)/cpu_features
include $(ROOT)/build/cpu_features/Makefile.defs

#----------------------------------------------------------------------------------------------  

define HELPTEXT
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

make test          # run unit and flow tests

make unit_tests    # run unit tests

make flow_tests    # run tests
  TEST=name        # run test matching 'name'
  TEST_ARGS="..."  # RLTest arguments
  QUICK=1          # shortcut for GEN=1 AOF=0 SLAVES=0 AOF_SLAVES=0 OSS_CLUSTER=0
  GEN=1            # run general tests on a standalone Redis topology
  AOF=1            # run AOF persistency tests on a standalone Redis topology
  SLAVES=1         # run replication tests on standalone Redis topology
  AOF_SLAVES=1     # run AND and replication tests on standalone Redis topology
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

make docker        # build for specified platform
  OSNICK=nick        # platform to build for (default: host platform)
  TEST=1             # run tests after build
  PACK=1             # create package
  ARTIFACTS=1        # copy artifacts to host

make sanbox        # create container with CLang Sanitizer

endef

#----------------------------------------------------------------------------------------------  

MK_CUSTOM_CLEAN=1

BINDIR=$(BINROOT)/src
SRCDIR=src

CC_C_STD=gnu11

#----------------------------------------------------------------------------------------------

TARGET=$(BINROOT)/redistimeseries.so

define CC_INCLUDES +=
	$(ROOT)/deps/RedisModulesSDK
	$(ROOT)/deps
	$(LIBEVENT_BINDIR)/include
endef

define CC_DEFS +=
	REDIS_MODULE_TARGET
	REDISTIMESERIES_GIT_SHA=\"$(GIT_SHA)\"
	REDISMODULE_SDK_RLEC
endef

CC_PEDANTIC=1

ifeq ($(VG),1)
CC_DEFS += _VALGRIND
endif

define LD_LIBS.deps +=
	  $(LIBMR)
	  $(FAST_DOUBLE_PARSER_C)
	  $(DRAGONBOX)
	  $(LIBEVENT_LIBS)
	  $(HIREDIS)
	  $(RMUTIL)
	  $(CPU_FEATURES)
endef
LD_LIBS += $(call flatten,$(LD_LIBS.deps))

LD_LIBS.ext += ssl crypto
LD_FLAGS.macos += -L$(openssl_prefix)/lib

define _SOURCES
	chunk.c
	compaction.c
	compressed_chunk.c
	config.c
	consts.c
	endianconv.c
	filter_iterator.c
	generic_chunk.c
	gorilla.c
	indexer.c
	libmr_integration.c
	libmr_commands.c
	module.c
	parse_policies.c
	query_language.c
	reply.c
	rdb.c
	short_read.c
	resultset.c
	tsdb.c
	series_iterator.c
	utils/arch_features.c
	sample_iterator.c
	enriched_chunk.c
	utils/heap.c
	multiseries_sample_iterator.c
	multiseries_agg_dup_sample_iterator.c
	utils/blocked_client.c
endef

ifeq ($(ARCH),x64)
define _SOURCES_AVX512
	compactions/compaction_avx512f.c
endef

define _SOURCES_AVX2
	compactions/compaction_avx2.c
endef

_SOURCES += $(_SOURCES_AVX512) $(_SOURCES_AVX2)
endif

SOURCES=$(addprefix $(SRCDIR)/,$(call flatten,$(_SOURCES)))
HEADERS=$(patsubst $(SRCDIR)/%.c,$(SRCDIR)/%.h,$(SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.d,$(SOURCES))

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

ifeq ($(wildcard $(CPU_FEATURES)),)
MISSING_DEPS += $(CPU_FEATURES)
endif

ifeq ($(wildcard $(LIBMR)),)
MISSING_DEPS += $(LIBMR)
endif

ifneq ($(MISSING_DEPS),)
DEPS=1
endif

DEPENDENCIES=cpu_features rmutil libmr hiredis libevent fast_double_parser_c dragonbox

ifneq ($(filter all deps $(DEPENDENCIES) pack,$(MAKECMDGOALS)),)
DEPS=1
endif

.PHONY: deps $(DEPENDENCIES)

#----------------------------------------------------------------------------------------------

.PHONY: pack clean all bindirs

all: bindirs $(TARGET)

include $(MK)/rules

#----------------------------------------------------------------------------------------------

ifeq ($(DEPS),1)

.PHONY: cpu_features rmutil libmr hiredis dragonbox fast_double_parser_c

deps: $(CPU_FEATURES) $(RMUTIL) $(LIBEVENT) $(HIREDIS) $(LIBMR) $(FAST_DOUBLE_PARSER_C) $(DRAGONBOX)

cpu_features: $(CPU_FEATURES)

$(CPU_FEATURES):
	@echo Building $@ ...
	$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/cpu_features DEBUG=''

rmutil: $(RMUTIL)

$(RMUTIL):
	@echo Building $@ ...
	$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/rmutil

libevent: $(LIBEVENT)

$(LIBEVENT):
	@echo Building $@ ...
	$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/libevent DEBUG=''

hiredis: $(HIREDIS)

$(HIREDIS):
	@echo Building $@ ...
	$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/hiredis

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
	@echo Cleaning ...
ifeq ($(ALL),1)
	-$(SHOW)rm -rf $(BINROOT) $(LIBEVENT_BINDIR) $(DRAGONBOX_BINDIR) $(FAST_DOUBLE_PARSER_C_BINDIR) $(CPU_FEATURES_BINDIR)
	$(SHOW)$(MAKE) -C $(ROOT)/build/libevent clean AUTOGEN=1
else
	-$(SHOW)rm -rf $(BINDIR)
ifeq ($(DEPS),1)
	-$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/rmutil clean
	-$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/hiredis clean
	-$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/LibMR clean
	-$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/cpu_features DEBUG='' clean
	-$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/libevent DEBUG='' clean
	-$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/dragonbox DEBUG='' clean
	-$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/build/fast_double_parser_c DEBUG='' clean
	-$(SHOW)$(MAKE) --no-print-directory -C $(ROOT)/tests/unit DEBUG='' clean
endif
endif

-include $(CC_DEPS)

AVX512F_OBJECTS=$(filter %512f.o,$(OBJECTS))
AVX2_OBJECTS=$(filter %avx2.o,$(OBJECTS))
REGULAR_OBJECTS=$(filter-out $(AVX512F_OBJECTS) $(AVX2_OBJECTS),$(OBJECTS))

$(AVX512F_OBJECTS) : $(BINDIR)/%.o : $(SRCDIR)/%.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) $(CC_C_FLAGS) -mavx512f -c $< -o $@

$(AVX2_OBJECTS) : $(BINDIR)/%.o : $(SRCDIR)/%.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) $(CC_C_FLAGS) -mavx2 -c $< -o $@

$(REGULAR_OBJECTS) : $(BINDIR)/%.o : $(SRCDIR)/%.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) $(CC_C_FLAGS) -c $< -o $@

$(TARGET): $(BIN_DIRS) $(MISSING_DEPS) $(OBJECTS)
	@echo Linking $@...
	$(SHOW)$(CXX) $(SO_LD_FLAGS) $(LD_FLAGS) -o $@ $(OBJECTS) $(LD_LIBS)

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

test: unit_tests flow_tests

clean-tests:
	$(SHOW)$(MAKE) -C $(ROOT)/tests/unit clean

.PHONY: test unit_tests flow_tests clean-tests

#----------------------------------------------------------------------------------------------

UNITTESTS_RUNNER=$(BINROOT)/unit_tests/unit_tests

$(UNITTESTS_RUNNER)	: $(TARGET)
	$(SHOW)$(MAKE) -C $(ROOT)/tests/unit

unit_tests: $(UNITTESTS_RUNNER)
	@echo Running unit tests...
	$(SHOW)$<

#----------------------------------------------------------------------------------------------

ifeq ($(QUICK),1)
export GEN=1
export SLAVES=0
export AOF=0
export AOF_SLAVES=0
export OSS_CLUSTER=0
else
export GEN ?= 1
export SLAVES ?= 1
export AOF ?= 1
export AOF_SLAVES ?= 1
export OSS_CLUSTER ?= 1
endif

ifneq ($(RLEC),1)

flow_tests: #$(TARGET)
	$(SHOW)\
	MODULE=$(realpath $(TARGET)) \
	GEN=$(GEN) AOF=$(AOF) SLAVES=$(SLAVES) AOF_SLAVES=$(AOF_SLAVES) OSS_CLUSTER=$(OSS_CLUSTER) \
	VALGRIND=$(VALGRIND) \
	TEST=$(TEST) \
	$(ROOT)/tests/flow/tests.sh

else # RLEC

flow_tests: #$(TARGET)
	$(SHOW)RLEC=1 $(ROOT)/tests/flow/tests.sh

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

COV_EXCLUDE_DIRS += \
	deps \
	tests/unit

COV_EXCLUDE+=$(foreach D,$(COV_EXCLUDE_DIRS),'$(realpath $(ROOT))/$(D)/*')

coverage:
	$(SHOW)$(MAKE) build COV=1
	$(SHOW)$(COVERAGE_RESET)
	-$(SHOW)$(MAKE) test COV=1
	$(SHOW)$(COVERAGE_COLLECT_REPORT)

.PHONY: coverage

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

docker:
	$(SHOW)$(MAKE) -C build/docker

ifneq ($(wildcard /w/*),)
SANBOX_ARGS += -v /w:/w
endif

sanbox:
	@docker run -it -v $(PWD):/build -w /build --cap-add=SYS_PTRACE --security-opt seccomp=unconfined $(SANBOX_ARGS) redisfab/clang:16-x64-bullseye bash

.PHONY: box sanbox

#----------------------------------------------------------------------------------------------

pack: $(TARGET)
	@echo Creating packages...
	$(SHOW)MODULE=$(realpath $(TARGET)) BINDIR=$(BINDIR) $(ROOT)/sbin/pack.sh

upload-release:
	@RELEASE=1 $(ROOT)/sbin/upload-artifacts

upload-artifacts:
	@SNAPSHOT=1 $(ROOT)/sbin/upload-artifacts

.PHONY: pack upload-release upload-artifacts
