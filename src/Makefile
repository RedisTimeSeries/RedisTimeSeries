ROOT=..
include $(ROOT)/deps/readies/mk/main

#----------------------------------------------------------------------------------------------  

define HELP
make setup         # install packages required for build
make fetch         # download and prepare dependant modules

make build
  DEBUG=1          # build debug variant
  VARIANT=name     # use a build variant 'name'
  DEPS=1           # also build dependant modules
  COV=1            # perform coverage analysis (implies debug build)
make clean         # remove binary files
  ALL=1            # remove binary directories
  DEPS=1           # also clean dependant modules

make deps          # build dependant modules
make all           # build all libraries and packages

make test          # run tests
  TEST=name        # run test maching 'name'
  TEST_ARGS="..."  # RLTest arguments
  COV=1            # perform coverage analysis

make pack          # build packages (ramp & dependencies)
endef

#----------------------------------------------------------------------------------------------  

MK_CUSTOM_CLEAN=1

BINDIR=$(BINROOT)/src
SRCDIR=.

ifeq ($(OS),macosx)
# pip installed packages like ramp reside here:
export PATH:=$(PATH):$(HOME)/Library/Python/2.7/bin
endif

#----------------------------------------------------------------------------------------------  

PREFIX ?= /usr/local
INSTALL_LIB:=$(PREFIX)/lib
INSTALL:=/usr/bin/install
PAKCAGE_NAME ?= $(BINROOT)/redistimeseries.{os}-{architecture}.latest.zip

# set environment variable RM_INCLUDE_DIR to the location of redismodule.h
SDK_DIR=$(ROOT)/deps/RedisModulesSDK
RMUTIL_LIBDIR=$(BINROOT)/rmutil
LIBRMUTIL=$(RMUTIL_LIBDIR)/librmutil.a

#----------------------------------------------------------------------------------------------

TARGET=$(BINROOT)/redistimeseries.so
UNITTESTS_RUNNER=$(BINROOT)/unittests_runner

CC=gcc

CC_FLAGS = \
	-I$(SDK_DIR) \
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
LD_LIBS += -lc -lm -L$(RMUTIL_LIBDIR) -lrmutil

ifeq ($(OS),linux)
SO_LD_FLAGS += -shared -Bsymbolic $(LD_FLAGS)
endif

ifeq ($(OS),macosx)
SO_LD_FLAGS += -bundle -undefined dynamic_lookup -Wl,-macosx_version_min,10.8 $(LD_FLAGS)
endif

ifeq ($(DEBUG),1)
CC_FLAGS += -g -ggdb -O0 -DVALGRIND
LD_FLAGS += -g
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
	generic_chunk.c \
	gorilla.c \
	indexer.c \
	module.c \
	parse_policies.c \
	rdb.c \
	tsdb.c

_TEST_SOURCES=\
	unittests.c \
	unittests_parse_policies.c \
	unittests_uncompressed_chunk.c \
	unittests_compressed_chunk.c \
	endianconv.c

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

TEST_SOURCES=$(addprefix $(SRCDIR)/,$(_TEST_SOURCES))
TEST_OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(TEST_SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.c, $(BINDIR)/%.d, $(SOURCES) $(TEST_SOURCES))

include $(MK)/defs

#----------------------------------------------------------------------------------------------

.PHONY: package tests unittests clean all install uninstall docker bindirs

all: bindirs $(TARGET)

include $(MK)/rules

rmutil:
	@echo Building $@...
	$(SHOW)$(MAKE) -C $(ROOT)/build/rmutil

$(LIBRMUTIL): rmutil

docker_lint:
	docker build -t llvm-toolset -f llvm.Dockerfile .
	docker run --rm -w /code/src -v `pwd`/..:/code llvm-toolset make lint

lint:
	clang-format -Werror -n $(SOURCES)

format:
	clang-format -i $(SOURCES)

clean:
	-$(SHOW)[ -e $(BINDIR) ] && find $(BINDIR) -name '*.[oadh]' -type f -delete
	-$(SHOW)$(MAKE) -C $(ROOT)/build/rmutil clean
	-$(SHOW)rm -f $(TARGET) $(UNITTESTS_RUNNER)

-include $(CC_DEPS)

$(BINDIR)/%.o: $(SRCDIR)/%.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) -c $< -o $@

$(TARGET): $(BIN_DIRS) $(OBJECTS) $(LIBRMUTIL)
	@echo Linking $@...
	$(SHOW)$(CC) $(SO_LD_FLAGS) -o $@ $(OBJECTS) $(LD_LIBS)
	$(SHOW)cd $(BINROOT)/..; ln -sf $(FULL_VARIANT)/$(notdir $(TARGET)) $(notdir $(TARGET))

#----------------------------------------------------------------------------------------------

TEST_REPORT_DIR ?= $(PWD)
TEST_ARGS += \
	--junitxml=$(TEST_REPORT_DIR)/tests_report.xml

tests: $(TARGET)
	@if ! command -v redis-server > /dev/null; then \
		echo "Cannot find redis-server. Aborting." ;\
		exit 1 ;\
	fi
ifeq ($(COV),1)
	$(COVERAGE_RESET)
endif
ifeq ($(TEST),)
	$(SHOW)set -e; \
	cd ../tests; \
	REDISTIMESERIES=$(TARGET) python -m pytest  $(TEST_ARGS) -svv .
else
	cd ../tests; \
	PYDEBUG=1 REDIS_VERBOSE=1 REDISTIMESERIES=$(TARGET) python -m pytest -k "${TEST}" -svv .
endif
ifeq ($(COV),1)
	$(COVERAGE_COLLECT_REPORT)
endif

$(UNITTESTS_RUNNER)	: $(TARGET) $(OBJECTS) $(TEST_OBJECTS)
	$(SHOW)$(CC) $(LD_FLAGS) -o $@ $(OBJECTS) $(TEST_OBJECTS) $(LD_LIBS)

unittests: $(UNITTESTS_RUNNER)
	@echo Running unit tests...
	$(SHOW)$<

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
	$(SHOW)gdb --args `which redis-server` --loadmodule $(realpath $(TARGET)) --dir /tmp

cgdb: $(TARGET)
	$(SHOW)cgdb --args `which redis-server` --loadmodule $(realpath $(TARGET)) --dir /tmp

#----------------------------------------------------------------------------------------------

VALGRIND_ARGS=\
	--leak-check=full \
	--show-leak-kinds=all \
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
	DEBUG=1 COVERAGE=1 make tests
	DEBUG=1 COVERAGE=1 make unittests
	mkdir -p tmp/lcov
	lcov -d ../bin/linux-x64-debug/src -c -o tmp/lcov/hiredis.info
	lcov -l tmp/lcov/hiredis.info
	genhtml --legend -o tmp/lcov/report tmp/lcov/hiredis.info > /dev/null 2>&1


#----------------------------------------------------------------------------------------------

# arguments:
#   BRANCH: specifies branch names to serve as an exta package tag
#   INTO: package destination directory (optinal)

define ramp_pack # (1=tag, 2=subdir)
$(SHOW) python -m RAMP.ramp pack -m $(ROOT)/ramp.yml -o $(BINROOT)/redistimeseries.{os}-{architecture}.$1.zip $(abspath $(TARGET)) 2> /dev/null | grep '.zip' > /tmp/ramp_output
$(SHOW). $(ROOT)/deps/readies/shibumi/functions; realpath `tail -1 /tmp/ramp_output` > $(BINROOT)/../PACKAGE.$(1)
$(SHOW)$(ROOT)/src/clean_ramp.py `cat $(BINROOT)/../PACKAGE.$(1)`
$(SHOW)if [ ! -z "$(INTO)" ]; then { mkdir -p $(INTO)/$(2); cp `cat $(BINROOT)/../PACKAGE.$(1)` $(INTO)/$(2); }; fi
endef

package: $(TARGET)
	@echo Creating packages...
	$(SHOW)if ! command -v redis-server > /dev/null; then \
		echo Cannot find redis-server. Aborting. ;\
		exit 1 ;\
	fi
	$(call ramp_pack,latest,release)
	$(call ramp_pack,{semantic_version},release)
ifneq ($(BRANCH),)
	$(call ramp_pack,$(BRANCH),branch)
endif
