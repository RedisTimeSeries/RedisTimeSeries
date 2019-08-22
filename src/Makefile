ROOT=..
include $(ROOT)/build/mk/defs

BINDIR=$(BINROOT)/src
SRCDIR=.

#----------------------------------------------------------------------------------------------  

# set environment variable RM_INCLUDE_DIR to the location of redismodule.h
SDK_DIR=$(ROOT)/deps/RedisModulesSDK
REDISEARCH_DIR=$(ROOT)/deps/RediSearch

LIBRMUTIL=$(BINROOT)/rmutil/librmutil.a
LIBREDISEARCH=$(BINROOT)/RediSearch/libredisearch.a

#----------------------------------------------------------------------------------------------

TARGET=$(BINROOT)/redistimeseries.so
UNITTESTS_RUNNER=$(BINROOT)/unittests_runner

CC=gcc

CC_FLAGS = \
	-I$(SDK_DIR) \
	-I$(REDISEARCH_DIR)/src \
	-Wall \
	-fPIC \
	-std=gnu99 \
	-MMD \
	-MF $(@:.o=.d) \
	-include $(SRCDIR)/common.h \
	-DREDIS_MODULE_TARGET \
	-DREDISMODULE_EXPERIMENTAL_API

LD_FLAGS += 
LD_LIBS += -lc -lm -lpthread -ldl $(LIBREDISEARCH)
# $(LIBRMUTIL)

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

ifeq ($(COV),1)
CC_FLAGS += -fprofile-arcs -ftest-coverage
LD_FLAGS += -fprofile-arcs
SO_LD_FLAGS += -fprofile-arcs
endif

_SOURCES=\
	chunk.c \
	compaction.c \
	config.c \
	endianconv.c \
	indexer.c \
	module.c \
	parse_policies.c \
	rdb.c \
	tsdb.c \
	search.c

_TEST_SOURCES=\
	tests.c \
	endianconv.c

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

TEST_SOURCES=$(addprefix $(SRCDIR)/,$(_TEST_SOURCES))
TEST_OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(TEST_SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.c, $(BINDIR)/%.d, $(SOURCES) $(TEST_SOURCES))

#----------------------------------------------------------------------------------------------  

PREFIX ?= /usr/local
INSTALL_LIB:=$(PREFIX)/lib
INSTALL:=/usr/bin/install

PAKCAGE_NAME ?= $(BINROOT)/redistimeseries.{os}-{architecture}.latest.zip

#----------------------------------------------------------------------------------------------

.PHONY: all package tests unittests clean all install uninstall docker bindirs deps RediSearch rmutil build

build: $(TARGET)

deps: RediSearch rmutil

include $(MK)/rules

clean:
ifeq ($(DEPS),1)
endif
	-$(SHOW)[ -e $(BINDIR) ] && find $(BINDIR) -name '*.[oadh]' -type f -delete
	-$(SHOW)$(MAKE) -C $(ROOT)/build/rmutil clean
	-$(SHOW)rm -f $(TARGET) $(UNITTESTS_RUNNER)
	-$(SHOW)find $(BINDIR) -name '*.gcov' -delete
	-$(SHOW)find $(BINDIR) -name '*.gcda' -delete
	-$(SHOW)find $(BINDIR) -name '*.gcno' -delete

-include $(CC_DEPS)

$(BINDIR)/%.o: $(SRCDIR)/%.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) -c $< -o $@

$(TARGET): $(BIN_DIRS) $(OBJECTS) $(LIBRMUTIL) $(LIBREDISEARCH)
	@echo Linking $@...
	$(SHOW)$(CC) $(SO_LD_FLAGS) -o $@ $(OBJECTS) $(LD_LIBS)
	$(SHOW)cd $(BINROOT)/..; ln -sf $(FULL_VARIANT)/$(notdir $(TARGET)) $(notdir $(TARGET))

#----------------------------------------------------------------------------------------------

rmutil: $(LIBRMUTIL)

RediSearch: $(LIBREDISEARCH)

$(LIBRMUTIL):
	@echo Building $@...
	$(SHOW)$(MAKE) -C $(ROOT)/build/rmutil

$(LIBREDISEARCH):
	@echo Building $@...
	$(SHOW)$(MAKE) -C $(ROOT)/build/RediSearch

#----------------------------------------------------------------------------------------------

TEST_REPORT_DIR ?= $(PWD)

tests: $(TARGET)
	$(SHOW)cd ../tests; \
	REDISTIMESERIES=$(TARGET) python -m pytest --junitxml=$(TEST_REPORT_DIR)/tests_report.xml $(TEST_ARGS) -svv .

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
	$(SHOW)redis-server --loadmodule $(realpath $(TARGET)) $(REDIS_ARGS) --dir /tmp

gdb: $(TARGET)
	$(SHOW)gdb --args `which redis-server` --loadmodule $(realpath $(TARGET)) $(REDIS_ARGS) --dir /tmp

cgdb: $(TARGET)
	$(SHOW)cgdb --args `which redis-server` --loadmodule $(realpath $(TARGET)) $(REDIS_ARGS) --dir /tmp

#----------------------------------------------------------------------------------------------

VALGRIND_ARGS=\
	--leak-check=full \
	--show-leak-kinds=all \
	-v redis-server

valgrind: $(TARGET)
	$(SHOW)valgrind $(VALGRIND_ARGS) --loadmodule $(realpath $(TARGET)) $(REDIS_ARGS) --dir /tmp

#----------------------------------------------------------------------------------------------

COV_DIR=$(BINDIR)/lcov

cov coverage:
	@$(MAKE) clean
	@$(MAKE) tests COV=1
	$(SHOW)mkdir -p $(COV_DIR)
	$(SHOW)gcov -c -b /* > /dev/null 2>&1
	$(SHOW)lcov -d . -c -o $(COV_DIR)/gcov.info --no-external > /dev/null 2>&1
	$(SHOW)lcov -r $(COV_DIR)/gcov.info "*test*" "*contrib*" "*redismodule.h" "*util.c*" -o $(COV_DIR)/gcov.info > /dev/null 2>&1
	$(SHOW)lcov -l $(COV_DIR)/gcov.info
	$(SHOW)genhtml --legend -o $(COV_DIR)/report $(COV_DIR)/gcov.info > /dev/null 2>&1

#----------------------------------------------------------------------------------------------

install: all
	$(SHOW)mkdir -p $(INSTALL_LIB)
	$(SHOW)$(INSTALL) $(TARGET) $(INSTALL_LIB)

uninstall:
	$(SHOW)rm -f $(INSTALL_LIB)/$(notdir $(TARGET))

docker:
	$(SHOW)cd .. && docker build -t redis-tsdb .

#----------------------------------------------------------------------------------------------

# arguments:
#   BRANCH: specifies branch names to serve as an exta package tag
#   INTO: package destination directory (optinal)

define ramp_pack # (1=tag, 2=subdir)
$(SHOW)ramp pack -m $(ROOT)/ramp.yml -o $(BINROOT)/redistimeseries.{os}-{architecture}.$1.zip $(abspath $(TARGET)) 2> /dev/null | grep '.zip' > /tmp/ramp_output
$(SHOW)realpath `tail -1 /tmp/ramp_output` > $(BINROOT)/../PACKAGE.$(1)
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

#----------------------------------------------------------------------------------------------
