ROOT=..
include $(ROOT)/build/mk/defs

BINDIR=$(BINROOT)/src
SRCDIR=.

#----------------------------------------------------------------------------------------------  

PREFIX ?= /usr/local
INSTALL_LIB:=$(PREFIX)/lib
INSTALL:=/usr/bin/install
PAKCAGE_NAME ?= $(BINROOT)/redistimeseries.{os}-{architecture}.latest.zip

# set environment variable RM_INCLUDE_DIR to the location of redismodule.h
SDK_DIR=$(ROOT)/deps/RedisModulesSDK
RMUTIL_LIBDIR=$(BINROOT)/rmutil

#----------------------------------------------------------------------------------------------

TARGET=$(BINROOT)/redistimeseries.so
UNITTESTS_RUNNER=$(BINROOT)/unittests_runner

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

LD_FLAGS += 
LD_LIBS += -lc -lm -L$(RMUTIL_LIBDIR) -lrmutil

ifeq ($(OS),linux)
SO_LD_FLAGS += -shared -Bsymbolic $(LD_FLAGS)
endif

ifeq ($(OS),macosx) 
CC_FLAGS += -dynamic
SO_LD_FLAGS += -bundle -undefined dynamic_lookup $(LD_FLAGS)
endif

ifeq ($(DEBUG),1)
CC_FLAGS += -g -ggdb -O0 -DVALGRIND
LD_FLAGS += -g
else
CC_FLAGS += -O3
endif

_SOURCES=\
	chunk.c \
	compaction.c \
	config.c \
	indexer.c \
	module.c \
	parse_policies.c \
	rdb.c \
	tsdb.c

_TEST_SOURCES=\
	tests.c \
	endianconv.c

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

TEST_SOURCES=$(addprefix $(SRCDIR)/,$(_TEST_SOURCES))
TEST_OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(TEST_SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.c, $(BINDIR)/%.d, $(SOURCES) $(TEST_SOURCES))

#----------------------------------------------------------------------------------------------

all: bindirs $(TARGET)

include $(MK)/rules

.PHONY: package tests unittests clean all install uninstall docker

rmutil:
	@echo Building $@...
	$(SHOW)$(MAKE) -C $(ROOT)/build/rmutil

clean:
	-$(SHOW)find $(BINDIR) -name '*.[oadh]' -type f -delete
	-$(SHOW)$(MAKE) -C $(ROOT)/build/rmutil clean
	-$(SHOW)rm -f $(TARGET) $(UNITTESTS_RUNNER)

-include $(CC_DEPS)

$(BINDIR)/%.o: $(SRCDIR)/%.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) -c $< -o $@

$(TARGET): rmutil $(OBJECTS)
	@echo Linking $@...
	$(SHOW)$(CC) -shared $(SO_LD_FLAGS) -o $@ $(OBJECTS) $(LD_LIBS)
ifneq ($(DEBUG),1)
	$(SHOW)strip $@
endif
	$(SHOW)cd $(BINROOT)/..; ln -sf $(FULL_VARIANT)/$(notdir $(TARGET)) $(notdir $(TARGET))

#----------------------------------------------------------------------------------------------

tests: $(TARGET)
	$(SHOW)cd ../tests; \
	REDISTIMESERIES=$(TARGET) python2 -m pytest --junitxml=$$PWD/tests_report.xml ${EXTRA_ARGS} -svv .

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

$(UNITTESTS_RUNNER)	: $(TARGET) $(OBJECTS) $(TEST_OBJECTS)
	$(SHOW)$(CC) $(LD_FLAGS) -o $@ $(OBJECTS) $(TEST_OBJECTS) $(LD_LIBS)

unittests: $(UNITTESTS_RUNNER)	
	@echo Running unit tests...
	$(SHOW)$<

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
$(SHOW)ramp pack -m "$(ROOT)/ramp.yml" -v -o "$(BINROOT)/redistimeseries.{os}-{architecture}.$1.zip" $(TARGET) 2> /dev/null | grep '.zip' > /tmp/ramp_output
$(SHOW)tail -1 /tmp/ramp_output > $(BINROOT)/../PACKAGE.$(1)
$(SHOW)$(ROOT)/src/clean_ramp.py `cat $(BINROOT)/../PACKAGE.$(1)`
$(SHOW)[[ ! -z $(INTO) ]] && { mkdir -p $(INTO)/$(2); cp `cat $(BINROOT)/../PACKAGE.$(1)` $(INTO)/$(2); }
endef

package: $(TARGET)
	$(call ramp_pack,release,latest)
	$(call ramp_pack,release,{semantic_version})
ifneq ($(BRANCH),)
	$(call ramp_pack,branch,$(BRANCH))
endif
