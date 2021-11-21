
ifeq ($(VG),1)
override VALGRIND:=1
export VALGRIND
endif

ifeq ($(VALGRIND),1)
override DEBUG:=1
export DEBUG
endif

ROOT=../..
MK.pyver:=3
include $(ROOT)/deps/readies/mk/main

define HELPTEXT
make build    # configure and compile
make clean    # clean generated sbinaries
  ALL=1       # remote entire binary directory
endef

MK_ALL_TARGETS=build

#----------------------------------------------------------------------------------------------

BINDIR=$(BINROOT)/unit_tests
SRCDIR=$(ROOT)/tests/unit

TARGET=$(BINDIR)/unit_tests

#----------------------------------------------------------------------------------------------

MK_CUSTOM_CLEAN=1

include $(MK)/defs

CC=gcc

_SOURCES=unittests.c

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.c, $(BINDIR)/%.d, $(SOURCES))

CC_FLAGS += \
	-fPIC \
	-std=gnu99 \
	-MMD -MF $(@:.o=.d) \
	-I$(SRCDIR) \
	-I$(BINDIR) \
	-I$(ROOT)/deps \
	-I$(ROOT)/deps/RedisModulesSDK \
	-I$(ROOT)/src \
	-I$(ROOT)/deps/minunit \
	-I/usr/local/opt/openssl/include \
	$(CC_FLAGS.coverage)

ifeq ($(DEBUG),1)
CC_FLAGS += -g -O0
LD_FLAGS += -g
else
CC_FLAGS += -O2 -Wno-unused-result
endif

ifeq ($(OS),macos)
LD_FLAGS += -undefined dynamic_lookup
endif

LD_FLAGS += $(LD_FLAGS.coverage)

LD_LIBS += $(realpath $(BINROOT)/redistimeseries.so)

#----------------------------------------------------------------------------------------------

include $(MK)/rules

-include $(CC_DEPS)

$(BINDIR)/%.o: $(SRCDIR)/%.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) -fvisibility=hidden -c $< -o $@

$(TARGET): $(OBJECTS)
	@echo Creating $@...
	$(SHOW)$(CC) $(LD_FLAGS) -o $@ $(OBJECTS) $(LD_LIBS)

clean:
ifeq ($(ALL),1)
	$(SHOW)rm -rf $(BINDIR) $(TARGET)
else
	-$(SHOW)find $(BINDIR) -name '*.[oadh]' -type f -delete
	$(SHOW)rm -f $(TARGET)
endif

run: $(TARGET)
	@echo Running unit tests ...
	$(SHOW)$<

#----------------------------------------------------------------------------------------------

lint:
	$(SHOW)clang-format -Werror -n $(SOURCES) $(patsubst %.cpp,%.h,$(SOURCES))

format:
	$(SHOW)clang-format -i $(SOURCES) $(patsubst %.cpp,%.h,$(SOURCES))
