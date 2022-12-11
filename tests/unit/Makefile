
ROOT=../..

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

_SOURCES=unittests.c

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.c, $(BINDIR)/%.d, $(SOURCES))

define CC_INCLUDES +=
	$(SRCDIR)
	$(BINDIR)
	$(ROOT)/deps
	$(ROOT)/deps/RedisModulesSDK
	$(ROOT)/src
	$(ROOT)/deps/minunit
endef

CC_FLAGS += \
	-std=gnu11

ifeq ($(OS),macos)
LD_FLAGS += -undefined dynamic_lookup
LD_SHARED_EXT=dylib
else
LD_SHARED_EXT=so
endif

LD_LIBS += $(realpath $(BINROOT)/redistimeseries.$(LD_SHARED_EXT))

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
