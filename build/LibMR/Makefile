
ROOT=../..
MK.pyver:=3
include $(ROOT)/deps/readies/mk/main

define HELPTEXT
make build    # configure and compile
make clean    # clean generated binaries
  ALL=1       # remote entire binary directory
endef

MK_ALL_TARGETS=build

#----------------------------------------------------------------------------------------------

MODULE_NAME=timeseries

BINDIR=$(BINROOT)/LibMR
SRCDIR=$(ROOT)/deps/LibMR/src

TARGET=$(BINDIR)/libmr.a

#----------------------------------------------------------------------------------------------

MK_CUSTOM_CLEAN=1

include $(MK)/defs

CC=gcc

_SOURCES=\
	cluster.c \
	crc16.c \
	event_loop.c \
	mr.c \
	record.c \
	utils/adlist.c \
	utils/buffer.c \
	utils/dict.c \
	utils/siphash.c \
	utils/thpool.c

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.d,$(SOURCES))

CC_FLAGS += \
	-fvisibility=hidden \
	-fPIC \
	-std=gnu99 \
	-MMD -MF $(@:.o=.d) \
	-DREDISMODULE_EXPERIMENTAL_API \
	-DMODULE_NAME=$(MODULE_NAME) \
	-I$(SRCDIR) \
	-I$(BINDIR) \
	-I$(ROOT)/deps/LibMR/deps/hiredis \
	-I$(ROOT)/deps/LibMR/deps/hiredis/adapters \
	-I$(ROOT)/deps/LibMR/deps/libevent/include \
	-I$(BINROOT)/libevent/include \
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

#----------------------------------------------------------------------------------------------

include $(MK)/rules

-include $(CC_DEPS)

$(BINDIR)/%.o: $(SRCDIR)/%.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) -c $< -o $@

$(TARGET): $(OBJECTS)
	@echo Creating $@...
	$(SHOW)$(AR) rcs $@ $(OBJECTS)

clean:
ifeq ($(ALL),1)
	$(SHOW)rm -rf $(BINDIR) $(TARGET)
else
	-$(SHOW)find $(BINDIR) -name '*.[oadh]' -type f -delete
	$(SHOW)rm -f $(TARGET)
endif
