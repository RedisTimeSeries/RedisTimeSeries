
ROOT=../..

include $(ROOT)/deps/readies/mk/main

define HELPTEXT
make build    # configure and compile
make clean    # clean generated binaries
  ALL=1       # remote entire binary directory
endef

MK_ALL_TARGETS=build

#----------------------------------------------------------------------------------------------

MODULE_NAME=timeseries

ifeq ($(LIBMR_BINDIR),)
override BINDIR=$(LIBMR_BINDIR)
else
BINDIR=$(BINROOT)/LibMR
endif

SRCDIR=$(ROOT)/deps/LibMR/src

TARGET=$(BINDIR)/libmr.a

SSL ?= 1

#----------------------------------------------------------------------------------------------

MK_CUSTOM_CLEAN=1

CC_C_STD=gnu99

include $(MK)/defs

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

define CC_DEFS +=
	REDISMODULE_EXPERIMENTAL_API
	MODULE_NAME=$(MODULE_NAME)
endef

define CC_INCLUDES +=
	$(SRCDIR)
	$(BINDIR)
	$(ROOT)/deps/LibMR/deps/hiredis
	$(ROOT)/deps/LibMR/deps/hiredis/adapters
	$(ROOT)/deps/LibMR/deps/libevent/include
	$(LIBEVENT_BINDIR)/include
endef

ifeq ($(SSL),1)
CC_INCLUDES += $(openssl_prefix)/include
endif

#----------------------------------------------------------------------------------------------

include $(MK)/rules

-include $(CC_DEPS)

$(BINDIR)/%.o: $(SRCDIR)/%.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) $(CC_C_FLAGS) -c $< -o $@

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
