
ROOT=../..

include $(ROOT)/deps/readies/mk/main

define HELPTEXT
make build    # configure and compile
make clean    # clean generated sbinaries
  ALL=1       # remote entire binary directory
endef

MK_ALL_TARGETS=build

#----------------------------------------------------------------------------------------------

ifeq ($(HIREDIS_BINDIR),)
override BINDIR=$(HIREDIS_BINDIR)
else
BINDIR=$(BINROOT)/hiredis
endif

SRCDIR=$(ROOT)/deps/LibMR/deps/hiredis

TARGET=$(BINDIR)/libhiredis.a

SSL ?= 1

#----------------------------------------------------------------------------------------------

MK_CUSTOM_CLEAN=1

CC_C_STD=gnu99

include $(MK)/defs

_SOURCES=alloc.c async.c dict.c hiredis.c net.c read.c sds.c sockcompat.c
ifeq ($(SSL),1)
_SOURCES += ssl.c
endif

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.c, $(BINDIR)/%.d, $(SOURCES))

define CC_INCLUDES +=
	$(SRCDIR)
	$(BINDIR)
endef

ifeq ($(SSL),1)
CC_INCLUDES += $(openssl_prefix)/include
endif

#----------------------------------------------------------------------------------------------

include $(MK)/rules

-include $(CC_DEPS)

$(BINDIR)/%.o: $(SRCDIR)/%.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) -fvisibility=hidden -c $< -o $@

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
