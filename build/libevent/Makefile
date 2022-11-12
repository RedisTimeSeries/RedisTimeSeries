
.NOTPARALLEL:

ROOT=../..

include $(ROOT)/deps/readies/mk/main

MK.configure=1

define HELPTEXT
make build    # configure and compile
make clean    # clean generated sbinaries
  ALL=1       # remote entire binary directory
make source   # fetch sources and generate configure script
endef

#----------------------------------------------------------------------------------------------

ifeq ($(LIBEVENT_BINDIR),)
override BINDIR=$(LIBEVENT_BINDIR)
else
BINDIR=$(BINROOT)/libevent
endif

SRCDIR=$(ROOT)/deps/LibMR/deps/libevent

TARGET=$(BINDIR)/.libs/libevent.a

export CFLAGS += -fPIC

#----------------------------------------------------------------------------------------------

MK_CUSTOM_CLEAN=1

include $(MK)/defs
include $(MK)/rules

#----------------------------------------------------------------------------------------------

source: $(SRCDIR)/configure

.PHONY: source

$(SRCDIR)/configure: $(BIN_DIRS) $(SRCDIR)/configure.ac
	$(SHOW)cd $(SRCDIR); ./autogen.sh

clean:
ifeq ($(ALL),1)
	$(SHOW)-rm -rf $(CONFIGURE_BUILD_DIR)
else
	$(SHOW)[[ -d $(CONFIGURE_BUILD_DIR) ]] && $(MAKE) -C $(CONFIGURE_BUILD_DIR) clean
endif
ifeq ($(AUTOGEN),1)
	$(SHOW)cd $(SRCDIR); git ls-files -o | xargs -r rm
endif
