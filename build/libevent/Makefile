
ROOT=../..
MK.pyver:=3
include $(ROOT)/deps/readies/mk/main

MK.configure=1

define HELPTEXT
make build    # configure and compile
make clean    # clean generated sbinaries
  ALL=1       # remote entire binary directory
make source   # fetch sources and generate configure script
endef

#----------------------------------------------------------------------------------------------

BINDIR=$(BINROOT)/libevent
SRCDIR=$(ROOT)/deps/LibMR/deps/libevent

TARGET=$(BINDIR)/.libs/libevent.a

export CFLAGS += -fPIC

#----------------------------------------------------------------------------------------------

include $(MK)/defs
include $(MK)/rules

#----------------------------------------------------------------------------------------------

source: $(SRCDIR)/configure

.PHONY: source

$(SRCDIR)/configure:
	$(SHOW)cd $(SRCDIR); ./autogen.sh
