
ROOT=../..
include $(ROOT)/deps/readies/mk/main

MK_CUSTOM_CLEAN=1

BINDIR=$(BINROOT)/googletest
SRCDIR=$(ROOT)/deps/googletest
BUILDDIR=$(ROOT)/deps/googletest/build

TARGET=$(BINDIR)/libgoogletest.a

include $(MK)/defs

#----------------------------------------------------------------------------------------------

.PHONY: all clean

all: bindirs $(TARGET)

include $(MK)/rules

clean:
	-$(SHOW)[ -e $(BINDIR) ] && find $(BINDIR) -name '*.[oadh]' -type f -delete
	rm -rf $(BUILDDIR)/*

-include $(CC_DEPS)

libgtest:
	@echo Building $@...
	mkdir -p $(BUILDDIR) && cd $(BUILDDIR) && cmake .. 
	$(SHOW)$(MAKE) -C $(BUILDDIR)
	mkdir -p $(BINDIR)
	cp $(BUILDDIR)/lib/libgtest_main.a $(BINDIR)
	cp $(BUILDDIR)/lib/libgtest.a $(BINDIR)

$(TARGET): libgtest

