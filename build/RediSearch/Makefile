
ROOT=../..

BINDIR=$(BINROOT)/RediSearch
SRCDIR=$(ROOT)/deps/RediSearch

TARGET=$(BINDIR)/libredisearch.a

CMAKE_FLAGS=\
	-DRS_BUILD_STATIC=ON

include $(ROOT)/build/mk/defs

include $(MK)/rules

$(BINDIR)/Makefile : $(SRCDIR)/CMakeLists.txt
	$(SHOW)cd $(BINDIR); cmake $(CMAKE_FLAGS) $(realpath $(SRCDIR))

build: $(TARGET)

$(TARGET): $(BINDIR)/Makefile
	$(SHOW)$(MAKE) -C $(BINDIR)

ifeq ($(wildcard $(BINDIR)/Makefile),)
clean: ;
else
clean:
ifeq ($(FORCE),1)
	$(SHOW)rm -rf $(BINDIR)
else
	$(SHOW)$(MAKE) -C $(BINDIR) clean
endif
endif