
ROOT=../..

include $(ROOT)/deps/readies/mk/main

define HELPTEXT
make build    # configure and compile
make clean    # clean generated sbinaries
  ALL=1       # remote entire binary directory
endef

#----------------------------------------------------------------------------------------------

ifeq ($(DRAGONBOX_BINDIR),)
override BINDIR=$(DRAGONBOX_BINDIR)
else
BINDIR=$(BINROOT)/dragonbox
endif

SRCDIR=$(ROOT)/deps/dragonbox

TARGET=$(BINDIR)/dragonbox.a

#----------------------------------------------------------------------------------------------

MK_CUSTOM_CLEAN=1

CC_CXX_STD=c++17

include $(MK)/defs

_SOURCES=dragonbox.cpp

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.cpp,$(BINDIR)/%.o,$(SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.cpp, $(BINDIR)/%.d, $(SOURCES))

#----------------------------------------------------------------------------------------------

include $(MK)/rules

-include $(CC_DEPS)

$(BINDIR)/%.o: $(SRCDIR)/%.cpp
	@echo Compiling $<...
	$(SHOW)$(CXX) $(CC_FLAGS) $(CC_CXX_FLAGS) -c $< -o $@

$(TARGET): $(OBJECTS)
	@echo Creating $@...
	$(SHOW)$(AR) rcs $@ $<

clean:
ifeq ($(ALL),1)
	$(SHOW)rm -rf $(BINDIR) $(TARGET)
else
	-$(SHOW)find $(BINDIR) -name '*.[oadh]' -type f -delete
	$(SHOW)rm -f $(TARGET)
endif
