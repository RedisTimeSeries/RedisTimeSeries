
ROOT=../..
MK.pyver:=3
include $(ROOT)/deps/readies/mk/main

define HELPTEXT
make build    # configure and compile
make clean    # clean generated sbinaries
  ALL=1       # remote entire binary directory
endef

#----------------------------------------------------------------------------------------------

BINDIR=$(BINROOT)/dragonbox
SRCDIR=$(ROOT)/deps/dragonbox

TARGET=$(BINDIR)/dragonbox.a

#----------------------------------------------------------------------------------------------

MK_CUSTOM_CLEAN=1

include $(MK)/defs

CC=gcc
CXX=g++

_SOURCES=dragonbox.cpp

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.cpp,$(BINDIR)/%.o,$(SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.cpp, $(BINDIR)/%.d, $(SOURCES))

CC_FLAGS += \
	-fPIC \
	-MMD -MF $(@:.o=.d) \
	-I$(SRCDIR) \
	-I$(BINDIR) \
	$(CC_FLAGS.coverage)

CXX_FLAGS += \
	-std=c++17

ifeq ($(DEBUG),1)
CC_FLAGS += -g -O0
LD_FLAGS += -g
else
CC_FLAGS += -O3 -flto -Wno-unused-result
endif

ifeq ($(OS),macos)
LD_FLAGS += -undefined dynamic_lookup
endif

LD_FLAGS += $(LD_FLAGS.coverage)

#----------------------------------------------------------------------------------------------

include $(MK)/rules

-include $(CC_DEPS)

$(BINDIR)/%.o: $(SRCDIR)/%.cpp
	@echo Compiling $<...
	$(SHOW)$(CXX) $(CC_FLAGS) $(CXX_FLAGS) -fvisibility=hidden -c $< -o $@

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
