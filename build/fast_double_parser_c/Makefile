
ROOT=../..
MK.pyver:=3
include $(ROOT)/deps/readies/mk/main

define HELPTEXT
make build    # configure and compile
make clean    # clean generated sbinaries
  ALL=1       # remote entire binary directory
endef

MK_ALL_TARGETS=build

#----------------------------------------------------------------------------------------------

BINDIR=$(BINROOT)/fast_double_parser_c
SRCDIR=$(ROOT)/src/fast_double_parser_c

FAST_DOUBLE_PARSER_DIR=$(ROOT)/deps/fast_double_parser

TARGET=$(BINDIR)/fast_double_parser_c.a

#----------------------------------------------------------------------------------------------

MK_CUSTOM_CLEAN=1

include $(MK)/defs

CXX=g++

_SOURCES=\
	fast_double_parser_c.cpp

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.cpp,$(BINDIR)/%.o,$(SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.cpp, $(BINDIR)/%.d, $(SOURCES))

CC_FLAGS += \
	-fPIC \
	-Wall \
	-Wextra \
	-MMD -MF $(@:.o=.d) \
	-I$(SRCDIR) \
	-I$(BINDIR) \
	-I$(FAST_DOUBLE_PARSER_DIR)/include

CXX_FLAGS=\
	-std=c++11

ifeq ($(PROFILE),1)
CC_FLAGS += -g -ggdb -fno-omit-frame-pointer
endif

ifeq ($(DEBUG),1)
CC_FLAGS += -g -ggdb -O0
LD_FLAGS += -g
else ifeq ($(PROFILE),1)
CC_FLAGS += -O2
else
CC_FLAGS += -O3 -flto
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
	$(SHOW)$(CXX) $(CC_FLAGS) $(CXX_FLAGS) -c $< -o $@

$(TARGET): $(BIN_DIRS) $(OBJECTS)
	@echo Creating $@...
	$(SHOW)$(AR) rcs $@ $(OBJECTS)

clean:
ifeq ($(ALL),1)
	$(SHOW)rm -rf $(BINDIR) $(TARGET)
else
	-$(SHOW)find $(BINDIR) -name '*.[oadh]' -type f -delete
	$(SHOW)rm -f $(TARGET)
endif

lint:
	$(SHOW)clang-format -Werror -n $(SOURCES) $(patsubst %.cpp,%.h,$(SOURCES))

format:
	$(SHOW)clang-format -i $(SOURCES) $(patsubst %.cpp,%.h,$(SOURCES))
