
ROOT=../..

include $(ROOT)/deps/readies/mk/main

define HELPTEXT
make build    # configure and compile
make clean    # clean generated sbinaries
  ALL=1       # remote entire binary directory
endef

MK_ALL_TARGETS=build

#----------------------------------------------------------------------------------------------

ifeq ($(FAST_DOUBLE_PARSER_C_BINDIR),)
BINDIR=$(BINROOT)/fast_double_parser_c
else
override BINDIR=$(FAST_DOUBLE_PARSER_C_BINDIR)
endif

SRCDIR=$(ROOT)/src/fast_double_parser_c

FAST_DOUBLE_PARSER_DIR=$(ROOT)/deps/fast_double_parser

TARGET=$(BINDIR)/fast_double_parser_c.a

#----------------------------------------------------------------------------------------------

MK_CUSTOM_CLEAN=1

include $(MK)/defs

_SOURCES=\
	fast_double_parser_c.cpp

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.cpp,$(BINDIR)/%.o,$(SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.cpp, $(BINDIR)/%.d, $(SOURCES))

define CC_INCLUDES +=
	$(SRCDIR)
	$(BINDIR)
	$(FAST_DOUBLE_PARSER_DIR)/include
endef

CC_CXX_STD=c++11

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
