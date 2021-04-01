ROOT=./../..
MK.pyver:=3

include $(ROOT)/deps/readies/mk/main

BINDIR=$(BINROOT)/fast_double_parser_c
SRCDIR=$(ROOT)/src/fast_double_parser_c
 
FAST_DOUBLE_PARSER_LIBDIR=$(ROOT)/deps/fast_double_parser/include
TARGET=$(BINDIR)/libfast_double_parser_c.a

CXXFLAGS=\
	-fPIC \
	-Wall \
	-Wextra \
	-std=c++11 \
	-I$(FAST_DOUBLE_PARSER_LIBDIR)

ifeq ($(PROFILE),1)
CXXFLAGS += -g -ggdb -fno-omit-frame-pointer
endif

ifeq ($(DEBUG),1)
CXXFLAGS += -g -ggdb -O0
else
ifeq ($(PROFILE),1)
CXXFLAGS += -O2
else
CXXFLAGS += -O3
endif
endif

all: $(TARGET)

$(TARGET):
	@echo Compiling $<...
	$(SHOW)mkdir -p  $(BINDIR)
	$(SHOW)$(CXX) $(CXXFLAGS) -c $(SRCDIR)/fast_double_parser_c.cpp -o $(BINDIR)/fast_double_parser_c.o
	@echo Creating $@...
	$(SHOW)ar rcs $@ $(BINDIR)/fast_double_parser_c.o
	
clean:
	$(SHOW)rm -f $(BINDIR)/libfast_double_parser_c.a $(BINDIR)/*.o

lint:
	$(SHOW)clang-format -Werror -n fast_double_parser_c.cpp
	$(SHOW)clang-format -Werror -n fast_double_parser_c.h

format:
	$(SHOW)clang-format -i fast_double_parser_c.cpp
	$(SHOW)clang-format -i fast_double_parser_c.h
