
ROOT=../..
include $(ROOT)/deps/readies/mk/main

MK_CUSTOM_CLEAN=1

BINDIR=$(BINROOT)/rmutil
SRCDIR=$(ROOT)/deps/RedisModulesSDK/rmutil

CC=gcc

CC_FLAGS=\
	-g \
	-O3 \
	-I$(SRCDIR)/.. \
	-Wall \
	-fPIC \
	-Wno-unused-function \
	-std=gnu99 \
	-MMD -MF $(@:.o=.d)

_SOURCES=util.c strings.c sds.c vector.c alloc.c periodic.c

SOURCES=$(addprefix $(SRCDIR)/,$(_SOURCES))
OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.c, $(BINDIR)/%.d, $(SOURCES))

TARGET=$(BINDIR)/librmutil.a

include $(MK)/defs

#----------------------------------------------------------------------------------------------

.PHONY: all clean test_vector test_periodic test

all: bindirs $(TARGET)

include $(MK)/rules

clean:
	-$(SHOW)[ -e $(BINDIR) ] && find $(BINDIR) -name '*.[oadh]' -type f -delete

-include $(CC_DEPS)

$(BINDIR)/%.o: $(SRCDIR)/%.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) -c $< -o $@

$(TARGET): $(OBJECTS)
	@echo Creating $@...
	$(SHOW)ar rcs $@ $^

#----------------------------------------------------------------------------------------------

$(BINDIR)/test_vector: test_vector.o vector.o
	$(CC) -Wall -o $@ $^ -lc -lpthread -O0
	sh -c ./$@

$(BINDIR)/test_periodic: test_periodic.o periodic.o
	$(CC) -Wall -o $@ $^ -lc -lpthread -O0
	sh -c ./$@
	
test: test_periodic test_vector
