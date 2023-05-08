
ROOT=../..

MK.cmake=1

include $(ROOT)/deps/readies/mk/main

#----------------------------------------------------------------------------------------------

define HELPTEXT
make build          # compile and link
  DEBUG=1             # build for debugging
  WHY=1               # explain CMake decisions (in /tmp/cmake-why)
  FORCE=1             # Force CMake rerun (default)
  CMAKE_ARGS=...      # extra arguments to CMake
  SLOW=1              # do not parallelize build (for diagnostics)
make clean         # remove build artifacts
  ALL=1              # remove entire artifacts directory

endef

#----------------------------------------------------------------------------------------------

ifeq ($(CPU_FEATURES_BINDIR),)
BINDIR=$(BINROOT)/cpu_features
else
override BINDIR=$(CPU_FEATURES_BINDIR)
endif

TARGET=$(BINDIR)/libcpu_features.a

#----------------------------------------------------------------------------------------------

SRCDIR=$(ROOT)/deps/cpu_features
CMAKE_TARGETS=cpu_features
MK_CMAKE_FILES=$(CURDIR)/CMakeLists.txt

CMAKE_DEFS += \
	BUILD_TESTING=off \
	CMAKE_POSITION_INDEPENDENT_CODE=on

#----------------------------------------------------------------------------------------------

include $(MK)/defs

MK_CUSTOM_CLEAN=1

#----------------------------------------------------------------------------------------------

all: bindirs $(TARGET)

include $(MK)/rules

.PHONY: clean

clean:
ifeq ($(ALL),1)
	$(SHOW)rm -rf $(BINDIR)
else
	$(SHOW)$(MAKE) -C $(BINDIR) clean
endif

#----------------------------------------------------------------------------------------------
