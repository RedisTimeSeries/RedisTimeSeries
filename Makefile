
ROOT=.
include $(ROOT)/deps/readies/mk/main

MK_CUSTOM_CLEAN=1
BINDIR=$(BINROOT)


ifeq ($(wildcard $(MK)/rules),)
  $(error Please run 'git submodule update --init --recursive' first!)
endif

include $(MK)/defs
include $(MK)/rules

.PHONY: all setup fetch build clean test pack help

all: fetch build

help:
	@$(MAKE) -C src help

setup:
	@./deps/readies/bin/getpy2
	@./system-setup.py

fetch:
	-@git submodule update --init --recursive

build:
	@$(MAKE) -C src all -j $(NCPUS)

clean:
	@$(MAKE) -C src clean

test:
	@$(MAKE) -C src tests

pack:
	@$(MAKE) -C src package

# deploy:
#	@make -C src deploy
