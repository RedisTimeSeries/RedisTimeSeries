
ROOT=.
MK.pyver:=3

MK_ALL_TARGETS=bindirs deps build package

ifeq ($(wildcard $(ROOT)/deps/readies/mk),)
$(error Submodules not present. Please run 'git submodule update --init --recursive')
endif
include $(ROOT)/deps/readies/mk/main

MK_CUSTOM_CLEAN=1
MK_CUSTOM_HELP=1
BINDIR=$(BINROOT)

include $(MK)/defs
include $(MK)/rules

.PHONY: all setup fetch build clean deps test pack help

all: fetch deps build

help:
	@$(MAKE) -C src help

setup:
	@echo Setting up system...
	@./deps/readies/bin/getpy3
	@./system-setup.py

fetch:
	-@git submodule update --init --recursive

build:
	@$(MAKE) -C src all -j $(NCPUS)

clean:
	@$(MAKE) -C src clean

deps:
	@$(MAKE) -C src deps

lint:
	@$(MAKE) -C src lint

format:
	@$(MAKE) -C src format

test:
	@$(MAKE) -C src unit_tests flow_tests

unit_tests:
	@$(MAKE) -C src unit_tests

flow_tests:
	@$(MAKE) -C src flow_tests

pack:
	@$(MAKE) -C src package

run:
	@$(MAKE) -C src run

valgrind:
	@$(MAKE) -C src $@

run:
	@$(MAKE) -C src $@

gdb:
	@$(MAKE) -C src $@


benchmark:
	@$(MAKE) -C src benchmark

# deploy:
#	@make -C src deploy
