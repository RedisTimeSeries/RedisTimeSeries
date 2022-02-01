
ROOT=.

MK_ALL_TARGETS=bindirs deps build package

ifeq ($(wildcard $(ROOT)/deps/readies/*),)
___:=$(shell git submodule update --init --recursive &> /dev/null)
endif

MK.pyver:=3
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
	@$(MAKE) -C src pack

run:
	@$(MAKE) -C src run

benchmark:
	@$(MAKE) -C src benchmark

docker:
	@$(MAKE) -C src docker

# deploy:
#	@make -C src deploy
