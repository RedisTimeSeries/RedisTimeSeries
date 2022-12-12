
.NOTPARALLEL:

ROOT=.

MK_ALL_TARGETS=bindirs deps build package

include $(ROOT)/deps/readies/mk/main

MK_CUSTOM_CLEAN=1
MK_CUSTOM_HELP=1
BINDIR=$(BINROOT)

include $(MK)/defs
include $(MK)/rules

.PHONY: all setup fetch build clean deps test coverage upload-cov pack upload-release upload-artifacts help

all:
	@$(MAKE) -C src bindirs
	@$(MAKE) -C src deps NOPAR=1 -j $(NPROC)
	@$(MAKE) -C src build -j $(NPROC)
	@$(MAKE) -C src pack

help:
	@$(MAKE) -C src help

setup:
	@echo Setting up system...
	@./sbin/setup

fetch:
	-@git submodule update --init --recursive

build:
	@$(MAKE) -C src bindirs
	@$(MAKE) -C src deps NOPAR=1 -j $(NPROC)
	@$(MAKE) -C src build -j $(NPROC)

clean:
	@$(MAKE) -C src clean

deps:
	@$(MAKE) -C src deps

lint:
	@$(MAKE) -C src lint

format:
	@$(MAKE) -C src format

test:
	@$(MAKE) -C src test

unit_tests:
	@$(MAKE) -C src unit_tests

flow_tests:
	@$(MAKE) -C src flow_tests

clean-tests:
	@$(MAKE) -C src clean-tests

coverage:
	@$(MAKE) -C src coverage

upload-cov:
	@$(MAKE) -C src upload-cov

pack:
	@$(MAKE) -C src pack

upload-release:
	@$(MAKE) -C src upload-release

upload-artifacts:
	@$(MAKE) -C src upload-artifacts

run:
	@$(MAKE) -C src run

benchmark:
	@$(MAKE) -C src benchmark

docker:
	@$(MAKE) -C src docker
