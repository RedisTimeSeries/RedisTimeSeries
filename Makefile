
.PHONY: all setup fetch build test pack

all: fetch build

SUDO:=$(shell [ $$( command -v sudo >/dev/null 2>&1; echo $$? ) = 0 ] && echo sudo)

setup:
	@$(SUDO) ./deps/readies/bin/getpy2
	@$(SUDO) ./system-setup.py

fetch:
	-@git submodule update --init --recursive

build:
	@make -C src all -j

test:
	@make -C src tests

pack:
	@make -C src package

# deploy:
#	@make -C src deploy
