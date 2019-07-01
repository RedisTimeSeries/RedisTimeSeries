.PHONY: all setup fetch build test pack

all: setup fetch build

setup:
	@./deps/readies/bin/getpy2
	@./system-setup.py

fetch:
	@git submodule update --init --recursive

build:
	@make -C src all -j $(nproc)

test:
	@make -C src tests

pack:
	@make -C src package
