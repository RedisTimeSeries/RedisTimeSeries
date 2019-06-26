.PHONY: setup fetch build test pack

setup:
	@./system-setup.py

fetch:
	@git submodule update --init --recursive

build:
	@make -C src all

test:
	@make -C src tests

pack:
	@make -C src package
