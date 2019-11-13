
.PHONY: all setup fetch build test pack

all: fetch build

setup:
	@./deps/readies/bin/getpy2
	@sudo python2 ./system-setup.py

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
