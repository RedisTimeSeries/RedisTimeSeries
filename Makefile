
.PHONY: all setup fetch build test pack

all: fetch build

setup:
	@echo Setting up system...
	@./deps/readies/bin/getpy2
	@python ./system-setup.py

fetch:
	-@git submodule update --init --recursive

build:
	@make -C src all -j

clean:
	@make -C src clean

test:
	@make -C src tests

pack:
	@make -C src package

# deploy:
#	@make -C src deploy
