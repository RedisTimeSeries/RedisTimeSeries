
.PHONY: all setup fetch build clean test pack

all: fetch build

setup:
	@./deps/readies/bin/getpy2
	@./system-setup.py

fetch:
	-@git submodule update --init --recursive

build:
	@$(MAKE) -C src all -j

clean:
	@$(MAKE) -C src clean

test:
	@$(MAKE) -C src tests

pack:
	@$(MAKE) -C src package

# deploy:
#	@make -C src deploy
