
ROOT=../..
MK.pyver:=3

include $(ROOT)/deps/readies/mk/main

# OSNICK=bullseye|focal|bionic|xenial|centos8|centos7
OSNICK ?= bullseye

REPO=redistimeseries

REDIS_VER=6.2.5

INT_BRANCHES=1.4 1.2 1.0

ART_DIR=$(ROOT)/bin/artifacts
ART_INT_DIR=/var/opt/redislabs/artifacts

include $(MK)/docker.native
