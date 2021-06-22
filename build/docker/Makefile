
ROOT=../..
MK.pyver:=3

include $(ROOT)/deps/readies/mk/main

# OSNICK=bullseye|buster|stretch|hirsute|focal|bionic|xenial|centos8|centos7
OSNICK ?= buster

REPO=redistimeseries

REDIS_VER=6.2.4

ART_DIR=$(ROOT)/bin/artifacts
ART_INT_DIR=/var/opt/redislabs/artifacts

include $(MK)/docker.native
