
ROOT=../..
MK.pyver:=3

include $(ROOT)/deps/readies/mk/main

# OSNICK=buster|stretch|bionic|xenial|centos7
OSNICK ?= buster

REPO=redisfab
STEM=$(REPO)/redistimeseries

REDIS_VER=6.2.3

ART_DIR=$(ROOT)/artifacts
ART_INT_DIR=/var/opt/redislabs/artifacts

include $(MK)/docker
