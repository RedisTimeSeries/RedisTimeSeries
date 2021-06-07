# Feel free to change these
PRODUCT=RedisTimeSeries
DOCKER_ORG=redislabs
REDIS_VERSION=6.2.3
DOCKER_OPTS=  # set, to pass custom options to docker, but remember to quote them
OSNICK ?= bionic
DOCKER_SOURCES="" # set to pass multiple directories as docker sources

# setting any of the below to 1 activates them
# see readies/bin/dockerwrapper
NOP=0
PUBLISH=0
KEEP=0
VERBOSE=0
DOCKER_TAGS=


# e.g redislabs/RedisTimeSeries:edge
DEFAULT_TAG=${DOCKER_ORG}/${PRODUCT}:${VERSION}

build:
	DOCKER_SOURCES="${DOCKER_SOURCES}" \
	DOCKER_OPTS="${DOCKER_OPTS}" \
	DOCKER_TAGS="${DOCKER_TAGS}" \
	NOP=${NOP} \
	DOCKER_PUSH=${PUBLISH} \
	VERBOSE=${VERBOSE} \
	KEEP=${KEEP} \
	REDIS_VERSION=${REDIS_VERSION} \
	REDIS_PRODUCT=${PRODUCT} \
	REDIS_DOCKER_ORG=${DOCKER_ORG} \
	REDIS_OSNICK=${OSNICK} \
	REDIS_OS=${OS} \
	REDIS_ARCH=${ARCH} \
	${READIES}/bin/dockerwrapper \
		-d ${ROOT}/Dockerfile \
		-t ${DEFAULT_TAG} \
		-S ${DOCKER_SOURCES} \
		-e REDIS \
		-D "${DOCKER_OPTS}"

### Defaults ###
ROOT=../..
READIES=${ROOT}/deps/readies

ARCH=$(shell ${READIES}/bin/platform --arch)

# get the release version, unless one is defined
# since in CI, branches with {d}.{d} push, we allow anything here
# but, master is remapped to edge
# if this is a tag (v1.2.3), we remove the v
ifndef VERSION
	VERSION=$(shell git branch --show-current|sed -e 's/v//g')
	ifeq ($(VERSION),master)
		VERSION=edge
	endif
endif

# There are mappings between OSNICK to specific OS values, due to docker containers used
# The global mappings are found here.
include ${READIES}/mk/osnick.defs
