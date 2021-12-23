#!/bin/bash

# [[ $VERBOSE == 1 ]] && set -x
[[ $IGNERR == 1 ]] || set -e

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/../.. && pwd)
READIES=$ROOT/deps/readies 
. $READIES/shibumi/defs

VALGRIND_REDIS_VER=6.2

#----------------------------------------------------------------------------------------------

help() {
	cat <<-END
		Run flow tests.

		[ARGVARS...] tests.sh [--help|help] [<module-so-path>]

		Argument variables:
		MODULE=path         Module .so path

		GEN=0|1             General tests on standalone Redis (default)
		AOF=0|1             AOF persistency tests on standalone Redis
		SLAVES=0|1          Replication tests on standalone Redis
		OSS_CLUSTER=0|1     General tests on Redis OSS Cluster
		SHARDS=n            Number of shards (default: 2)
		RLEC=0|1            General tests on RLEC

		REDIS_SERVER=path   Location of redis-server
		REDIS_CONFIG=file   Path to redis.conf config file
		EXT|EXISTING_ENV=1  Run the tests on existing env

		TEST=test           Run specific test (e.g. test.py:test_name)
		VALGRIND|VG=1       Run with Valgrind

		DOCKER_HOST         Address of Docker server (default: localhost)
		RLEC_PORT           Port of existing-env in RLEC container (default: 12000)

		RLTEST_ARGS=...     Extra RLTest arguments
		VERBOSE=1           Print commands
		IGNERR=1            Do not abort on error

	END
}

#----------------------------------------------------------------------------------------------

setup_redis_server() {
	if [[ $VALGRIND == 1 ]]; then
		REDIS_SERVER=${REDIS_SERVER:-redis-server-vg}
		if ! is_command $REDIS_SERVER; then
			echo Building Redis for Valgrind ...
			$READIES/bin/getredis -v $VALGRIND_REDIS_VER --valgrind --suffix vg
		fi
	else
		REDIS_SERVER=${REDIS_SERVER:-redis-server}
	fi

	if ! is_command $REDIS_SERVER; then
		echo "Cannot find $REDIS_SERVER. Aborting."
		exit 1
	fi
}

#----------------------------------------------------------------------------------------------

valgrind_config() {
	export VG_OPTIONS="
		-q \
		--leak-check=full \
		--show-reachable=no \
		--track-origins=yes \
		--show-possibly-lost=no"

	# To generate supressions and/or log to file
	# --gen-suppressions=all --log-file=valgrind.log

	VALGRIND_SUPRESSIONS=$ROOT/tests/redis_valgrind.sup

	VALGRIND_ARGS+="\
		--no-output-catch \
		--use-valgrind \
		--vg-verbose \
		--vg-suppressions $VALGRIND_SUPRESSIONS"

	export VALGRIND=1
}

#----------------------------------------------------------------------------------------------

run_tests() {
	local title="$1"
	if [[ -n $title ]]; then
		$READIES/bin/sep -0
		printf "Running $title:\n\n"
	fi

	if [[ $EXISTING_ENV != 1 ]]; then
		rltest_config=$(mktemp "${TMPDIR:-/tmp}/rltest.XXXXXXX")
		rm -f $rltest_config
		if [[ $RLEC != 1 ]]; then
			cat <<-EOF > $rltest_config
				--clear-logs
				--oss-redis-path=$REDIS_SERVER
				--module $MODULE
				--module-args '$MODARGS'
				$RLTEST_ARGS
				$VALGRIND_ARGS

				EOF
		else
			cat <<-EOF > $rltest_config
				--clear-logs
				$RLTEST_ARGS
				$VALGRIND_ARGS

				EOF
		fi
	else # existing env
		rltest_config=$(mktemp "${TMPDIR:-/tmp}/xredis_rltest.XXXXXXX")
		rm -f $rltest_config
		cat <<-EOF > $rltest_config
			--env existing-env
			$RLTEST_ARGS

			EOF
	fi

	cd $ROOT/tests/flow
	if [[ -n $REDIS_CONFIG && -e $REDIS_CONFIG ]]; then
		cat $REDIS_CONFIG >> $rltest_config
	fi

	if [[ $VERBOSE == 1 ]]; then
		echo "RLTest configuration:"
		cat $rltest_config
	fi

	[[ $RLEC == 1 ]] && export RLEC_CLUSTER=1
	
	local E=0
	if [[ $NOP != 1 ]]; then
		{ $OP python3 -m RLTest @$rltest_config; (( E |= $? )); } || true
	else
		$OP python3 -m RLTest @$rltest_config
	fi

	[[ $KEEP != 1 ]] && rm -f $rltest_config

	return $E
}

#----------------------------------------------------------------------------------------------

[[ $1 == --help || $1 == help ]] && {
	help
	exit 0
}

OP=""
[[ $NOP == 1 ]] && OP=echo

#----------------------------------------------------------------------------------------------

GEN=${GEN:-1}
SLAVES=${SLAVES:-1}
AOF=${AOF:-1}
OSS_CLUSTER=${OSS_CLUSTER:-0}
SHARDS=${SHARDS:-2}
RLEC=${RLEC:-0}

[[ $EXT == 1 ]] && EXISTING_ENV=1

#----------------------------------------------------------------------------------------------

if [[ $RLEC == 1 ]]; then
	GEN=0
	SLAVES=0
	AOF=0
	OSS_CLUSTER=0
fi

#----------------------------------------------------------------------------------------------

DOCKER_HOST=${DOCKER_HOST:-localhost}
RLEC_PORT=${RLEC_PORT:-12000}

#----------------------------------------------------------------------------------------------

if [[ $RLEC != 1 ]]; then
	MODULE=${MODULE:-$1}
	[[ -z $MODULE || ! -f $MODULE ]] && {
		echo "Module not found at ${MODULE}. Aborting."
		exit 1
	}
fi

GDB=${GDB:-0}

[[ $VG == 1 ]] && VALGRIND=1
if [[ $VALGRIND == 1 ]]; then
	valgrind_config
fi

if [[ -n $TEST ]]; then
	RLTEST_ARGS+=" --test $TEST"
	export BB=${BB:-1}
fi

[[ $VERBOSE == 1 ]] && RLTEST_ARGS+=" -v"
[[ $GDB == 1 ]] && RLTEST_ARGS+=" -i --verbose"

cd $ROOT/tests/flow

[[ $RLEC != 1 ]] && setup_redis_server

# RLTEST_ARGS+=" --print-server-cmd"

E=0
[[ $GEN == 1 ]]    && { (run_tests "general tests"); (( E |= $? )); } || true
[[ $SLAVES == 1 ]] && { (RLTEST_ARGS="${RLTEST_ARGS} --use-slaves" run_tests "tests with slaves"); (( E |= $? )); } || true
[[ $AOF == 1 ]]    && { (RLTEST_ARGS="${RLTEST_ARGS} --use-aof" run_tests "tests with AOF"); (( E |= $? )); } || true
[[ $OSS_CLUSTER == 1 ]] && { (RLTEST_ARGS="${RLTEST_ARGS} --env oss-cluster --shards-count $SHARDS" run_tests "tests on OSS cluster"); (( E |= $? )); } || true

[[ $RLEC == 1 ]]   && { (RLTEST_ARGS="${RLTEST_ARGS} --env existing-env --existing-env-addr $DOCKER_HOST:$RLEC_PORT" run_tests "tests on RLEC"); (( E |= $? )); } || true

exit $E
