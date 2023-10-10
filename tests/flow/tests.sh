#!/bin/bash

# [[ $VERBOSE == 1 ]] && set -x

PROGNAME="${BASH_SOURCE[0]}"
HERE="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"
ROOT=$(cd $HERE/../.. && pwd)
READIES=$ROOT/deps/readies
. $READIES/shibumi/defs

export PYTHONUNBUFFERED=1

VALGRIND_REDIS_VER=6.2
SAN_REDIS_VER=6.2

cd $HERE

#----------------------------------------------------------------------------------------------

help() {
	cat <<-'END'
		Run flow tests.

		[ARGVARS...] tests.sh [--help|help] [<module-so-path>]

		Argument variables:
		MODULE=path         Module .so path

		GEN=0|1             General tests on standalone Redis (default)
		AOF=0|1             AOF persistency tests on standalone Redis
		SLAVES=0|1          Replication tests on standalone Redis
		AOF_SLAVES=0|1      AOF together SLAVES persistency tests on standalone Redis
		OSS_CLUSTER=0|1     General tests on Redis OSS Cluster
		SHARDS=n            Number of shards (default: 3)

		QUICK|SIMPLE=1      Perform only one test variant

		RLEC=0|1            General tests on RLEC

		REDIS_SERVER=path   Location of redis-server
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

traps() {
	local func="$1"
	shift
	local sig
	for sig in "$@"; do
		trap "$func $sig" "$sig"
	done
}

linux_stop() {
	local pgid=$(cat /proc/$PID/status | grep pgid | awk '{print $2}')
	kill -9 -- -$pgid
}

macos_stop() {
	local pgid=$(ps -o pid,pgid -p $PID | awk "/$PID/"'{ print $2 }' | tail -1)
	pkill -9 -g $pgid
}

stop() {
	trap - SIGINT
	if [[ $OS == linux ]]; then
		linux_stop
	elif [[ $OS == macos ]]; then
		macos_stop
	fi
	exit 1
}

traps 'stop' SIGINT

#---------------------------------------------------------------------------------------------- 

setup_rltest() {
	if [[ $RLTEST == view ]]; then
		if [[ ! -d $ROOT/../RLTest ]]; then
			eprint "RLTest not found in view $ROOT"
			exit 1
		fi
		RLTEST=$(cd $ROOT/../RLTest; pwd)
	fi

	if [[ -n $RLTEST ]]; then
		if [[ ! -d $RLTEST ]]; then
			eprint "Invalid RLTest location: $RLTEST"
			exit 1
		fi

		# Specifically search for it in the specified location
		export PYTHONPATH="$PYTHONPATH:$RLTEST"
		if [[ $VERBOSE == 1 ]]; then
			echo "PYTHONPATH=$PYTHONPATH"
		fi
	fi
	
	if [[ $RLTEST_VERBOSE == 1 ]]; then
		RLTEST_ARGS+=" -v"
	fi
	if [[ $RLTEST_DEBUG == 1 ]]; then
		RLTEST_ARGS+=" --debug-print"
	fi
	if [[ -n $RLTEST_LOG && $RLTEST_LOG != 1 ]]; then
		RLTEST_ARGS+=" -s"
	fi
	if [[ $RLTEST_CONSOLE == 1 ]]; then
		RLTEST_ARGS+=" -i"
	fi
	RLTEST_ARGS+=" --enable-debug-command --enable-protected-configs"
}

#----------------------------------------------------------------------------------------------

setup_clang_sanitizer() {
	local ignorelist=$ROOT/tests/memcheck/redis.san-ignorelist
	if ! grep THPIsEnabled $ignorelist &> /dev/null; then
		echo "fun:THPIsEnabled" >> $ignorelist
	fi

	# for RLTest
	export SANITIZER="$SAN"
	
	# --no-output-catch --exit-on-failure --check-exitcode
	RLTEST_SAN_ARGS="--sanitizer $SAN"

	if [[ $SAN == addr || $SAN == address ]]; then
		REDIS_SERVER=${REDIS_SERVER:-redis-server-asan-$SAN_REDIS_VER}
		if ! command -v $REDIS_SERVER > /dev/null; then
			echo Building Redis for clang-asan ...
			V="$VERBOSE" runn $READIES/bin/getredis --force -v $SAN_GETREDIS_VER --suffix asan-$SAN_REDIS_VER --own-openssl --no-run \
				--clang-asan \
				--clang-san-blacklist $ignorelist
		fi

		# RLTest places log file details in ASAN_OPTIONS
		export ASAN_OPTIONS="detect_odr_violation=0:halt_on_error=0:detect_leaks=1"
		export LSAN_OPTIONS="suppressions=$ROOT/tests/memcheck/asan.supp"
		# :use_tls=0

	elif [[ $SAN == mem || $SAN == memory ]]; then
		REDIS_SERVER=${REDIS_SERVER:-redis-server-msan-$SAN_REDIS_VER}
		if ! command -v $REDIS_SERVER > /dev/null; then
			echo Building Redis for clang-msan ...
			V="$VERBOSE" runn $READIES/bin/getredis --force -v $SAN_GETREDIS_VER  --suffix msan-$SAN_REDIS_VER --own-openssl --no-run \
				--clang-msan --llvm-dir /opt/llvm-project/build-msan \
				--clang-san-blacklist $ignorelist
		fi
	fi
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

if [[ $1 == --help || $1 == help || $HELP == 1 ]]; then
	help
	exit 0
fi

OP=""
[[ $NOP == 1 ]] && OP=echo

#----------------------------------------------------------------------------------------------

if [[ $QUICK != 1 && $SIMPLE != 1 ]]; then
	GEN=${GEN:-1}
	SLAVES=${SLAVES:-1}
	AOF=${AOF:-1}
	AOF_SLAVES=${AOF_SLAVES:-1}
	OSS_CLUSTER=${OSS_CLUSTER:-0}
else
	GEN=1
	SLAVES=0
	AOF=0
	AOF_SLAVES=0
	OSS_CLUSTER=0
fi
SHARDS=${SHARDS:-3}
RLEC=${RLEC:-0}

[[ $EXT == 1 ]] && EXISTING_ENV=1

#----------------------------------------------------------------------------------------------

if [[ $RLEC == 1 ]]; then
	GEN=0
	SLAVES=0
	AOF=0
	AOF_SLAVES=0
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
[[ $AOF_SLAVES == 1 ]] && { (RLTEST_ARGS="${RLTEST_ARGS} --use-aof --use-slaves" run_tests "tests with AOF and slaves"); (( E |= $? )); } || true
if [[ $OSS_CLUSTER == 1 ]]; then
	if [[ -z $TEST || $TEST != test_ts_password ]]; then
		{ (RLTEST_ARGS="${RLTEST_ARGS} --env oss-cluster --shards-count $SHARDS" \
			run_tests "tests on OSS cluster"); (( E |= $? )); } || true
	fi
	if [[ -z $TEST || $TEST == test_ts_password* ]]; then
		RLTEST_ARGS_1="$RLTEST_ARGS"
		[[ -z $TEST ]] && RLTEST_ARGS_1+=" --test test_ts_password"
		{ (RLTEST_ARGS="${RLTEST_ARGS_1} --env oss-cluster --shards-count $SHARDS --oss_password password" \
			run_tests "tests on OSS cluster with password"); (( E |= $? )); } || true
	fi
fi

[[ $RLEC == 1 ]]   && { (RLTEST_ARGS="${RLTEST_ARGS} --env existing-env --existing-env-addr $DOCKER_HOST:$RLEC_PORT" run_tests "tests on RLEC"); (( E |= $? )); } || true

exit $E
