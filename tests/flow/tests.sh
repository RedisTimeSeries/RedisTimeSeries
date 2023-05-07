#!/bin/bash

# [[ $VERBOSE == 1 ]] && set -x
[[ $IGNERR == 1 ]] || set -e

PROGNAME="${BASH_SOURCE[0]}"
HERE="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"
ROOT=$(cd $HERE/../.. && pwd)
READIES=$ROOT/deps/readies
. $READIES/shibumi/defs

VALGRIND_REDIS_VER=6.2
SAN_REDIS_VER=6.2

cd $HERE

#----------------------------------------------------------------------------------------------

help() {
	cat <<-END
		Run flow tests.

		[ARGVARS...] tests.sh [--help|help] [<module-so-path>]

		Argument variables:
		MODULE=path         Module .so path

		TEST=test           Run specific test (e.g. test.py:test_name)

		RLTEST=path|'view'  Take RLTest from repo path or from local view
		RLTEST_ARGS=...     Extra RLTest arguments

		GEN=0|1             General tests on standalone Redis (default)
		AOF=0|1             AOF persistency tests on standalone Redis
		SLAVES=0|1          Replication tests on standalone Redis
		AOF_SLAVES=0|1      AOF together SLAVES persistency tests on standalone Redis
		OSS_CLUSTER=0|1     General tests on Redis OSS Cluster
		SHARDS=n            Number of shards (default: 3)

		QUICK=1             Perform only one test variant
		PARALLEL=1          Runs RLTest tests in parallel


		RLEC=0|1            General tests on RLEC
		DOCKER_HOST=addr    Address of Docker server (default: localhost)
		RLEC_PORT=n         Port of RLEC database (default: 12000)

		REDIS_SERVER=path   Location of redis-server

		EXISTING_ENV=1      Test on existing env (like EXT=1)
		EXT=1|run           Test on existing env (1=running; run=start redis-server)
		EXT_HOST=addr       Address if existing env (default: 127.0.0.1)
		EXT_PORT=n          Port of existing env

		COV=1               Run with coverage analysis
		VG=1                Run with Valgrind
		VG_LEAKS=0          Do not detect leaks
		SAN=type            Use LLVM sanitizer (type=address|memory|leak|thread) 
		GDB=1               Enable interactive gdb debugging (in single-test mode)

		LIST=1                List all tests and exit
		VERBOSE=1           Print commands and Redis output
		LOG=1               Send results to log (even on single-test mode)
		KEEP=1              Do not remove intermediate files
		IGNERR=1            Do not abort on error
		NOP=1               Dry run
		HELP=1              Show help


	END
	exit 0
}

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

	if [[ -n $TEST ]]; then
		RLTEST_ARGS+=" --test $TEST"
		export BB=${BB:-1}
	fi

	GDB=${GDB:-0}
	if [[ $GDB == 1 ]]; then
		RLTEST_ARGS+=" -i --verbose"
	fi

	if [[ $PARALLEL == 1 ]]; then
		RLTEST_PARALLEL_ARG="--parallelism $($READIES/bin/nproc)"
	fi
}

#----------------------------------------------------------------------------------------------

setup_clang_sanitizer() {
	if ! grep THPIsEnabled /build/redis.blacklist &> /dev/null; then
		echo "fun:THPIsEnabled" >> /build/redis.blacklist
	fi


	# for RLTest
	export SANITIZER="$SAN"
	
	# --no-output-catch --exit-on-failure --check-exitcode
	# RLTEST_SAN_ARGS="--unix --sanitizer $SAN"
	RLTEST_SAN_ARGS="--sanitizer $SAN"

	if [[ $SAN == addr || $SAN == address ]]; then
		REDIS_SERVER=${REDIS_SERVER:-redis-server-asan-$SAN_REDIS_VER}
		if ! command -v $REDIS_SERVER > /dev/null; then
			echo Building Redis for clang-asan ...
			$READIES/bin/getredis --force -v $SAN_REDIS_VER --own-openssl --no-run --suffix asan --clang-asan --clang-san-blacklist /build/redis.blacklist
		fi

		export ASAN_OPTIONS=detect_odr_violation=0
		# :detect_leaks=0

	elif [[ $SAN == mem || $SAN == memory ]]; then
		REDIS_SERVER=${REDIS_SERVER:-redis-server-msan-$SAN_REDIS_VER}
		if ! command -v $REDIS_SERVER > /dev/null; then
			echo Building Redis for clang-msan ...
			$READIES/bin/getredis --force -v $SAN_REDIS_VER  --no-run --own-openssl --suffix msan --clang-msan --llvm-dir /opt/llvm-project/build-msan --clang-san-blacklist /build/redis.blacklist
		fi
	fi
}

clang_sanitizer_summary() {
	if grep -l "leaked in" logs/*.asan.log* &> /dev/null; then
		echo
		echo "${LIGHTRED}Sanitizer: leaks detected:${RED}"
		grep -l "leaked in" logs/*.asan.log*
		echo "${NOCOLOR}"
		E=1
	fi
}

#----------------------------------------------------------------------------------------------

setup_redis_server() {
	REDIS_SERVER=${REDIS_SERVER:-redis-server}

	if ! is_command $REDIS_SERVER; then
		echo "Cannot find $REDIS_SERVER. Aborting."
		exit 1
	fi
}

#----------------------------------------------------------------------------------------------

setup_valgrind() {
	REDIS_SERVER=${REDIS_SERVER:-redis-server-vg}
	if ! is_command $REDIS_SERVER; then
		echo Building Redis for Valgrind ...
		$READIES/bin/getredis -v $VALGRIND_REDIS_VER --valgrind --suffix vg
	fi
}

valgrind_config() {
	export VG_OPTIONS="\
		-q \
		--leak-check=full \
		--show-reachable=no \
		--track-origins=yes \
		--show-possibly-lost=no"

	# To generate supressions and/or log to file
	# --gen-suppressions=all --log-file=valgrind.log

	VALGRIND_SUPRESSIONS=$ROOT/tests/redis_valgrind.sup

	RLTEST_VALGRIND_ARGS+="\
		--no-output-catch \
		--use-valgrind \
		--vg-verbose \
		--vg-suppressions $VALGRIND_SUPRESSIONS"

	export VALGRIND=1
}

valgrind_summary() {
	# Collect name of each flow log that contains leaks
	FILES_WITH_LEAKS=$(grep -l "definitely lost" logs/*.valgrind.log)
	if [[ ! -z $FILES_WITH_LEAKS ]]; then
		echo "Memory leaks introduced in flow tests."
		echo $FILES_WITH_LEAKS
		# Print the full Valgrind output for each leaking file
		echo $FILES_WITH_LEAKS | xargs cat
		exit 1
	else
		echo Valgrind test ok
	fi
}

#----------------------------------------------------------------------------------------------

setup_coverage() {
	# RLTEST_COV_ARGS="--unix"

	export CODE_COVERAGE=1
}

#----------------------------------------------------------------------------------------------

run_tests() {
	local title="$1"
	shift
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
				$RLTEST_PARALLEL_ARG
				$RLTEST_VALGRIND_ARGS
				$RLTEST_SAN_ARGS
				$RLTEST_COV_ARGS

				EOF
		else
			cat <<-EOF > $rltest_config
				--clear-logs
				$RLTEST_ARGS
				$RLTEST_VALGRIND_ARGS

				EOF
		fi
	else # existing env
		if [[ $EXT == run ]]; then
			xredis_conf=$(mktemp "${TMPDIR:-/tmp}/xredis_conf.XXXXXXX")
			rm -f $xredis_conf
			cat <<-EOF > $xredis_conf
				loadmodule $MODULE $MODARGS
				EOF

			rltest_config=$(mktemp "${TMPDIR:-/tmp}/xredis_rltest.XXXXXXX")
			rm -f $rltest_config
			cat <<-EOF > $rltest_config
				--env existing-env
				$RLTEST_ARGS

				EOF

			if [[ $VERBOSE == 1 ]]; then
				echo "External redis-server configuration:"
				cat $xredis_conf
			fi

			$REDIS_SERVER $xredis_conf &
			XREDIS_PID=$!
			echo "External redis-server pid: " $XREDIS_PID

		else # EXT=1
			rltest_config=$(mktemp "${TMPDIR:-/tmp}/xredis_rltest.XXXXXXX")
			[[ $KEEP != 1 ]] && rm -f $rltest_config
			cat <<-EOF > $rltest_config
				--env existing-env
				--existing-env-addr $EXT_HOST:$EXT_PORT
				$RLTEST_ARGS

				EOF
		fi
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

	if [[ -n $XREDIS_PID ]]; then
		echo "killing external redis-server: $XREDIS_PID"
		kill -TERM $XREDIS_PID
	fi

	return $E
}

#----------------------------------------------------------------------------------------------

[[ $1 == --help || $1 == help || $HELP == 1 ]] && { help; exit 0; }

if [[ $RLEC != 1 ]]; then
	MODULE="${MODULE:-$1}"
	if [[ -z $MODULE || ! -f $MODULE ]]; then
		echo "Module not found at ${MODULE}. Aborting."
		exit 1
	fi
fi

if [[ $REDIS_VERBOSE == 1 || $VERBOSE == 1 ]]; then
	if [[ $LOG != 1 ]]; then
		RLTEST_ARGS+=" -s -v"
	fi
fi

if [[ $COV == 1 ]]; then
	setup_coverage
fi

[[ $EXT == 1 || $EXT == run ]] && EXISTING_ENV=1

OP=""
[[ $NOP == 1 ]] && OP=echo

#----------------------------------------------------------------------------------------------

if [[ $QUICK != 1 ]]; then
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

[[ $SAN == addr ]] && SAN=address
[[ $SAN == mem ]] && SAN=memory

EXT_HOST=${EXT_HOST:-127.0.0.1}
EXT_PORT=${EXT_PORT:-6379}

DOCKER_HOST=${DOCKER_HOST:-localhost}

RLEC=${RLEC:-0}
RLEC_PORT=${RLEC_PORT:-12000}

[[ $EXT == 1 || $EXT == run || $EXISTING_ENV == 1 ]] && PARALLEL=0

#----------------------------------------------------------------------------------------------

setup_rltest

if [[ -n $SAN ]]; then
	setup_clang_sanitizer
fi

if [[ $VG == 1 ]]; then
	setup_valgrind
fi

if [[ $RLEC != 1 ]]; then
	setup_redis_server
fi

#----------------------------------------------------------------------------------------------

if [[ $RLEC == 1 ]]; then
	GEN=0
	SLAVES=0
	AOF=0
	AOF_SLAVES=0
	OSS_CLUSTER=0
fi

#----------------------------------------------------------------------------------------------

E=0
[[ $GEN == 1 ]]         && { (run_tests "general tests"); (( E |= $? )); } || true
[[ $SLAVES == 1 ]]      && { (RLTEST_ARGS="${RLTEST_ARGS} --use-slaves" run_tests "tests with slaves"); (( E |= $? )); } || true
[[ $AOF == 1 ]]         && { (RLTEST_ARGS="${RLTEST_ARGS} --use-aof" run_tests "tests with AOF"); (( E |= $? )); } || true
[[ $AOF_SLAVES == 1 ]]  && { (RLTEST_ARGS="${RLTEST_ARGS} --use-aof --use-slaves" run_tests "tests with AOF and slaves"); (( E |= $? )); } || true
[[ $OSS_CLUSTER == 1 ]] && { (RLTEST_ARGS="${RLTEST_ARGS} --env oss-cluster --shards-count $SHARDS" run_tests "tests on OSS cluster"); (( E |= $? )); } || true

[[ $RLEC == 1 ]]        && { (RLTEST_ARGS="${RLTEST_ARGS} --env existing-env --existing-env-addr $DOCKER_HOST:$RLEC_PORT" run_tests "tests on RLEC"); (( E |= $? )); } || true

if [[ $NOP != 1 && -n $SAN ]]; then
	clang_sanitizer_summary
fi

exit $E
