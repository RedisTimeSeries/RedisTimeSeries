#!/bin/bash

# [[ $VERBOSE == 1 ]] && set -x

PROGNAME="${BASH_SOURCE[0]}"
HERE="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"
ROOT=$(cd $HERE/../.. && pwd)
READIES=$ROOT/deps/readies
. $READIES/shibumi/defs

export PYTHONUNBUFFERED=1

VALGRIND_GETREDIS_VER=7
SAN_REDIS_VER=7.2
SAN_GETREDIS_VER=7

cd $HERE

#----------------------------------------------------------------------------------------------

help() {
	cat <<-'END'
		Run flow tests.

		[ARGVARS...] tests.sh [--help|help] [<module-so-path>]

		Argument variables:
		MODULE=path           Module .so path

		TEST=test             Run specific test (e.g. test.py:test_name)

		RLTEST=path|'view'    Take RLTest from repo path or from local view
		RLTEST_ARGS=...       Extra RLTest arguments

		GEN=0|1               General tests on standalone Redis (default)
		AOF=0|1               AOF persistency tests on standalone Redis
		SLAVES=0|1            Replication tests on standalone Redis
		AOF_SLAVES=0|1        AOF together SLAVES persistency tests on standalone Redis
		OSS_CLUSTER=0|1       General tests on Redis OSS Cluster
		SHARDS=n              Number of shards (default: 3)

		QUICK=1               Perform only one test variant

		TEST=name             Run specific test (e.g. test.py:test_name)
		TESTFILE=file         Run tests listed in `file`
		FAILEDFILE=file       Write failed tests into `file`

		UNSTABLE=1            Do not skip unstable tests (default: 0)
		ONLY_STABLE=1         Skip unstable tests

		REDIS_SERVER=path     Location of redis-server
		REDIS_PORT=n          Redis server port

		EXT=1|run             Test on existing env (1=running; run=start redis-server)
		EXT_HOST=addr         Address of existing env (default: 127.0.0.1)
		EXT_PORT=n            Port of existing env

		RLEC=0|1              General tests on RLEC
		DOCKER_HOST=addr      Address of Docker server (default: localhost)
		RLEC_PORT=port        Port of RLEC database (default: 12000)

		COV=1                 Run with coverage analysis
		VG=1                  Run with Valgrind
		VG_LEAKS=0            Do not detect leaks
		SAN=type              Use LLVM sanitizer (type=address|memory|leak|thread) 
		BB=1                  Enable Python debugger (break using BB() in tests)
		GDB=1                 Enable interactive gdb debugging (in single-test mode)

		RLTEST=path|'view'    Take RLTest from repo path or from local view
		RLTEST_DEBUG=1        Show debugging printouts from tests
		RLTEST_ARGS=args      Extra RLTest args

		PARALLEL=1            Runs tests in parallel
		SLOW=1                Do not test in parallel
		UNIX=1                Use unix sockets
		RANDPORTS=1           Use randomized ports

		PLATFORM_MODE=1       Implies NOFAIL & COLLECT_LOGS into STATFILE
		COLLECT_LOGS=1        Collect logs into .tar file
		CLEAR_LOGS=0          Do not remove logs prior to running tests
		NOFAIL=1              Do not fail on errors (always exit with 0)
		STATFILE=file         Write test status (0|1) into `file`

		LIST=1                List all tests and exit
		VERBOSE=1             Print commands and Redis output
		LOG=1                 Send results to log (even on single-test mode)
		KEEP=1                Do not remove intermediate files
		NOP=1                 Dry run
		HELP=1                Show help

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
		$READIES/bin/getredis -v $VALGRIND_GETREDIS_VER --valgrind --suffix vg
	fi

	if [[ $VG_LEAKS == 0 ]]; then
		VG_LEAK_CHECK=no
		RLTEST_VG_NOLEAKS="--vg-no-leakcheck"
	else
		VG_LEAK_CHECK=full
		RLTEST_VG_NOLEAKS=""
	fi
	# RLTest reads this
	VG_OPTIONS="\
		-q \
		--leak-check=$VG_LEAK_CHECK \
		--show-reachable=no \
		--track-origins=yes \
		--show-possibly-lost=no"

	VALGRIND_SUPRESSIONS=$ROOT/tests/memcheck/valgrind.supp

	RLTEST_VG_ARGS+="\
		--use-valgrind \
		--vg-verbose \
		$RLTEST_VG_NOLEAKS \
		--vg-no-fail-on-errors \
		--vg-suppressions $VALGRIND_SUPRESSIONS"

	# for RLTest
	export VALGRIND=1
	export VG_OPTIONS
	export RLTEST_VG_ARGS
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
		if [[ -n $GITHUB_ACTIONS ]]; then
			echo "::group::$title"
		else
			$READIES/bin/sep1 -0
			printf "Running $title:\n\n"
		fi
	fi

	if [[ $EXT != 1 ]]; then
		rltest_config=$(mktemp "${TMPDIR:-/tmp}/rltest.XXXXXXX")
		rm -f $rltest_config
		if [[ $RLEC != 1 ]]; then
			cat <<-EOF > $rltest_config
				--oss-redis-path=$REDIS_SERVER
				--module $MODULE
				--module-args '$MODARGS'
				$RLTEST_ARGS
				$RLTEST_TEST_ARGS
				$RLTEST_PARALLEL_ARG
				$RLTEST_VG_ARGS
				$RLTEST_SAN_ARGS
				$RLTEST_COV_ARGS

				EOF
		else
			cat <<-EOF > $rltest_config
				$RLTEST_ARGS
				$RLTEST_VG_ARGS

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
				$RLTEST_TEST_ARGS

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
				$RLTEST_TEST_ARGS

				EOF
		fi
	fi


	if [[ $VERBOSE == 1 || $NOP == 1 ]]; then
		echo "RLTest configuration:"
		cat $rltest_config
		[[ -n $VG_OPTIONS ]] && { echo "VG_OPTIONS: $VG_OPTIONS"; echo; }
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

	if [[ -n $GITHUB_ACTIONS ]]; then
		echo "::endgroup::"
	fi
	return $E
}

#------------------------------------------------------------------------------------ Arguments

if [[ $1 == --help || $1 == help || $HELP == 1 ]]; then
	help
	exit 0
fi

OP=""
[[ $NOP == 1 ]] && OP=echo

#--------------------------------------------------------------------------------- Environments

DOCKER_HOST=${DOCKER_HOST:-127.0.0.1}
RLEC_PORT=${RLEC_PORT:-12000}

EXT_HOST=${EXT_HOST:-127.0.0.1}
EXT_PORT=${EXT_PORT:-6379}

PID=$$
OS=$($READIES/bin/platform --os)

#---------------------------------------------------------------------------------- Tests scope
if [[ $RLEC != 1 ]]; then
	MODULE="${MODULE:-$1}"
	if [[ -z $MODULE || ! -f $MODULE ]]; then
		echo "Module not found at ${MODULE}. Aborting."
		exit 1
	fi
fi

SHARDS=${SHARDS:-3}

#------------------------------------------------------------------------------------ Debugging

VG_LEAKS=${VG_LEAKS:-1}
VG_ACCESS=${VG_ACCESS:-1}

GDB=${GDB:-0}

if [[ $GDB == 1 ]]; then
	[[ $LOG != 1 ]] && RLTEST_LOG=0
	RLTEST_CONSOLE=1
fi

[[ $SAN == addr ]] && SAN=address
[[ $SAN == mem ]] && SAN=memory

if [[ -n $TEST ]]; then
	[[ $LOG != 1 ]] && RLTEST_LOG=0
	# export BB=${BB:-1}
	export RUST_BACKTRACE=1
fi

#-------------------------------------------------------------------------------- Platform Mode

if [[ $PLATFORM_MODE == 1 ]]; then
	CLEAR_LOGS=0
	COLLECT_LOGS=1
	NOFAIL=1
fi
STATFILE=${STATFILE:-$ROOT/bin/artifacts/tests/status}

#---------------------------------------------------------------------------------- Parallelism

[[ $SLOW == 1 ]] && PARALLEL=0

PARALLEL=${PARALLEL:-1}

# due to Python "Can't pickle local object" problem in RLTest
[[ $OS == macos ]] && PARALLEL=0

[[ $EXT == 1 || $EXT == run || $BB == 1 || $GDB == 1 ]] && PARALLEL=0

if [[ -n $PARALLEL && $PARALLEL != 0 ]]; then
	if [[ $PARALLEL == 1 ]]; then
		parallel="$($READIES/bin/nproc)"
	else
		parallel="$PARALLEL"
	fi
	RLTEST_PARALLEL_ARG="--parallelism $parallel"
fi

#------------------------------------------------------------------------------- Test selection

if [[ -n $TEST ]]; then
	RLTEST_TEST_ARGS+=$(echo -n " "; echo "$TEST" | awk 'BEGIN { RS=" "; ORS=" " } { print "--test " $1 }')
fi

if [[ -n $TESTFILE && -z $TEST ]]; then
	if ! is_abspath "$TESTFILE"; then
		TESTFILE="$ROOT/$TESTFILE"
	fi
	RLTEST_TEST_ARGS+=" -f $TESTFILE"
fi

if [[ -n $FAILEDFILE ]]; then
	if ! is_abspath "$FAILEDFILE"; then
		TESTFILE="$ROOT/$FAILEDFILE"
	fi
	RLTEST_TEST_ARGS+=" -F $FAILEDFILE"
	RLTEST_TEST_ARGS_1+=" -F $FAILEDFILE"
fi

if [[ $LIST == 1 ]]; then
	NO_SUMMARY=1
	RLTEST_ARGS+=" --collect-only"
fi

#---------------------------------------------------------------------------------------- Setup

if [[ $VERBOSE == 1 ]]; then
	RLTEST_VERBOSE=1
fi

RLTEST_LOG=${RLTEST_LOG:-$LOG}

if [[ $COV == 1 ]]; then
	setup_coverage
fi

RLTEST_ARGS+=" $@"

if [[ -n $REDIS_PORT ]]; then
	RLTEST_ARGS+="--redis-port $REDIS_PORT"
fi

[[ $UNIX == 1 ]] && RLTEST_ARGS+=" --unix"
[[ $RANDPORTS == 1 ]] && RLTEST_ARGS+=" --randomize-ports"

#----------------------------------------------------------------------------------------------

setup_rltest

if [[ -n $SAN ]]; then
	setup_clang_sanitizer
fi

if [[ $VG == 1 ]]; then
	export VALGRIND=1
	setup_valgrind
fi

if [[ $RLEC != 1 ]]; then
	setup_redis_server
fi

#----------------------------------------------------------------------------------------------

if [[ $QUICK != 1 ]]; then
	GEN=${GEN:-1}
	SLAVES=${SLAVES:-1}
	AOF=${AOF:-1}
	AOF_SLAVES=${AOF_SLAVES:-1}
	OSS_CLUSTER=${OSS_CLUSTER:-1}
else
	GEN=1
	SLAVES=0
	AOF=0
	AOF_SLAVES=0
	OSS_CLUSTER=0
fi

if [[ $RLEC == 1 ]]; then
	GEN=0
	SLAVES=0
	AOF=0
	AOF_SLAVES=0
	OSS_CLUSTER=0
fi

#-------------------------------------------------------------------------------- Running tests

if [[ $CLEAR_LOGS != 0 ]]; then
	rm -rf $HERE/logs
fi

E=0
[[ $GEN == 1 ]]         && { (run_tests "general tests"); (( E |= $? )); } || true
[[ $SLAVES == 1 ]]      && { (RLTEST_ARGS="${RLTEST_ARGS} --use-slaves" run_tests "tests with slaves"); (( E |= $? )); } || true
[[ $AOF == 1 ]]         && { (RLTEST_ARGS="${RLTEST_ARGS} --use-aof" run_tests "tests with AOF"); (( E |= $? )); } || true
[[ $AOF_SLAVES == 1 ]]  && { (RLTEST_ARGS="${RLTEST_ARGS} --use-aof --use-slaves" run_tests "tests with AOF and slaves"); (( E |= $? )); } || true
if [[ $OSS_CLUSTER == 1 ]]; then
	RLTEST_ARGS="${RLTEST_ARGS} --cluster_node_timeout 60000"
	if [[ -z $TEST || $TEST != test_ts_password ]]; then
		{ (RLTEST_ARGS="${RLTEST_ARGS} --env oss-cluster --shards-count $SHARDS" \
			run_tests "tests on OSS cluster"); (( E |= $? )); } || true
	fi
	if [[ -z $TEST || $TEST == test_ts_password* ]]; then
		RLTEST_ARGS_1="$RLTEST_ARGS"
		RLTEST_TEST_ARGS_1=" --test test_ts_password"
		{ (RLTEST_ARGS="${RLTEST_ARGS_1} --env oss-cluster --shards-count $SHARDS --oss_password password" \
		   RLTEST_TEST_ARGS="$RLTEST_TEST_ARGS_1" \
		   run_tests "tests on OSS cluster with password"); (( E |= $? )); } || true
	fi
fi

if [[ $RLEC == 1 ]]; then
	RLTEST_ARGS="${RLTEST_ARGS} --cluster_node_timeout 60000"
	dhost=$(echo "$DOCKER_HOST" | awk -F[/:] '{print $4}') # DOCKER_HOST=tcp://host:port
	dhost=${dhost:-127.0.0.1}
	{ (RLTEST_ARGS+="${RLTEST_ARGS} --env existing-env --existing-env-addr $dhost:$RLEC_PORT" \
	   run_tests "tests on RLEC"); (( E |= $? )); } || true
fi

#-------------------------------------------------------------------------------------- Summary

if [[ $NO_SUMMARY == 1 ]]; then
	exit 0
fi

if [[ $NOP != 1 ]]; then
	if [[ -n $SAN || $VG == 1 ]]; then
		{ FLOW=1 $ROOT/sbin/memcheck-summary; (( E |= $? )); } || true
	fi
fi

if [[ $COLLECT_LOGS == 1 ]]; then
	ARCH=$($READIES/bin/platform --arch)
	OSNICK=$($READIES/bin/platform --osnick)
	cd $ROOT
	mkdir -p bin/artifacts/tests
	test_tar="bin/artifacts/tests/tests-flow-logs-${ARCH}-${OSNICK}.tgz"
	rm -f "$test_tar"
	find tests/flow/logs -name "*.log*" | tar -czf "$test_tar" -T -
	echo "Test logs:"
	du -ah --apparent-size bin/artifacts/tests
fi

if [[ -n $STATFILE ]]; then
	mkdir -p "$(dirname "$STATFILE")"
	if [[ -f $STATFILE ]]; then
		(( E |= $(cat $STATFILE || echo 1) )) || true
	fi
	echo $E > $STATFILE
fi

if [[ $NOFAIL == 1 ]]; then
	exit 0
fi

exit $E
