#!/usr/bin/env bash

# A script to run Redis with RedisTimeSeries module.
# Supports running under release and debug modes, using locally-built
# Redis server instance path and may run under gdb.

MODULE_RELEASE_PATH=$PWD/bin/linux-x64-release/redistimeseries.so
MODULE_DEBUG_PATH=$PWD/bin/linux-x64-debug/redistimeseries.so
REDIS_ARGUMENTS="--enable-debug-command yes"
REDIS_GLOBAL_PATH=redis-server
DEBUGGER_SCRIPT="gdb --args"
VALGRIND="valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes -s"

function launch() {
    local redis_path=$1
    local timeseries_module_path=$2
    local redis_arguments="$3 $4"

    $redis_path --loadmodule $timeseries_module_path $redis_arguments
}

function parse_args_and_launch() {
    local redis_path=$REDIS_GLOBAL_PATH
    local timeseries_module_path=$MODULE_RELEASE_PATH
    local redis_arguments=$REDIS_ARGUMENTS
    local prefix=""

    while getopts 'dDs:l:vh' opt; do
    case "$opt" in
        d)
        echo "Setting up to run against the debug binaries."
        timeseries_module_path=$MODULE_DEBUG_PATH
        ;;

        D)
        echo "Setting up to run through a debugger."
        timeseries_module_path=$MODULE_DEBUG_PATH
        prefix="${DEBUGGER_SCRIPT}"
        ;;

        s)
        arg="$OPTARG"
        echo "Setting up to run a custom redis server: '${OPTARG}'"
        redis_path=${OPTARG}
        ;;

        l)
        arg="$OPTARG"
        echo "Setting up a custom timeseries library path: '${OPTARG}'"
        timeseries_module_path="${OPTARG}/libredistimeseries.so"
        ;;

        v)
        echo "Setting up to run through valgrind."
        prefix="${VALGRIND}"
        ;;

        ?|h)
        printf "Usage: $(basename $0) [-d] [-D] [-v] [-s custom-redis-path] [-l custom-library-path]\nArguments:\n\t-d\tUse debug binaries\n\t-D\tRun in debugger\n\t-v\tRun via valgrind\n\t-s\tSpecify custom redis server\n\t-l\tSpecify custom library path\n\nExample: $(basename $0) -d -s ../redis/src/redis-server\n"
        exit 1
        ;;
    esac
    done

    launch "${prefix} ${redis_path}" $timeseries_module_path $redis_arguments
}

parse_args_and_launch $@
