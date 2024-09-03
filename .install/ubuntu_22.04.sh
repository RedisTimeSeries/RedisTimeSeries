#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

pip install -r .install/build_package_requirements.txt  # required for packing the module (todo: move to pack.sh after refactor)

$MODE apt update -qq
$MODE apt install -yqq git wget build-essential lcov openssl libssl-dev python3 python3-venv python3-dev unzip rsync autoconf automake libtool valgrind
source install_cmake.sh $MODE
