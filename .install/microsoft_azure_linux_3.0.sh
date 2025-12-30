#!/bin/bash
# This script automates the process of setting up a development environment for RedisTimeSeries on a Microsoft Azure Linux virtual machine.
# It performs the following actions:
# 1. Updates the system and installs essential development tools and libraries.
# 2. Downloads, compiles, and installs Python 3.9.9 with optimizations.
# 3. Configures symbolic links for python3 and pip3 to point to the newly installed Python version.
# 4. Upgrades pip, setuptools, and wheel.
# 5. Installs Python dependencies from requirements files.
# 6. Installs the AWS CLI.
#
# The script assumes it is run from the root directory of the RedisTimeSeries project.

tdnf -y update && \
    tdnf install -y \
        build-essential \
        git \
        wget \
        gcc \
        g++ \
        make \
        cmake \
        libffi-devel \
        openssl-devel \
        zlib-devel \
        bzip2-devel \
        readline-devel \
        which \
        unzip \
        jq ca-certificates
git config --global --add safe.directory $PWD

wget https://www.python.org/ftp/python/3.9.9/Python-3.9.9.tgz && \
    tar -xf Python-3.9.9.tgz && \
    cd Python-3.9.9 && \
    ./configure --enable-optimizations && \
    make -j $(nproc) && \
    make altinstall && \
    cd .. && \
    rm -rf Python-3.9.9 Python-3.9.9.tgz && \
    ln -sf /usr/local/bin/python3.9 /usr/local/bin/python3 && \
    ln -sf /usr/local/bin/pip3.9 /usr/local/bin/pip3

python3 -m pip install --upgrade pip setuptools wheel
pip install -r tests/flow/requirements.txt

pip install -r .install/build_package_requirements.txt  # required for packing the module (todo: move to pack.sh after refactor)
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install
