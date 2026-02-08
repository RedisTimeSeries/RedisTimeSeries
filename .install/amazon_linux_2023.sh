#!/bin/bash
set -e
MODE=$1 # whether to install using sudo or not

# Use --allowerasing to replace curl-minimal with full curl
$MODE dnf -y update && $MODE dnf -y install --allowerasing \
    git \
    gcc \
    gcc-c++ \
    make \
    cmake \
    automake \
    autoconf \
    libtool \
    openssl-devel \
    zlib-devel \
    bzip2-devel \
    libffi-devel \
    wget \
    curl \
    which \
    tar \
    unzip \
    jq \
    clang 

$MODE dnf clean all
