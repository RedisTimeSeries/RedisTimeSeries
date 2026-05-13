#!/usr/bin/env bash
# Shared package sets, by package-manager family. Single source of truth for
# everything that is the same across (most of) a family. Per-OS files in
# ../os/ compose these and add their own deltas (extra packages, repo enables,
# update-alternatives, profile snippets, ...).
#
# Sourced by os/<osnick>.sh after lib/pm.sh. All variables here are plain
# space-separated strings so callers can splat them with `apt_install $SET`.

# ----------------------------------------------------------------------------
# Debian family (apt)
# ----------------------------------------------------------------------------
DEBIAN_BASE="
    ca-certificates wget curl git make autoconf automake libtool pkg-config
    build-essential clang libclang-dev
    openssl libssl-dev libbz2-dev libffi-dev zlib1g-dev libblocksruntime-dev
    tcl
    python3 python3-pip python3-venv python3-dev
    cmake
    unzip rsync valgrind lcov jq tar gdb
"

# ----------------------------------------------------------------------------
# RHEL family (dnf / yum). EL10 base repos do not ship lcov; install from
# EPEL inline if a particular CI lane needs it.
# ----------------------------------------------------------------------------
RHEL_BASE="
    ca-certificates wget curl git make autoconf automake libtool
    gcc gcc-c++
    openssl openssl-devel bzip2-devel libffi-devel zlib-devel
    clang clang-devel
    tcl
    python3 python3-pip python3-devel
    cmake
    unzip rsync valgrind jq tar which gdb
"

# ----------------------------------------------------------------------------
# CBL-Mariner / Azure Linux (tdnf). Smaller repo set than dnf; readline-devel
# is here because Azure Linux's Python build setup requests it.
# ----------------------------------------------------------------------------
TDNF_BASE="
    ca-certificates wget curl git
    build-essential gcc g++ make cmake autoconf automake libtool clang
    openssl-devel bzip2-devel libffi-devel zlib-devel readline-devel
    python3 python3-pip python3-devel
    unzip jq tar which
"

# ----------------------------------------------------------------------------
# Alpine (apk). Includes the musl-specific extras that used to live in
# quirks/alpine.sh — they are always required on Alpine, so they belong here.
# ----------------------------------------------------------------------------
ALPINE_BASE="
    ca-certificates wget curl git make autoconf automake libtool
    build-base g++ clang18 clang18-libclang
    openssl openssl-dev bzip2-dev libffi-dev zlib-dev
    tcl
    python3 python3-dev py3-pip py-virtualenv
    cmake
    unzip rsync valgrind jq tar gdb
    bash bsd-compat-headers gcompat libstdc++ libgcc linux-headers musl-dev
    xz openssh
"

# ----------------------------------------------------------------------------
# macOS (homebrew). Apple clang already ships at /usr/bin/cc; we layer
# llvm@18 on top via PATH (see os/macos.sh). Python is intentionally absent
# here — most Macs already have one and `brew install python@3.11` collides
# with Apple's framework symlinks (see os/macos.sh's conditional install).
# ----------------------------------------------------------------------------
BREW_BASE="
    autoconf automake libtool
    make cmake
    llvm@18 openssl@3 libffi bzip2 zlib
    coreutils wget curl rsync jq lcov
"
