#!/usr/bin/env python3

import sys
import os
import argparse

HERE = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
READIES = os.path.join(ROOT, "deps/readies")
sys.path.insert(0, READIES)
import paella

#----------------------------------------------------------------------------------------------

class RedisTimeSeriesSetup(paella.Setup):
    def __init__(self, args):
        paella.Setup.__init__(self, args.nop)
        self.sudoIf(self.os != 'macos')
        self.pytools = not args.no_pytools

    def common_first(self):
        self.install_downloaders()
        self.install("git jq")
        self.run(f"{READIES}/bin/enable-utf8", sudo=self.os != 'macos')

        self.install("autoconf libtool m4 automake")
        self.install("openssl")

    def debian_compat(self):
        self.run(f"{READIES}/bin/getgcc --modern")
        self.install("libssl-dev")

    def redhat_compat(self):
        self.install("which")
        self.run(f"{READIES}/bin/getepel", sudo=True)
        self.install("openssl-devel")
        self.run(f"{READIES}/bin/getgcc --modern")

    def archlinux(self):
        self.run(f"{READIES}/bin/getgcc --modern")
        self.install("lcov-git", aur=True)

    def fedora(self):
        self.run(f"{READIES}/bin/getgcc --modern")
        self.install("openssl-devel")
        self.install("python3-networkx")

    def linux_last(self):
        self.install("valgrind")

    def macos(self):
        self.install_gnu_utils()

    def common_last(self):
        self.run(f"{self.python} {READIES}/bin/getcmake --usr")
        if self.dist != "arch":
            self.install("lcov")
        else:
            self.install("lcov-git", aur=True)
        self.run(f"{READIES}/bin/getaws")
        if self.pytools:
            self.run(f"{self.python} {READIES}/bin/getrmpytools --reinstall --modern")
            self.pip_install(f"-r {ROOT}/tests/flow/requirements.txt")
            self.run(f"NO_PY2=1 {READIES}/bin/getpudb")

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
parser.add_argument('--no-pytools', action="store_true", help='Do not install Python tools')
args = parser.parse_args()

RedisTimeSeriesSetup(args).setup()
