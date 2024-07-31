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
        self.run("%s/bin/enable-utf8" % READIES, sudo=self.os != 'macos')

        self.install("autoconf libtool m4 automake")
        self.install("openssl")

    def debian_compat(self):
        self.run("%s/bin/getgcc --modern" % READIES)
        self.install("libssl-dev")

    def redhat_compat(self):
        self.install("redhat-lsb-core")
        self.install("which")
        self.run("%s/bin/getepel" % READIES, sudo=True)
        self.install("openssl-devel")
        self.run("%s/bin/getgcc --modern" % READIES)

    def archlinux(self):
        self.run("%s/bin/getgcc --modern" % READIES)
        self.install("lcov-git", aur=True)

    def fedora(self):
        self.run("%s/bin/getgcc --modern" % READIES)
        self.install("openssl-devel")
        self.install("python3-networkx")

    def linux_last(self):
        self.install("valgrind")

    def macos(self):
        self.install_gnu_utils()

    def common_last(self):
        self.run("{PYTHON} {READIES}/bin/getcmake --usr".format(PYTHON=self.python, READIES=READIES))
        if self.dist != "arch":
            self.install("lcov")
        else:
            self.install("lcov-git", aur=True)
        self.run("{READIES}/bin/getaws".format(READIES=READIES))
        if self.pytools:
            self.run("{PYTHON} {READIES}/bin/getrmpytools --reinstall --pypi --ramp-version github:redis-py-3.5".format(PYTHON=self.python, READIES=READIES))
            # self.run("{PYTHON} {READIES}/bin/getrmpytools --reinstall --pypi --modern".format(PYTHON=self.python, READIES=READIES))
            # self.run(f"{self.python} {READIES}/bin/getrmpytools --reinstall --modern")
            self.pip_install("-r {ROOT}/tests/flow/requirements.txt".format(ROOT=ROOT))
            self.run("NO_PY2=1 %s/bin/getpudb" % READIES)

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
parser.add_argument('--no-pytools', action="store_true", help='Do not install Python tools')
args = parser.parse_args()

RedisTimeSeriesSetup(args).setup()
