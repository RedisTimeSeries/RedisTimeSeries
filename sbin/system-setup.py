#!/usr/bin/env python3

import sys
import os
import argparse

ROOT = HERE = os.path.abspath(os.path.dirname(__file__))
READIES = os.path.join(ROOT, "deps/readies")
sys.path.insert(0, READIES)
import paella

#----------------------------------------------------------------------------------------------

class RedisTimeSeriesSetup(paella.Setup):
    def __init__(self, nop=False):
        paella.Setup.__init__(self, nop)

    def common_first(self):
        self.install("git jq curl unzip")
        self.run("%s/bin/enable-utf8" % READIES, sudo=self.os != 'macos')

        self.install("autoconf libtool m4 automake")
        self.install("openssl")

    def debian_compat(self):
        self.run("%s/bin/getgcc --modern" % READIES)
        self.install("libssl-dev")

    def redhat_compat(self):
        self.install("redhat-lsb-core")
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

    def macos(self):
        self.install_gnu_utils()

    def common_last(self):
        self.run("{PYTHON} {READIES}/bin/getcmake --usr".format(PYTHON=self.python, READIES=READIES))
        if self.dist != "arch":
            self.install("lcov")
        else:
            self.install("lcov-git", aur=True)
        self.pip_install("pudb awscli")

        # self.run("{PYTHON} {READIES}/bin/getrmpytools".format(PYTHON=self.python, READIES=READIES))
        self.pip_install("-r {ROOT}/tests/flow/requirements.txt".format(ROOT=ROOT))
        self.pip_install("gevent")

    def linux_last(self):
        self.install("valgrind")

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

RedisTimeSeriesSetup(nop = args.nop).setup()
