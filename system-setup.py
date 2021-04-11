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
        self.pip_install("wheel")
        self.pip_install("setuptools --upgrade")

        self.install("git jq curl")
        self.run("%s/bin/enable-utf8" % READIES)

    def debian_compat(self):
        self.run("%s/bin/getgcc" % READIES)

    def redhat_compat(self):
        self.install("redhat-lsb-core")
        self.run("%s/bin/getepel" % READIES)
        self.run("%s/bin/getgcc --modern" % READIES)

        if self.dist == "amzn":
            self.run("amazon-linux-extras install epel")
            self.install("python3-devel")
        elif self.dist == "centos" and self.os_version[0] == 8:
            self.install("https://pkgs.dyn.su/el8/base/x86_64/lcov-1.14-3.el8.noarch.rpm")
        else:
            self.install("python3-devel libaec-devel")

    def arch_compat(self):
        self.install("lcov-git", aur=True)

    def fedora(self):
        self.run("%s/bin/getgcc" % READIES)
        self.install("python3-networkx")

    def common_last(self):
        if not self.has_command("lcov"):
            self.install("lcov")
        self.run("{PYTHON} {READIES}/bin/getrmpytools --reinstall".format(PYTHON=self.python, READIES=READIES))
        self.pip_install("-r tests/flow/requirements.txt")

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

RedisTimeSeriesSetup(nop = args.nop).setup()
