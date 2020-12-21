#!/usr/bin/env python3

import sys
import os
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "deps/readies"))
import paella

#----------------------------------------------------------------------------------------------

class RedisTimeSeriesSetup(paella.Setup):
    def __init__(self, nop=False):
        paella.Setup.__init__(self, nop)

    def common_first(self):
        self.setup_pip()
        self.pip_install("wheel")
        self.pip_install("setuptools --upgrade")

        self.install("git jq curl")

    def debian_compat(self):
        self.install("build-essential")
        self.install("python3-psutil")

    def redhat_compat(self):
        self.group_install("'Development Tools'")
        self.install("redhat-lsb-core")
        if not self.dist == "amzn":
            self.install("epel-release")
            self.install("python3-devel libaec-devel")
            self.install("python36-psutil")
        else:
            self.run("amazon-linux-extras install epel", output_on_error=True)
            self.install("python3-devel")
            self.pip_install("psutil")

    def fedora(self):
        self.group_install("'Development Tools'")
        self.install("python3 python3-psutil python3-networkx")

    def common_last(self):
        self.run("python3 -m pip uninstall -y RLTest || true")
        self.install("lcov")
        if not self.has_command("ramp"):
            self.pip_install("git+https://github.com/RedisLabs/RAMP@master")
        self.pip_install("-r tests/flow/requirements.txt")
        self.pip_install("jinja2")

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

RedisTimeSeriesSetup(nop = args.nop).setup()
