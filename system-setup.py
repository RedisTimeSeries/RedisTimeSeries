#!/usr/bin/env python

import sys
import os
import popen2
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "deps/readies"))
import paella

os.environ["PYTHONWARNINGS"] = 'ignore:DEPRECATION::pip._internal.cli.base_command'

#----------------------------------------------------------------------------------------------

class RedisGearsSetup(paella.Setup):
    def __init__(self, nop=False):
        paella.Setup.__init__(self, nop)

    def common_first(self):
        self.setup_pip()
        self.pip_install("wheel")
        self.pip_install("setuptools --upgrade")
        self.pip_install("-r tests/requirements.txt")
        
        self.install("git jq")

    def debian_compat(self):
        self.install("build-essential")

    def redhat_compat(self):
        self.group_install("'Development Tools'")
        self.install("redhat-lsb-core")

    def fedora(self):
        self.group_install("'Development Tools'")

    def macosx(self):
        r, w, e = popen2.popen3('xcode-select -p')
        if r.readlines() == []:
            fatal("Xcode tools are not installed. Please run xcode-select --install.")

    def common_last(self):
        if not self.has_command("ramp"):
            self.pip_install("git+https://github.com/RedisLabs/RAMP --upgrade")

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

RedisGearsSetup(nop = args.nop).setup()
