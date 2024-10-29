#!/bin/bash

# This script prepares the environment for the project by installing the
# necessary dependencies and fetching the necessary files.

set -e

rm -rf .venv
python3 -m venv .venv
python3 -m pip install --upgrade pip virtualenv
source .venv/bin/activate
python3 -m pip install --upgrade pip setuptools wheel
python3 -m pip install -r tests/flow/requirements.txt
python3 -m pip install jinja2 ramp-packer
# ./sbin/setup
