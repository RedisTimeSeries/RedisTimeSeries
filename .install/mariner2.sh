#!/bin/bash

tdnf install -y build-essential wget tar openssl-devel cmake python3 python3-pip which

git config --global --add safe.directory $PWD

pip install --upgrade setuptools
pip install -r tests/flow/requirements.txt
