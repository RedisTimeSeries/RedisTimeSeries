#!/bin/bash

tdnf install -y build-essential git wget ca-certificates tar openssl-devel cmake python3 python3-pip

pip install --upgrade setuptools
pip install -r tests/flow/requirements.txt
