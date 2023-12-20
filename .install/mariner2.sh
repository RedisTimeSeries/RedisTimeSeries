#!/bin/bash

tdnf install -y build-essential wget tar openssl-devel cmake python3 python3-pip which unzip
git config --global --add safe.directory $PWD

pip install --upgrade setuptools
pip install RLTest~=0.4.2
pip install -r tests/flow/requirements.txt

pip install git+https://github.com/RedisLabs/RAMP@redis-py-3.5
pip install jinja2  # required for packing the module (todo: move to pack.sh after refactor)

# Install aws-cli for uploading artifacts to s3
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
./aws/install
