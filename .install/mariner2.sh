#!/bin/bash

tdnf install -y build-essential wget tar openssl-devel cmake python3 python3-pip which unzip jq
git config --global --add safe.directory $PWD

pip install --upgrade setuptools
pip3 install -q -r tests/flow/requirements.txt
pip3 install -q -r .install/build_package_requirements.txt

# Install aws-cli for uploading artifacts to s3
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
./aws/install
