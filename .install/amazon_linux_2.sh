#!/bin/bash
amazon-linux-extras install epel -y
yum -y install epel-release yum-utils
yum-config-manager --add-repo http://vault.centos.org/centos/7/sclo/x86_64/rh/
yum -y install gcc make autogen automake libtool cmake3 git python-pip openssl-devel bzip2-devel libffi-devel zlib-devel wget centos-release-scl scl-utils which tar unzip jq
yum -y install devtoolset-11-gcc devtoolset-11-gcc-c++ devtoolset-11-make --nogpgcheck
. scl_source enable devtoolset-11 || true
make --version
git --version

echo "::group::install Python 3.9.6"
wget https://www.python.org/ftp/python/3.9.6/Python-3.9.6.tgz
tar -xvf Python-3.9.6.tgz
cd Python-3.9.6
./configure
make -j `nproc`
make altinstall
cd ..
rm /usr/bin/python3 && ln -s `which python3.9` /usr/bin/python3
python3 --version
echo "::endgroup::"

