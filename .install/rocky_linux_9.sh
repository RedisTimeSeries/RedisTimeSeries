#!/bin/bash
yum -y install epel-release
yum -y update --nobest

yum -y install python3.11 python3.11-pip python3.11-devel

yum -y install gcc gcc-c++ || echo "Warning: gcc installation failed"
if ! command -v gcc &> /dev/null; then
    yum -y install --nobest gcc gcc-c++ || true
fi

yum -y install --nobest --skip-broken make cmake3 wget openssl-devel bzip2-devel libffi-devel zlib-devel scl-utils which jq unzip
yum -y install --nobest --skip-broken gcc-toolset-13-gcc gcc-toolset-13-gcc-c++ gcc-toolset-13-libatomic-devel || true
yum groupinstall "Development Tools" -y --nobest --skip-broken || true

if [ -f /opt/rh/gcc-toolset-13/enable ]; then
    cp /opt/rh/gcc-toolset-13/enable /etc/profile.d/gcc-toolset-13.sh
    source /opt/rh/gcc-toolset-13/enable || true
fi

alternatives --set python3 /usr/bin/python3.11 || ln -sf /usr/bin/python3.11 /usr/bin/python3
ln -sf /usr/bin/pip3.11 /usr/bin/pip3 || true

make --version || echo "make not available"
cmake --version || echo "cmake not available"
gcc --version || echo "gcc not available"
python3 --version
pip3 --version
# Detect architecture and download appropriate AWS CLI
ARCH=$(uname -m)
if [[ $ARCH == "aarch64" ]]; then
    curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"
else
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
fi
unzip awscliv2.zip
./aws/install
