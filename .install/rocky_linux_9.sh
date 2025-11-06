#!/bin/bash
# Update system packages first to avoid glibc version mismatches
yum -y update
yum -y install epel-release
yum -y install gcc make cmake3 wget openssl-devel bzip2-devel libffi-devel zlib-devel wget scl-utils which gcc-toolset-13-gcc gcc-toolset-13-gcc-c++ gcc-toolset-13-libatomic-devel jq
yum groupinstall "Development Tools" -y
cp /opt/rh/gcc-toolset-13/enable /etc/profile.d/gcc-toolset-13.sh

make --version
gcc --version
git --version

wget https://www.python.org/ftp/python/3.9.6/Python-3.9.6.tgz
tar -xvf Python-3.9.6.tgz
cd Python-3.9.6
./configure
make -j `nproc`
make altinstall
cd ..
# Create python3 symlink - use full path since which might not find it yet
rm -f /usr/bin/python3
ln -s /usr/local/bin/python3.9 /usr/bin/python3
# Also create pip3 symlink if it doesn't exist
if [ ! -f /usr/bin/pip3 ]; then
    ln -s /usr/local/bin/pip3.9 /usr/bin/pip3
fi
# Ensure /usr/local/bin is in PATH for subsequent steps
export PATH="/usr/local/bin:$PATH"
# Verify installations
cmake --version
python3 --version
pip3 --version
# Also verify python3 is accessible via the symlink
/usr/bin/python3 --version
# Detect architecture and download appropriate AWS CLI
ARCH=$(uname -m)
if [[ $ARCH == "aarch64" ]]; then
    curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"
else
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
fi
unzip awscliv2.zip
./aws/install
