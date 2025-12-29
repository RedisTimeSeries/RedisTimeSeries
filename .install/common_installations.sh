#!/bin/bash
set -e
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not
if command -v pip3 &> /dev/null; then
    PIP_CMD="pip3"
elif command -v pip &> /dev/null; then
    PIP_CMD="pip"
else
    echo "Neither pip3 nor pip is installed."
    exit 1
fi

$PIP_CMD install --upgrade pip
$PIP_CMD install -q --upgrade setuptools
echo "pip version: $($PIP_CMD --version)"
echo "pip path: $(which $PIP_CMD)"

$PIP_CMD install -q -r tests/flow/requirements.txt
# These packages are needed to build the package
$PIP_CMD install -q -r .install/build_package_requirements.txt

# List installed packages
$PIP_CMD list
