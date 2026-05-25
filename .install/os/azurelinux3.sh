#!/usr/bin/env bash
# Azure Linux 3 (tdnf). Same shape as mariner2 — Microsoft's distro lineage,
# tdnf-based, ships a smaller package set than dnf.

# shellcheck source=../lib/packages.sh
. "$LIB/packages.sh"

tdnf_default_install

# Install aws-cli for uploading artifacts to s3
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
./aws/install
