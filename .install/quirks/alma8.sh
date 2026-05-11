#!/usr/bin/env bash
#
# EL 8 family quirk (not "Alma-only" by behavior): gcc-toolset-11 + CRB/powertools.
# Invoked as:
#   - quirks/alma8.sh     when OSNICK=alma8
#   - quirks/rocky8.sh    sources this file (Rocky 8)
#   - quirks/centos8.sh / quirks/ol8.sh  thin wrappers → this file (optional OSNICKs)
# install_script.sh normalizes readies centos8|ol8 → rocky8 before quirks, so most
# CentOS/OL EL8 hosts still hit rocky8.sh → here. Real RHEL 8 needs codeready-builder,
# not a repo named powertools (see enable_el8_code_ready_or_powertools).
#
# Sourced by install_script.sh; receives MODE as $1.

set -eu

MODE="${1:-}"

# Rocky/Alma/CentOS use "powertools" (older) or "crb" (newer). Real RHEL 8
# (RHUI) exposes CodeReady Builder as codeready-builder-for-rhel-8-* — there
# is no repo literally named "powertools", so dnf would fail on RHEL.
enable_el8_code_ready_or_powertools() {
	if [ -r /etc/os-release ]; then
		# shellcheck disable=SC1091
		. /etc/os-release
		if [ "${ID:-}" = "rhel" ] && [ "${VERSION_ID%%.*}" = "8" ]; then
			local rid
			rid=$(dnf repolist --all 2>/dev/null | grep -i 'codeready-builder-for-rhel-8' | grep -vi source | head -1 | awk '{print $1}')
			if [ -n "$rid" ]; then
				$MODE dnf config-manager --set-enabled "$rid"
				return 0
			fi
			echo "quirks/alma8.sh: RHEL 8 needs CodeReady Builder; no codeready-builder-for-rhel-8 repo in 'dnf repolist --all'." >&2
			echo "  Fix RHUI/subscription repos, then re-run bootstrap." >&2
			exit 1
		fi
	fi
	if $MODE dnf config-manager --set-enabled powertools 2>/dev/null; then
		return 0
	fi
	if $MODE dnf config-manager --set-enabled crb 2>/dev/null; then
		return 0
	fi
	echo "quirks/alma8.sh: could not enable powertools or crb (non-RHEL EL8)" >&2
	exit 1
}

$MODE dnf -y groupinstall "Development Tools"
enable_el8_code_ready_or_powertools "$MODE"
$MODE dnf -y install epel-release
$MODE dnf -y install \
	bzip2-devel \
	gcc-toolset-11-gcc \
	gcc-toolset-11-gcc-c++ \
	gcc-toolset-11-libatomic-devel \
	libffi-devel \
	python3.11-devel \
	xz \
	zlib-devel

# Drop a profile snippet so subsequent shells (including Dockerfile RUNs that
# don't `source enable` themselves) get the toolset on PATH.
$MODE cp /opt/rh/gcc-toolset-11/enable /etc/profile.d/gcc-toolset-11.sh
