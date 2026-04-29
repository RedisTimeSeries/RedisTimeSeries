#!/usr/bin/env bash
#
# macOS-only post-install steps. Run after the abstract `brew install` of the
# package list in ../../dependencies.yaml. Mirrors the PATH-munging that the
# legacy .install/macos.sh did, but no longer duplicates the package
# installation (the abstract installer already ran `brew install make jq
# openssl@3 llvm@18 libblocksruntime autoconf automake libtool coreutils`).
#
# What the abstract installer cannot express:
#   * Persisting a PATH that prefers GNU coreutils, llvm@18, and GNU make
#     over their BSD/Apple counterparts (so `make`, `clang`, `tar`, `realpath`
#     etc. behave the same as on Linux when developers run `make build`).
#   * Writing the same PATH to $GITHUB_PATH inside GitHub Actions.
#
# Sourced by install_script.sh; receives MODE as $1 (unused on macOS, brew
# refuses to run as root).

set -eu

if ! command -v brew >/dev/null 2>&1; then
    echo "quirks/macos.sh: brew is not installed; install it from https://brew.sh" >&2
    exit 1
fi

LLVM_VERSION="18"
BREW_PREFIX="$(brew --prefix)"
GNUBIN="$BREW_PREFIX/opt/make/libexec/gnubin"
LLVM="$BREW_PREFIX/opt/llvm@$LLVM_VERSION/bin"
COREUTILS="$BREW_PREFIX/opt/coreutils/libexec/gnubin"

update_profile() {
    local profile_path=$1
    local newpath="export PATH=$COREUTILS:$LLVM:$GNUBIN:\$PATH"
    grep -qxF "$newpath" "$profile_path" || echo "$newpath" >> "$profile_path"
    echo "$newpath"
    # NB: we deliberately do NOT `. "$profile_path"` here. The legacy
    # macos.sh used to, but install_script.sh runs under `set -eu` and
    # sourcing a .zshrc from bash often trips `set -u` on zsh-specific
    # variables (e.g. $ZSH_VERSION). The PATH update is meant for the user's
    # next shell anyway; the Makefile's next step (python3 -m venv) finds
    # python3 via brew's existing /opt/homebrew/bin entry.
    if [ -n "${GITHUB_PATH:-}" ]; then
        echo "$newpath" >> "$GITHUB_PATH"
    fi
}

# Use if/fi instead of `&&` chains: under `set -e`, a `[ ... ] && cmd` line
# whose test fails returns 1 from the line itself and aborts the script.
if [ -f "$HOME/.bash_profile" ]; then update_profile "$HOME/.bash_profile"; fi
if [ -f "$HOME/.zshrc" ];        then update_profile "$HOME/.zshrc";        fi
