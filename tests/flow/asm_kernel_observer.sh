#!/usr/bin/env bash
# Diagnostic-only observer in a disposable container; no socket payloads read.
set -eu
gate=$1
trap 'rc=$?; if [ "$rc" != 0 ]; then echo "$rc" > "$gate/failed"; fi' EXIT
timeout 120 apt-get update -qq
timeout 120 apt-get install -y -qq bpftrace
mount -t tracefs tracefs /sys/kernel/tracing
uname -r
bpftrace --version
ns=$(readlink /proc/self/ns/net | tr -cd 0-9)
target=tcp_try_rmem_schedule
# The probe reads arg1 as sk_buff; do not substitute a one-argument function.
grep -q '^tcp_try_rmem_schedule\([[:space:]]\|$\)' /sys/kernel/tracing/available_filter_functions
grep -q '^tcp_ack\([[:space:]]\|$\)' /sys/kernel/tracing/available_filter_functions
grep -q '^tcp_write_wakeup\([[:space:]]\|$\)' /sys/kernel/tracing/available_filter_functions
echo "MOD-17216 kernel target=$target netns=$ns"
sed "s/tcp_try_rmem_schedule/$target/g" /workspace/tests/flow/asm_receive_memory.bt > "$gate/probe.bt"
export BPFTRACE_BTF=/sys/kernel/btf/vmlinux
exec bpftrace -B line "$gate/probe.bt" "$ns"
