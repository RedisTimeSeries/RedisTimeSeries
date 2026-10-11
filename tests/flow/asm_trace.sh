#!/usr/bin/env bash
# MOD-17216 diagnostic branch only. Do not merge this test wrapper.
set -eu
if ! command -v strace >/dev/null; then
    # Event CI uses disposable Debian-family containers. Bound provisioning and
    # report failure explicitly instead of silently running without tracing.
    if [ "$(id -u)" != 0 ] || ! command -v apt-get >/dev/null; then
        echo 'MOD-17216 syscall trace unavailable: install strace in this image' >&2
        exit 2
    fi
    timeout 120 apt-get update -qq
    timeout 120 apt-get install -y -qq strace
fi
if [ -n "${MOD17216_KERNEL_GATE:-}" ]; then
    for i in $(seq 1 190); do
        [ -f "$MOD17216_KERNEL_GATE/ready" ] && break
        [ -f "$MOD17216_KERNEL_GATE/failed" ] && break
        sleep 1
    done
    if [ ! -f "$MOD17216_KERNEL_GATE/ready" ]; then
        echo 'MOD-17216 kernel observer unavailable; workload not started' >&2
        exit 2
    fi
fi
destination="$(pwd)/tests/flow/logs/mod17216-setsockopt.log"
# tests.sh removes its logs at startup; keep the open trace outside that tree.
trace=$(mktemp /tmp/mod17216-setsockopt.XXXXXX)
echo "MOD-17216 syscall trace: $trace"
strace --version | head -n 1
# Follow children/threads from launch. Seccomp filtering avoids stopping on
# unselected syscalls. Raw arguments avoid decoding TLS/socket-option payloads;
# option numbers, return values, PID and timestamps remain available.
# Tracing may still perturb scheduling; a green run does not prove a fix.
set +e
timeout --kill-after=10 300 strace -f --seccomp-bpf -ttt \
    -e trace=setsockopt -e raw=setsockopt -o "$trace" "$@"
status=$?
set -e
mkdir -p "$(dirname "$destination")"
mv "$trace" "$destination"
trace=$destination
# Linux SOL_SOCKET=1, SO_RCVBUF=8, SO_RCVBUFFORCE=33 on both CI architectures.
echo "MOD-17216 receive-buffer setters (raw arguments, at most 100 lines):"
grep -E 'setsockopt\([^,]+, 0x1, (0x8|0x21),' "$trace" | head -n 100 || true
echo "MOD-17216 syscall trace finished: status=$status bytes=$(wc -c < "$trace")"
exit "$status"
