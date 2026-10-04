#!/usr/bin/env bash
# Bound the test process while leaving time for the workflow to upload logs.
set -euo pipefail

duration=$1
shift
diagnostics="${GITHUB_WORKSPACE:?}/macos-test-diagnostics.log"

snapshot() {
  {
    date -u
    uptime
    vm_stat
    df -h "$GITHUB_WORKSPACE"
    # Avoid command arguments, which can contain credentials.
    ps -axo pid,ppid,%cpu,%mem,rss,etime,comm
  } >> "$diagnostics" 2>&1 || true
}

snapshot
(
  pause_pid=''
  trap 'if [[ -n "$pause_pid" ]]; then kill "$pause_pid" 2>/dev/null || true; fi; exit 0' TERM INT
  while true; do
    sleep 300 &
    pause_pid=$!
    wait "$pause_pid"
    pause_pid=''
    snapshot
    echo "macOS test heartbeat: $(date -u)"
    tail -5 "$diagnostics"
  done
) &
monitor_pid=$!
cleanup() {
  kill "$monitor_pid" 2>/dev/null || true
  wait "$monitor_pid" 2>/dev/null || true
}
trap cleanup EXIT

status=0
gtimeout --signal=TERM --kill-after=30s "$duration" "$@" || status=$?
snapshot
if [[ "$status" == 124 ]]; then
  echo "::error::macOS tests exceeded $duration; see test_output.log and macos-test-diagnostics.log"
fi
exit "$status"
