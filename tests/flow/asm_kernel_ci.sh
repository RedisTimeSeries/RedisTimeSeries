#!/usr/bin/env bash
# Diagnostic-only replacement for docker run in focused Event CI jobs.
set -u
gate=$(mktemp -d "$PWD/.mod17216-kernel.XXXXXX")
container_gate=/workspace/$(basename "$gate")
name=mod17216-workload-$$
observer=mod17216-observer-$$
cleanup() {
    docker rm -f "$observer" "$name" >/dev/null 2>&1 || true
    # Host-owned path survives root-owned workload logs and launcher cleanup.
    if ! cp "$gate/observer.log" tests/flow/mod17216-kernel.log; then
        echo "MOD-17216 kernel artifact copy failed; preserving $gate" >&2
        return
    fi
    rm -rf "$gate"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
docker run --name "$name" -e MOD17216_KERNEL_GATE="$container_gate" "$@" &
workload=$!
for i in $(seq 1 30); do
    [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" = true ] && break
    kill -0 "$workload" 2>/dev/null || { wait "$workload"; exit $?; }
    sleep 1
done
# The MTU experiment must never operate in the host or another container's netns.
network_mode=$(docker inspect -f '{{.HostConfig.NetworkMode}}' "$name")
case "$network_mode" in
    default|bridge|none) ;;
    *) echo "MOD-17216 requires a private workload network namespace: $network_mode" >&2; exit 2 ;;
esac
docker run --name "$observer" --privileged --network "container:$name" \
    -v "$PWD:/workspace" -w /workspace ubuntu:24.04 \
    bash tests/flow/asm_kernel_observer.sh "$container_gate" \
    > "$gate/observer.log" 2>&1 &
observer_job=$!
# Relay readiness to the test container, including a concrete startup failure.
for i in $(seq 1 180); do
    if grep -q 'MOD-17216 kernel probe ready' "$gate/observer.log"; then
        touch "$gate/ready"
        break
    fi
    if ! kill -0 "$observer_job" 2>/dev/null; then
        touch "$gate/failed"
        break
    fi
    sleep 1
done
[ -f "$gate/ready" ] || touch "$gate/failed"
wait "$workload"
status=$?
# A probe that ended before the workload is incomplete coverage, not success.
if ! kill -0 "$observer_job" 2>/dev/null && [ "$status" = 0 ]; then status=2; fi
docker kill --signal=INT "$observer" >/dev/null 2>&1 || true
wait "$observer_job" || true
cat "$gate/observer.log"
echo "MOD-17216 kernel observer finished: workload_status=$status"
exit "$status"
