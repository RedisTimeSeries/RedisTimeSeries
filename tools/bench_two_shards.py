#!/usr/bin/env python3
import argparse
import os
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass


def crc16(data: bytes) -> int:
    """
    Redis cluster CRC16 (XMODEM) used for keyslot calculation.
    """
    crc = 0x0000
    for b in data:
        crc ^= (b << 8) & 0xFFFF
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc & 0xFFFF


def keyslot_from_hashtag(tag: str) -> int:
    return crc16(tag.encode("utf-8")) & 0x3FFF


def run(cmd: list[str], *, cwd: str | None = None, env: dict[str, str] | None = None) -> None:
    subprocess.run(cmd, cwd=cwd, env=env, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def ensure_redis_bins() -> None:
    for bin_name in ("redis-server", "redis-cli"):
        if shutil.which(bin_name) is None:
            raise RuntimeError(f"Missing required binary in PATH: {bin_name}")


def ensure_python_redis() -> None:
    try:
        import redis  # noqa: F401
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "Missing python package 'redis'. Install with: pip3 install redis"
        ) from e


@dataclass(frozen=True)
class BenchResult:
    fanout_ok: bool
    response_len: int
    tags: tuple[str, str]
    rps_by_concurrency: dict[int, float]

    @property
    def max_rps(self) -> float:
        return max(self.rps_by_concurrency.values()) if self.rps_by_concurrency else 0.0

    @property
    def best_concurrency(self) -> int:
        return max(self.rps_by_concurrency, key=self.rps_by_concurrency.get)


def parse_concurrency_list(s: str) -> list[int]:
    out: list[int] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    if not out:
        raise ValueError("empty concurrency list")
    return out


def start_cluster(workdir: str, base_port: int, module_path: str, nodes: int) -> list[int]:
    create_cluster = os.path.join(os.path.dirname(__file__), "..", "tests", "utils", "create-cluster")
    create_cluster = os.path.realpath(create_cluster)
    if not os.path.exists(create_cluster):
        raise RuntimeError(f"create-cluster script not found at {create_cluster}")

    os.makedirs(workdir, exist_ok=True)
    env = os.environ.copy()
    env["NODES"] = str(nodes)
    env["PORT"] = str(base_port)
    env["PROTECTED_MODE"] = "no"
    env["CLUSTER_HOST"] = "127.0.0.1"
    env["ADDITIONAL_OPTIONS"] = f"--loadmodule {module_path}"

    # start instances (daemonized), then create cluster
    run([create_cluster, "start"], cwd=workdir, env=env)
    # small wait for startup
    time.sleep(0.5)
    run([create_cluster, "create"], cwd=workdir, env=env)

    ports = [base_port + i for i in range(1, nodes + 1)]

    def wait_cluster_ok(port: int, timeout_s: float = 20.0) -> None:
        import redis  # type: ignore

        r = redis.Redis(host="127.0.0.1", port=port, decode_responses=True)
        deadline = time.time() + timeout_s
        last = ""
        while time.time() < deadline:
            try:
                info = r.execute_command("CLUSTER", "INFO")
                last = str(info)
                if "cluster_state:ok" in last:
                    return
            except Exception:
                pass
            time.sleep(0.2)
        raise RuntimeError(f"Cluster did not become ok in time on port {port}. Last info: {last}")

    # ensure cluster is ready before touching module cluster integration
    wait_cluster_ok(ports[0], timeout_s=30.0)

    # In OSS cluster mode, LibMR requires an explicit topology refresh on each node.
    try:
        import redis  # type: ignore

        for p in ports:
            redis.Redis(host="127.0.0.1", port=p, decode_responses=True).execute_command(
                "timeseries.REFRESHCLUSTER"
            )
        # Best-effort: establish connections early to avoid first-command penalty/timeouts.
        redis.Redis(host="127.0.0.1", port=ports[0], decode_responses=True).execute_command(
            "timeseries.FORCESHARDSCONNECTION"
        )
    except Exception as e:
        raise RuntimeError(
            "Failed running timeseries.REFRESHCLUSTER/FORCESHARDSCONNECTION (required for OSS cluster mode)"
        ) from e

    return ports


def stop_cluster(workdir: str, base_port: int, nodes: int) -> None:
    create_cluster = os.path.join(os.path.dirname(__file__), "..", "tests", "utils", "create-cluster")
    create_cluster = os.path.realpath(create_cluster)
    env = os.environ.copy()
    env["NODES"] = str(nodes)
    env["PORT"] = str(base_port)
    env["CLUSTER_HOST"] = "127.0.0.1"

    try:
        run([create_cluster, "stop"], cwd=workdir, env=env)
    finally:
        # cleanup files created by create-cluster
        run([create_cluster, "clean"], cwd=workdir, env=env)


def slot_ranges_by_port(ports: list[int]) -> dict[int, list[tuple[int, int]]]:
    import redis  # type: ignore

    r = redis.Redis(host="127.0.0.1", port=ports[0], decode_responses=False)
    slots = r.execute_command("CLUSTER", "SLOTS")
    # slots: [[start, end, [ip, port, nodeid], [replica...]], ...]
    out: dict[int, list[tuple[int, int]]] = {p: [] for p in ports}
    for entry in slots:
        start, end = int(entry[0]), int(entry[1])
        master = entry[2]
        master_port = int(master[1])
        if master_port in out:
            out[master_port].append((start, end))
    if not any(out[p] for p in ports):
        raise RuntimeError(f"Unexpected slot mapping from CLUSTER SLOTS: {out}")
    return out


def find_tag_for_port(target_port: int, ranges: list[tuple[int, int]]) -> str:
    for i in range(1, 500000):
        tag = f"p{target_port}_{i}"
        slot = keyslot_from_hashtag(tag)
        for start, end in ranges:
            if start <= slot <= end:
                return tag
    raise RuntimeError(f"Could not find hashtag mapping to port {target_port}")


def load_series(port: int, tag: str, n: int) -> None:
    import redis  # type: ignore

    r = redis.Redis(host="127.0.0.1", port=port, decode_responses=True)
    # Ensure module responds
    mod = r.execute_command("MODULE", "LIST")
    if not any(m[1] == "timeseries" for m in mod):
        raise RuntimeError("timeseries module not loaded")

    # create series + add a sample
    for j in range(n):
        key = f"{{{tag}}}ts:{j}"
        r.execute_command(
            "TS.CREATE",
            key,
            "LABELS",
            "measurement",
            "cpu",
            "fieldname",
            "usage_nice",
            "bench_shard_port",
            str(port),
        )
        r.execute_command("TS.ADD", key, "*", "1")


def verify_fanout(
    coordinator_port: int, tag1: str, tag2: str, tag3: str | None
) -> tuple[bool, int, dict[str, int]]:
    import redis  # type: ignore

    r = redis.Redis(host="127.0.0.1", port=coordinator_port, decode_responses=True)
    res = r.execute_command("TS.MGET", "FILTER", "measurement=cpu", "fieldname=usage_nice")
    # RESP2: list of [key, labels, [ts, val]]
    keys = []
    for item in res:
        if isinstance(item, (list, tuple)) and item:
            keys.append(item[0])
    cnt1 = sum(1 for k in keys if f"{{{tag1}}}" in k)
    cnt2 = sum(1 for k in keys if f"{{{tag2}}}" in k)
    cnt3 = 0
    if tag3 is not None:
        cnt3 = sum(1 for k in keys if f"{{{tag3}}}" in k)
    ok = (cnt1 > 0) and (cnt2 > 0) and (cnt3 == 0)
    return ok, len(keys), {"tag1": cnt1, "tag2": cnt2, "tag3": cnt3}


def bench_mget(coordinator_port: int, concurrency: int, duration_s: float, warmup_s: float) -> float:
    import redis  # type: ignore

    stop = threading.Event()
    counters = [0] * concurrency

    def worker(idx: int) -> None:
        r = redis.Redis(host="127.0.0.1", port=coordinator_port, decode_responses=False)
        args = ("TS.MGET", "FILTER", "measurement=cpu", "fieldname=usage_nice")
        local = 0
        while not stop.is_set():
            r.execute_command(*args)
            local += 1
        counters[idx] = local

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(concurrency)]
    for t in threads:
        t.start()

    time.sleep(warmup_s)
    # reset counters after warmup
    counters[:] = [0] * concurrency
    t0 = time.time()
    time.sleep(duration_s)
    t1 = time.time()
    stop.set()
    for t in threads:
        t.join(timeout=2.0)

    elapsed = max(t1 - t0, 1e-9)
    total = sum(counters)
    return total / elapsed


def run_benchmark(
    module_path: str,
    base_port: int,
    series_per_shard: int,
    duration_s: float,
    warmup_s: float,
    conc: list[int],
    *,
    nodes: int,
) -> BenchResult:
    workdir = os.path.realpath(os.path.join("/tmp", f"rts_bench_cluster_{base_port}"))
    if os.path.exists(workdir):
        shutil.rmtree(workdir)
    os.makedirs(workdir, exist_ok=True)

    ports: list[int] | None = None
    try:
        ports = start_cluster(workdir, base_port, module_path, nodes)
        if len(ports) < 3:
            raise RuntimeError("This benchmark expects an OSS Redis Cluster with >=3 masters.")

        p1, p2, p3 = ports[0], ports[1], ports[2]

        # determine slot ranges for each port and pick hashtags that map to each shard
        ranges = slot_ranges_by_port(ports)
        tag1 = find_tag_for_port(p1, ranges[p1])
        tag2 = find_tag_for_port(p2, ranges[p2])
        # a tag for shard3 (used only for "not present" fanout verification)
        tag3 = find_tag_for_port(p3, ranges[p3]) if ranges.get(p3) else None

        # load data directly on each shard's port (keys guaranteed to belong to that shard via hashtag)
        load_series(p1, tag1, series_per_shard)
        load_series(p2, tag2, series_per_shard)

        # quick check both shards have keys (DBSIZE is local)
        import redis  # type: ignore

        db1 = int(redis.Redis(host="127.0.0.1", port=p1).execute_command("DBSIZE"))
        db2 = int(redis.Redis(host="127.0.0.1", port=p2).execute_command("DBSIZE"))
        if db1 <= 0 or db2 <= 0:
            raise RuntimeError(f"Expected keys on both shards but DBSIZE is db1={db1}, db2={db2}")

        fanout_ok, resp_len, fanout_counts = verify_fanout(p1, tag1, tag2, tag3)
        if not fanout_ok:
            raise RuntimeError(
                "Fan-out verification failed: "
                f"response_len={resp_len}, counts={fanout_counts}, tags=(tag1={tag1}, tag2={tag2}, tag3={tag3})"
            )

        rps_by_c = {}
        for c in conc:
            rps_by_c[c] = bench_mget(p1, c, duration_s, warmup_s)

        return BenchResult(
            fanout_ok=fanout_ok,
            response_len=resp_len,
            tags=(tag1, tag2),
            rps_by_concurrency=rps_by_c,
        )
    finally:
        if ports is not None:
            try:
                stop_cluster(workdir, base_port, nodes)
            except Exception:
                # best-effort cleanup
                pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--module", required=True, help="Path to redistimeseries.so")
    ap.add_argument("--base-port", type=int, default=31000)
    ap.add_argument("--series-per-shard", type=int, default=200)
    ap.add_argument("--duration", type=float, default=10.0)
    ap.add_argument("--warmup", type=float, default=2.0)
    ap.add_argument("--concurrency", type=str, default="1,2,4,8,16,32")
    ap.add_argument("--nodes", type=int, default=3, help="Number of Redis cluster masters to start (OSS requires >=3).")
    args = ap.parse_args()

    ensure_redis_bins()
    ensure_python_redis()

    module_path = os.path.realpath(args.module)
    if not os.path.exists(module_path):
        print(f"Module not found: {module_path}", file=sys.stderr)
        return 2

    conc = parse_concurrency_list(args.concurrency)
    res = run_benchmark(
        module_path=module_path,
        base_port=args.base_port,
        series_per_shard=args.series_per_shard,
        duration_s=args.duration,
        warmup_s=args.warmup,
        conc=conc,
        nodes=args.nodes,
    )

    print("fanout_ok:", res.fanout_ok)
    print("response_len:", res.response_len)
    print("tags:", res.tags)
    print("rps_by_concurrency:")
    for c in sorted(res.rps_by_concurrency):
        print(f"  {c}: {res.rps_by_concurrency[c]:.2f}")
    print(f"max_rps: {res.max_rps:.2f} (best_concurrency={res.best_concurrency})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

