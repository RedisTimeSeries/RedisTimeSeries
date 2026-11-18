#!/usr/bin/env python3
"""Run random traffic and make server exits / memory checker errors fail CI."""

import argparse
from collections import deque
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[2]


def generator_command(args):
    return [sys.executable, "-m", "redis_command_generator.TimeSeriesGen",
            "--hosts", "localhost:{}".format(args.port), "--verbose",
            "--max_cmd_cnt", str(args.max_commands)]


def ping(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1) as conn:
            conn.sendall(b"*1\r\n$4\r\nPING\r\n")
            return conn.recv(64) == b"+PONG\r\n"
    except OSError:
        return False


def cleanup(process):
    if process is None:
        return
    # Each subprocess owns a session, including any workers it starts.
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()


def run(args):
    logs = Path(args.log_dir).resolve()
    logs.mkdir(parents=True, exist_ok=True)
    if args.module:
        module = Path(args.module).resolve()
    else:
        modules = list((ROOT / "bin").rglob("redistimeseries.so"))
        if len(modules) != 1:
            raise RuntimeError("Expected one module build; found {}".format(modules))
        module = modules[0].resolve()
    if not module.is_file():
        raise RuntimeError("Module not found: {}".format(module))

    command = [args.redis_server, "--bind", "127.0.0.1", "--port", str(args.port),
               "--daemonize", "no", "--save", "", "--appendonly", "no",
               "--loadmodule", str(module)]
    env = os.environ.copy()
    if args.mode == "asan":
        # Override inherited recovery settings: every ASan/LSan finding must fail.
        env["ASAN_OPTIONS"] = (
            env.get("ASAN_OPTIONS", "") + ":detect_odr_violation=0:detect_leaks=1:"
            "halt_on_error=1:abort_on_error=0:exitcode=99:log_path=" + str(logs / "asan")
        )
        env["LSAN_OPTIONS"] = env.get("LSAN_OPTIONS", "") + ":exitcode=99"
    elif args.mode == "valgrind":
        env["RS_GLOBAL_DTORS"] = "1"
        command = ["valgrind", "--tool=memcheck", "--track-origins=yes",
                   "--leak-check=full", "--show-leak-kinds=definite",
                   "--errors-for-leak-kinds=definite", "--error-exitcode=99",
                   "--suppressions=" + str(ROOT / "tests/memcheck/redis_valgrind.sup"),
                   "--log-file=" + str(logs / "valgrind.%p.log")] + command

    print("Mode: {}; module: {}".format(args.mode, module), flush=True)
    server = traffic = None
    with (logs / "redis.log").open("w") as server_log, \
            (logs / "random_traffic.txt").open("w") as traffic_log:
        try:
            server = subprocess.Popen(command, env=env, stdout=server_log,
                                      stderr=subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic() + args.startup_timeout
            while True:
                if server.poll() is not None:
                    raise RuntimeError("Redis exited during startup: {}".format(server.returncode))
                if ping(args.port):
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError("Timed out waiting for Redis readiness")
                time.sleep(0.2)

            traffic = subprocess.Popen(generator_command(args), stdout=traffic_log,
                                       stderr=subprocess.STDOUT, start_new_session=True,
                                       env=dict(os.environ, PYTHONUNBUFFERED="1"))
            deadline = time.monotonic() + args.traffic_timeout
            while traffic.poll() is None:
                if server.poll() is not None:
                    raise RuntimeError("Redis exited during traffic: {}".format(server.returncode))
                if time.monotonic() >= deadline:
                    raise RuntimeError("Random traffic timed out")
                time.sleep(0.2)
            if traffic.returncode:
                raise RuntimeError("Traffic generator failed: {}".format(traffic.returncode))
            if server.poll() is not None:
                raise RuntimeError("Redis exited before shutdown: {}".format(server.returncode))

            # Wait for graceful Redis shutdown so Valgrind and LSan can report
            # errors found at exit. Checking only the generator would miss these.
            server.terminate()
            try:
                status = server.wait(timeout=args.shutdown_timeout)
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError("Redis shutdown timed out") from exc
            if status:
                raise RuntimeError("Redis / memory checker exited with status {}".format(status))
        finally:
            cleanup(traffic)
            cleanup(server)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("plain", "asan", "valgrind"), default="plain")
    parser.add_argument("--module")
    parser.add_argument("--redis-server", default="redis-server")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--max-commands", type=int, default=250000)
    parser.add_argument("--log-dir", default="random-traffic-logs")
    parser.add_argument("--startup-timeout", type=float, default=120)
    parser.add_argument("--traffic-timeout", type=float, default=10800)
    parser.add_argument("--shutdown-timeout", type=float, default=120)
    args = parser.parse_args()
    try:
        run(args)
    except (RuntimeError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        for path in sorted(Path(args.log_dir).glob("*")):
            if path.is_file():
                print("=== {} (last 100 lines) ===".format(path), flush=True)
                with path.open(errors="replace") as log:
                    print("".join(deque(log, maxlen=100)), flush=True)
    return 0


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(128 + signum))
    sys.exit(main())
