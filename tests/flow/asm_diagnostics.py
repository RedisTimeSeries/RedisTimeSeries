"""Bounded runner measurements for the MOD-17216 timeout investigation."""
import json
import os
import time
from collections import deque
from contextlib import contextmanager
from pathlib import Path
from threading import Event, Thread


@contextmanager
def runner_samples():
    samples = deque(maxlen=30)
    done = Event()

    def sample():
        snapshot = {"time": time.time(), "cpu_count": os.cpu_count()}
        for name in (
            "/proc/stat", "/proc/loadavg", "/proc/pressure/cpu", "/proc/pressure/memory",
            "/proc/self/cgroup", "/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/cpu.max",
            "/sys/fs/cgroup/cpu.pressure", "/sys/fs/cgroup/memory.events",
            "/sys/fs/cgroup/memory.current",
        ):
            try:
                snapshot[name] = Path(name).read_text()
            except OSError as error:
                snapshot[name] = {"unavailable": str(error)}
        samples.append(snapshot)

    def observe():
        while not done.wait(1):
            sample()

    sample()
    worker = Thread(target=observe, daemon=True)
    worker.start()
    try:
        yield samples
    finally:
        done.set()
        worker.join(timeout=2)


def log_runner_samples(samples):
    print("MOD-17216 runner samples: " + json.dumps(list(samples)), flush=True)
