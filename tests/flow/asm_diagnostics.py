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


def process_transport_snapshot(pid):
    """Read Linux socket ownership, epoll interests and thread waits without attaching."""
    root = Path('/proc') / str(int(pid))
    snapshot = {'pid': int(pid), 'time': time.time()}

    def read(path):
        try:
            with path.open() as stream:
                return stream.read(16384)
        except OSError as error:
            return {'unavailable': str(error)}

    if not root.is_dir():
        return dict(snapshot, unavailable='Process proc directory is unavailable')
    sockets = set()
    descriptors = {}
    try:
        for path in sorted((root / 'fd').iterdir())[:256]:
            try:
                target = os.readlink(path)
            except OSError:
                continue  # A socket may close during this non-atomic snapshot.
            if target.startswith('socket:['):
                sockets.add(target[8:-1])
                descriptors[path.name] = {'target': target}
            elif target == 'anon_inode:[eventpoll]':
                descriptors[path.name] = {
                    'target': target, 'interests': read(root / 'fdinfo' / path.name)}
        snapshot['descriptors'] = descriptors
        for protocol in ('tcp', 'tcp6'):
            # /proc/PID/net is namespace-wide; retain only sockets owned by PID.
            with (root / 'net' / protocol).open() as stream:
                header = next(stream, '').strip()
                rows = [line.strip() for line in stream
                        if len(line.split()) > 9 and line.split()[9] in sockets]
            snapshot[protocol] = {'header': header, 'rows': rows}
    except OSError as error:
        snapshot['socket_error'] = str(error)
    try:
        snapshot['threads'] = {
            task.name: {name: read(task / name) for name in ('comm', 'stat', 'wchan', 'stack')}
            for task in sorted((root / 'task').iterdir())[:128]
        }
    except OSError as error:
        snapshot['thread_error'] = str(error)
    snapshot['finished_at'] = time.time()
    return snapshot
