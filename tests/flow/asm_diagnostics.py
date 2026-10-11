"""Bounded runner measurements for the MOD-17216 timeout investigation."""
import json
import os
import socket
import struct
import subprocess
import sys
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
            "/sys/fs/cgroup/memory.current", "/proc/sys/kernel/osrelease",
            "/proc/sys/net/ipv4/tcp_rmem", "/proc/sys/net/ipv4/tcp_wmem",
            "/proc/sys/net/ipv4/tcp_moderate_rcvbuf", "/proc/sys/net/ipv4/tcp_window_scaling",
            "/proc/sys/net/ipv4/tcp_adv_win_scale", "/proc/net/sockstat",
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
        # Keep the runner network baseline even when the test passes.
        baseline = {key: value for key, value in samples[0].items()
                    if key.startswith('/proc/sys/') or key == 'cpu_count'}
        print('MOD-17216 network baseline: ' + json.dumps(baseline), flush=True)
        # Run only after the ASM workload, so this control cannot perturb it.
        # A separate process bounds even a blocked worker thread to 30 seconds.
        if sys.platform.startswith('linux'):
            try:
                control = subprocess.run(
                    [sys.executable, str(Path(__file__).with_name('asm_tcp_control.py'))],
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    timeout=30, check=False)
                result = {'returncode': control.returncode,
                          'output': control.stdout.decode(errors='replace')[:65536]}
            except subprocess.TimeoutExpired as error:
                result = {'diagnostic_error': 'TCP control exceeded 30 seconds',
                          'output': (error.stdout or b'').decode(errors='replace')[:65536]}
            except Exception as error:
                result = {'diagnostic_error': str(error)}
            print('MOD-17216 plain TCP control: ' + json.dumps(result), flush=True)


def log_runner_samples(samples):
    # Query socket diagnostics only after failure: periodic SOCK_DIAG dumps may
    # perturb the timing of the race we are trying to observe.
    print('MOD-17216 TCP at timeout: ' + json.dumps(tcp_window_snapshot()), flush=True)
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


def tcp_window_snapshot():
    """Read loopback TCP_INFO and socket memory through Linux SOCK_DIAG.

    Keep TCP_INFO as raw hex because its trailing fields vary by kernel version.
    This needs no extra executable or access to another process's descriptors.
    """
    result = {'time': time.time(), 'sockets': []}
    if not hasattr(socket, 'AF_NETLINK'):
        return dict(result, unavailable='Linux socket diagnostics unavailable')
    try:
        with socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, 4) as diag:
            # NETLINK_SOCK_DIAG=4, SOCK_DIAG_BY_FAMILY=20; dump established IPv4.
            # Extensions: INET_DIAG_INFO=2 and INET_DIAG_SKMEMINFO=7.
            request = struct.pack('=BBBBI', socket.AF_INET, socket.IPPROTO_TCP, 66, 0, 2)
            request += bytes(48)
            diag.sendto(struct.pack('=IHHII', 16 + len(request), 20, 0x301, 1, 0) + request, (0, 0))
            deadline = time.monotonic() + .2
            for _ in range(32):
                diag.settimeout(max(.001, deadline - time.monotonic()))
                packet = diag.recv(65536)
                offset = 0
                while offset + 16 <= len(packet):
                    length, kind, _, _, _ = struct.unpack_from('=IHHII', packet, offset)
                    if length < 16 or offset + length > len(packet):
                        return dict(result, unavailable='Malformed diagnostic message')
                    data = packet[offset + 16:offset + length]
                    offset += (length + 3) & ~3
                    if kind == 3:  # NLMSG_DONE
                        return result
                    if kind == 2:  # NLMSG_ERROR
                        return dict(result, unavailable='Netlink error: ' + data.hex())
                    if kind != 20 or len(data) < 72:
                        continue
                    if data[8:12] != bytes((127, 0, 0, 1)) or data[24:28] != bytes((127, 0, 0, 1)):
                        continue
                    sport, dport = struct.unpack_from('!HH', data, 4)
                    expires, rx, tx, uid, inode = struct.unpack_from('=IIIII', data, 52)
                    row = dict(sport=sport, dport=dport, timer=data[2], expires=expires,
                               rx_queue=rx, tx_queue=tx, inode=inode, attributes={})
                    pos = 72
                    while pos + 4 <= len(data):
                        size, attr = struct.unpack_from('=HH', data, pos)
                        if size < 4 or pos + size > len(data):
                            break
                        row['attributes'][str(attr)] = data[pos + 4:pos + size].hex()
                        pos += (size + 3) & ~3
                    result['sockets'].append(row)
                    if len(result['sockets']) >= 64:
                        return dict(result, truncated=True)
                if time.monotonic() >= deadline:
                    break
            result['truncated'] = True
    except OSError as error:
        result['unavailable'] = str(error)
    return result
