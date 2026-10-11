"""Bounded loopback probe. Never sets SO_RCVBUF; no Redis dependencies."""
import concurrent.futures
import json
import pathlib
import socket
import time

TOTAL = 64 * 1024 * 1024


def trial(chunk, pause, attempt):
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen()
    listener.settimeout(12)
    address = listener.getsockname()
    start = time.monotonic()
    result = dict(chunk=chunk, pause=pause, attempt=attempt)

    def sender():
        with listener.accept()[0] as raw:
            raw.settimeout(10)
            conn = raw
            try:
                data = b'x' * chunk
                for offset in range(0, TOTAL, chunk):
                    conn.sendall(data[:min(chunk, TOTAL-offset)])
            finally:
                conn.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(sender)
        received = 0
        initial = minimum = maximum = previous = None
        transitions = []
        try:
            raw = socket.create_connection(address, timeout=10)
            conn = raw
            with conn:
                initial = conn.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
                minimum = maximum = initial
                transitions = []
                received = 0
                previous = initial
                while received < TOTAL:
                    data = conn.recv(16384)
                    if not data:
                        raise RuntimeError('unexpected EOF')
                    received += len(data)
                    current = conn.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
                    minimum, maximum = min(minimum, current), max(maximum, current)
                    if current != previous and len(transitions) < 100:
                        transitions.append([round(time.monotonic()-start, 6), received, current])
                    previous = current
                    if pause and received % (1024*1024) < len(data):
                        time.sleep(pause)
                result.update(bytes=received, initial=initial, minimum=minimum,
                              maximum=maximum, final=previous, transitions=transitions)
            future.result(timeout=12)
        except Exception as error:
            result['error'] = repr(error)
            result.update(bytes=received, initial=initial, minimum=minimum,
                          maximum=maximum, final=previous, transitions=transitions)
            try:
                future.result(timeout=12)
            except Exception as error:
                result['sender_error'] = repr(error)
        finally:
            listener.close()
    result['seconds'] = round(time.monotonic()-start, 3)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    print(json.dumps({'kernel': pathlib.Path('/proc/sys/kernel/osrelease').read_text().strip(),
                      'tcp_rmem': pathlib.Path('/proc/sys/net/ipv4/tcp_rmem').read_text().strip()}), flush=True)
    for chunk in (16384, 65536, 1048576):
        for pause in (0, 0.01):
            for attempt in range(2):
                trial(chunk, pause, attempt)
