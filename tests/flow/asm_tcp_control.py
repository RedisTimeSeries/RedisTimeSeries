"""Bounded loopback probe. Never sets SO_RCVBUF; no Redis dependencies."""
import concurrent.futures
import json
import pathlib
import socket
import ssl
import subprocess
import tempfile
import time

TOTAL = 64 * 1024 * 1024


def trial(chunk, pause, attempt, contexts=None, reply_bytes=None):
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen()
    listener.settimeout(12)
    address = listener.getsockname()
    start = time.monotonic()
    result = dict(chunk=chunk, pause=pause, attempt=attempt,
                  tls=contexts is not None, reply_bytes=reply_bytes)

    def sender():
        with listener.accept()[0] as raw:
            raw.settimeout(10)
            conn = contexts[0].wrap_socket(raw, server_side=True) if contexts else raw
            try:
                data = b'x' * chunk
                frame_size = reply_bytes or TOTAL
                for frame in range(0, TOTAL, frame_size):
                    if reply_bytes and conn.recv(1) != b'Q':
                        raise RuntimeError('missing request')
                    size = min(frame_size, TOTAL - frame)
                    for offset in range(0, size, chunk):
                        conn.sendall(data[:min(chunk, size-offset)])
            finally:
                conn.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(sender)
        received = 0
        initial = minimum = maximum = previous = None
        transitions = []
        try:
            raw = socket.create_connection(address, timeout=10)
            conn = contexts[1].wrap_socket(raw, server_hostname='localhost') if contexts else raw
            with conn:
                initial = conn.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
                minimum = maximum = initial
                transitions = []
                received = 0
                previous = initial
                frame_end = 0
                while received < TOTAL:
                    if received == frame_end:
                        frame_end = min(received + (reply_bytes or TOTAL), TOTAL)
                        if reply_bytes:
                            conn.sendall(b'Q')
                    data = conn.recv(min(16384, frame_end - received))
                    if not data:
                        raise RuntimeError('unexpected EOF')
                    received += len(data)
                    current = conn.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
                    minimum, maximum = min(minimum, current), max(maximum, current)
                    if current != previous and len(transitions) < 32:
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

    # Match request/reply bursts and TLS record boundaries without Redis or
    # LibMR. Certificates are ephemeral and stay in the temporary directory.
    try:
        with tempfile.TemporaryDirectory(prefix='mod17216-tls-') as directory:
            cert = str(pathlib.Path(directory) / 'cert.pem')
            key = str(pathlib.Path(directory) / 'key.pem')
            subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048',
                            '-nodes', '-keyout', key, '-out', cert, '-days', '1',
                            '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost'],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           timeout=5, check=True)
            server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server.load_cert_chain(cert, key)
            client = ssl.create_default_context(cafile=cert)
            print(json.dumps({'openssl': ssl.OPENSSL_VERSION}), flush=True)
            for contexts in (None, (server, client)):
                for chunk in (16384, 65536, 1048576):
                    for pause in (0, 0.01):
                        trial(chunk, pause, 0, contexts=contexts, reply_bytes=3 * 1024 * 1024)
    except Exception as error:
        print(json.dumps({'tls_control_setup_error': repr(error)}), flush=True)
