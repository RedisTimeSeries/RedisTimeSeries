import subprocess
import tempfile
import time
from pathlib import Path

from includes import Env


def _create_test_certificate(directory):
    cert = Path(directory) / "redis.crt"
    key = Path(directory) / "redis.key"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign",
            "-addext",
            "extendedKeyUsage=serverAuth,clientAuth",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return cert, key


def _cached_cluster_ports(conn):
    info = conn.execute_command("timeseries.INFOCLUSTER")
    return [
        int(dict(zip(node[::2], node[1::2]))["port"])
        for node in info[4]
    ]


def _wait_for_cached_port(conn, expected, timeout=5):
    deadline = time.time() + timeout
    last_ports = []
    while time.time() < deadline:
        last_ports = _cached_cluster_ports(conn)
        if last_ports == [expected]:
            return
        time.sleep(0.05)
    raise AssertionError(
        f"cached topology ports stayed {last_ports}; expected [{expected}]"
    )


def test_tls_cluster_change_refreshes_cached_topology():
    """Changing tls-cluster must refresh the module's cached preferred port."""
    with tempfile.TemporaryDirectory() as cert_dir:
        cert, key = _create_test_certificate(cert_dir)
        env = Env(
            shardsCount=1,
            decodeResponses=True,
            useTLS=True,
            dualTLS=True,
            tlsCertFile=str(cert),
            tlsKeyFile=str(key),
            tlsCaCertFile=str(cert),
        )
        if env.env != "oss-cluster":
            env.skip()

        conn = env.getConnection(0)
        conn.execute_command("DEBUG", "MARK-INTERNAL-CLIENT")
        tls_port = int(conn.execute_command("CONFIG", "GET", "tls-port")[1])
        tcp_port = int(conn.execute_command("CONFIG", "GET", "port")[1])
        assert tls_port != tcp_port

        _wait_for_cached_port(conn, tls_port)
        assert conn.execute_command("CONFIG", "SET", "tls-cluster", "no") == "OK"
        _wait_for_cached_port(conn, tcp_port)

        assert conn.execute_command("CONFIG", "SET", "tls-cluster", "yes") == "OK"
        _wait_for_cached_port(conn, tls_port)
