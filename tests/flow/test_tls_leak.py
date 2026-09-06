import socketserver
import threading
import time

from RLTest.debuggers import Valgrind
from includes import Env, is_line_in_server_log
from test_clusterset_noop import _long_form_clusterset


def test_tls_initialization_failure():
    """Run with --tls -V: Valgrind detects the SSL object lost on REDIS_ERR."""
    env = Env(moduleArgs="ts-topology-events no")
    if not env.useTLS or not isinstance(env.debugger, Valgrind):
        env.skip()

    class RejectTLS(socketserver.BaseRequestHandler):
        def handle(self):
            # Queue an invalid TLS record before the client's first SSL_connect.
            self.request.sendall(b"not a TLS server\r\n")

    conn = env.getConnection()
    try:
        conn.execute_command("DEBUG", "MARK-INTERNAL-CLIENT")
    except Exception:
        pass  # Older Redis versions do not have internal clients.
    old_tls_cluster = conn.execute_command("CONFIG", "GET", "tls-cluster")[1]
    conn.config_set("tls-cluster", "yes")
    try:
        with socketserver.TCPServer(("127.0.0.1", 0), RejectTLS) as peer:
            worker = threading.Thread(target=peer.serve_forever, daemon=True)
            worker.start()
            try:
                port = peer.server_address[1]
                my_port = conn.connection_pool.connection_kwargs["port"]
                conn.execute_command("timeseries.CLUSTERSET",
                                     *_long_form_clusterset(my_port, port))
                # Give the peer time to reject while checkTLS waits for Redis's
                # main-thread lock. Reconnects cover scheduling variations.
                with conn.pipeline(transaction=False) as pipe:
                    pipe.execute_command("timeseries.FORCESHARDSCONNECTION")
                    pipe.execute_command("DEBUG", "SLEEP", "0.1")
                    pipe.execute()
                deadline = time.monotonic() + 30
                while not is_line_in_server_log(env, f"SSL auth to 127.0.0.1:{port} failed"):
                    assert time.monotonic() < deadline, "Initial TLS failure path was not reached"
                    time.sleep(0.1)
                assert conn.ping()
                # Leak detection happens when RLTest stops Redis under Valgrind.
            finally:
                peer.shutdown()
                worker.join()
    finally:
        conn.config_set("tls-cluster", old_tls_cluster)
