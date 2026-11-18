"""Exercise supervision with real subprocesses, without Redis or memory tools."""

import importlib.util
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock


spec = importlib.util.spec_from_file_location("random_traffic_runner", Path(__file__).with_name("run.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


FAKE_SERVER = r'''
import json, os, signal, socket, sys
from pathlib import Path
if os.environ.get("FAKE_STARTUP_FAILURE"):
    sys.exit(17)
signal.signal(signal.SIGTERM, lambda *_: sys.exit(int(os.environ.get("FAKE_SHUTDOWN_STATUS", "0"))))
if os.environ.get("FAKE_IGNORE_TERM"):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
print(json.dumps({key: os.environ.get(key) for key in
                 ["ASAN_OPTIONS", "LSAN_OPTIONS", "RS_GLOBAL_DTORS"]}), flush=True)
port = int(sys.argv[sys.argv.index("--port") + 1])
with socket.socket() as listener:
    listener.bind(("127.0.0.1", port))
    listener.listen()
    listener.settimeout(0.05)
    while True:
        if Path(os.environ["FAKE_CRASH_FILE"]).exists():
            sys.exit(int(os.environ.get("FAKE_CRASH_STATUS", "17")))
        try:
            connection, _ = listener.accept()
        except socket.timeout:
            continue
        with connection:
            if connection.recv(64):
                connection.sendall(b"+PONG\r\n")
'''

FAKE_VALGRIND = r'''
import os, sys
arguments = sys.argv[1:]
assert "--error-exitcode=99" in arguments
assert "--track-origins=yes" in arguments
log = next(arg.split("=", 1)[1] for arg in arguments if arg.startswith("--log-file="))
with open(log.replace("%p", str(os.getpid())), "w") as output:
    output.write("mock memcheck log\n")
while arguments[0].startswith("--"):
    arguments.pop(0)
os.execv(arguments[0], arguments)
'''


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.server = self.root / "redis-server"
        self.server.write_text("#!{}\n{}".format(sys.executable, FAKE_SERVER))
        self.server.chmod(0o755)
        valgrind = self.root / "valgrind"
        valgrind.write_text("#!{}\n{}".format(sys.executable, FAKE_VALGRIND))
        valgrind.chmod(0o755)
        self.module = self.root / "redistimeseries.so"
        self.module.touch()
        self.crash = self.root / "crash"
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        self.args = SimpleNamespace(
            mode="plain", module=str(self.module), redis_server=str(self.server),
            log_dir=str(self.root / "logs"), port=port, max_commands=10,
            startup_timeout=3, traffic_timeout=3, shutdown_timeout=3,
        )
        env = mock.patch.dict(os.environ, {
            "PATH": str(self.root) + os.pathsep + os.environ.get("PATH", ""),
            "FAKE_CRASH_FILE": str(self.crash),
        })
        env.start()
        self.addCleanup(env.stop)

    def run_with_generator(self, code):
        with mock.patch.object(runner, "generator_command", return_value=[sys.executable, "-c", code]):
            runner.run(self.args)

    def test_plain_success_and_logs(self):
        self.run_with_generator('print("traffic completed")')
        self.assertIn("traffic completed", (self.root / "logs/random_traffic.txt").read_text())
        self.assertTrue((self.root / "logs/redis.log").is_file())

    def test_generator_failure_fails_run(self):
        with self.assertRaisesRegex(RuntimeError, "Traffic generator failed: 9"):
            self.run_with_generator('import sys; print("generator failure"); sys.exit(9)')
        self.assertIn("generator failure", (self.root / "logs/random_traffic.txt").read_text())

    def test_server_crash_fails_run_and_stops_generator(self):
        pidfile = self.root / "generator.pid"
        code = ("import os, time; from pathlib import Path; "
                "Path({!r}).write_text(str(os.getpid())); "
                "Path({!r}).touch(); time.sleep(30)").format(str(pidfile), str(self.crash))
        with self.assertRaisesRegex(RuntimeError, "Redis exited during traffic: 17"):
            self.run_with_generator(code)
        with self.assertRaises(ProcessLookupError):
            os.kill(int(pidfile.read_text()), 0)

    def test_valgrind_error_at_shutdown_fails_run(self):
        self.args.mode = "valgrind"
        with mock.patch.dict(os.environ, {"FAKE_SHUTDOWN_STATUS": "99"}):
            with self.assertRaisesRegex(RuntimeError, "memory checker exited with status 99"):
                self.run_with_generator("pass")
        self.assertTrue(list((self.root / "logs").glob("valgrind.*.log")))
        env = json.loads((self.root / "logs/redis.log").read_text())
        self.assertEqual(env["RS_GLOBAL_DTORS"], "1")

    def test_asan_error_fails_run_and_overrides_recovery(self):
        self.args.mode = "asan"
        with mock.patch.dict(os.environ, {
            "FAKE_SHUTDOWN_STATUS": "99", "ASAN_OPTIONS": "halt_on_error=0",
        }):
            with self.assertRaisesRegex(RuntimeError, "memory checker exited with status 99"):
                self.run_with_generator("pass")
        env = json.loads((self.root / "logs/redis.log").read_text())
        self.assertIn("halt_on_error=0:", env["ASAN_OPTIONS"])
        self.assertIn("halt_on_error=1:", env["ASAN_OPTIONS"])
        self.assertIn("exitcode=99", env["LSAN_OPTIONS"])

    def test_startup_failure_fails_run(self):
        with mock.patch.dict(os.environ, {"FAKE_STARTUP_FAILURE": "1"}):
            with self.assertRaisesRegex(RuntimeError, "Redis exited during startup: 17"):
                self.run_with_generator("pass")

    def test_traffic_timeout_fails_run(self):
        self.args.traffic_timeout = 0.1
        with self.assertRaisesRegex(RuntimeError, "Random traffic timed out"):
            self.run_with_generator("import time; time.sleep(30)")

    def test_shutdown_timeout_fails_run(self):
        self.args.shutdown_timeout = 0.1
        with mock.patch.dict(os.environ, {"FAKE_IGNORE_TERM": "1"}):
            with self.assertRaisesRegex(RuntimeError, "Redis shutdown timed out"):
                self.run_with_generator("pass")

    def test_premature_clean_server_exit_still_fails(self):
        code = ("import time; from pathlib import Path; "
                "Path({!r}).touch(); time.sleep(30)").format(str(self.crash))
        with mock.patch.dict(os.environ, {"FAKE_CRASH_STATUS": "0"}):
            with self.assertRaisesRegex(RuntimeError, "Redis exited during traffic: 0"):
                self.run_with_generator(code)


if __name__ == "__main__":
    unittest.main()
