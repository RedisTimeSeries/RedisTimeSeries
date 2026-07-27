import random
import subprocess
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from pathlib import Path

from includes import *
from utils import (
    fill_ts_data,
    wait_for_valid_cluster,
    compare_clusters,
    ClusterNode,
    SlotRange,
    NUMBER_OF_SLOTS,
    failover_node,
    added_slaves_to_cluster,
)
from test_asm import validate_queries_during_migrations


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


@contextmanager
def _endpoint_test_env(use_tls=False):
    kwargs = {
        "shardsCount": 1,
        "decodeResponses": True,
        "skipRefreshCluster": True,
        "noLog": False,
    }

    if not use_tls:
        env = Env(**kwargs)
        if env.env != "oss-cluster":
            env.skip()
        yield env
        return

    with tempfile.TemporaryDirectory() as cert_dir:
        cert, key = _create_test_certificate(cert_dir)
        env = Env(
            **kwargs,
            useTLS=True,
            dualTLS=True,
            tlsCertFile=str(cert),
            tlsKeyFile=str(key),
            tlsCaCertFile=str(cert),
        )
        if env.env != "oss-cluster":
            env.skip()
        yield env


def _wait_for_cached_endpoint(conn, expected_ip=None, expected_port=None, timeout=5):
    deadline = time.time() + timeout
    last_node = None
    while time.time() < deadline:
        cluster = ts_cluster_from_conn(conn)
        if cluster is not None and len(cluster) == 1:
            last_node = next(iter(cluster.values()))
            ip_matches = expected_ip is None or last_node.ip == expected_ip
            port_matches = expected_port is None or last_node.port == expected_port
            if ip_matches and port_matches:
                return
        time.sleep(0.05)
    raise AssertionError(
        f"cached topology endpoint stayed {last_node}; "
        f"expected ip={expected_ip}, port={expected_port}"
    )


def _node_topology_event_count(env):
    logfile = env.getConnection(0).execute_command("CONFIG", "GET", "logfile")[1]
    logfile = logfile.decode() if isinstance(logfile, bytes) else logfile
    path = Path(logfile)
    if not path.is_absolute():
        path = Path(env.logDir) / path
    with path.open(errors="replace") as server_log:
        return sum(
            "Cluster topology change:" in line and " NODE" in line
            for line in server_log
        )


def _wait_for_node_topology_event(env, previous_count, timeout=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        current_count = _node_topology_event_count(env)
        if current_count > previous_count:
            return
        time.sleep(0.05)
    raise AssertionError(
        f"NODE topology event count stayed {previous_count}"
    )


def _set_endpoint_config(
    env,
    conn,
    config,
    value,
    expected_ip=None,
    expected_port=None,
):
    previous_count = _node_topology_event_count(env)
    assert conn.execute_command("CONFIG", "SET", config, value) == "OK"
    _wait_for_node_topology_event(env, previous_count)
    if expected_ip is not None or expected_port is not None:
        _wait_for_cached_endpoint(conn, expected_ip, expected_port)


def test_tls_cluster_change_refreshes_cached_topology():
    with _endpoint_test_env(use_tls=True) as env:
        conn = env.getConnection(0)
        tls_port = int(conn.execute_command("CONFIG", "GET", "tls-port")[1])
        tcp_port = int(conn.execute_command("CONFIG", "GET", "port")[1])
        assert tls_port != tcp_port

        _wait_for_cached_endpoint(conn, expected_port=tls_port)
        _set_endpoint_config(env, conn, "tls-cluster", "no", expected_port=tcp_port)
        _set_endpoint_config(env, conn, "tls-cluster", "yes", expected_port=tls_port)


def test_cluster_announce_ip_change_refreshes_cached_topology():
    with _endpoint_test_env() as env:
        conn = env.getConnection(0)

        _set_endpoint_config(
            env,
            conn,
            "cluster-announce-ip",
            "127.0.0.2",
            expected_ip="127.0.0.2",
        )
        _set_endpoint_config(
            env,
            conn,
            "cluster-announce-ip",
            "",
            expected_ip="",
        )


def test_cluster_announce_hostname_change_refreshes_cached_topology():
    with _endpoint_test_env() as env:
        conn = env.getConnection(0)

        _set_endpoint_config(
            env,
            conn,
            "cluster-announce-hostname",
            "node.example.test",
        )
        assert ",node.example.test " in conn.execute_command("CLUSTER", "NODES")

        _set_endpoint_config(env, conn, "cluster-announce-hostname", "")
        assert ",node.example.test " not in conn.execute_command("CLUSTER", "NODES")


def test_cluster_announce_port_change_refreshes_cached_topology():
    with _endpoint_test_env() as env:
        conn = env.getConnection(0)
        initial_port = int(conn.execute_command("CONFIG", "GET", "port")[1])

        _wait_for_cached_endpoint(conn, expected_port=initial_port)
        _set_endpoint_config(
            env,
            conn,
            "cluster-announce-port",
            32001,
            expected_port=32001,
        )
        _set_endpoint_config(
            env,
            conn,
            "cluster-announce-port",
            0,
            expected_port=initial_port,
        )


def test_cluster_announce_tls_port_change_refreshes_cached_topology():
    with _endpoint_test_env(use_tls=True) as env:
        conn = env.getConnection(0)
        initial_tls_port = int(conn.execute_command("CONFIG", "GET", "tls-port")[1])

        _wait_for_cached_endpoint(conn, expected_port=initial_tls_port)
        _set_endpoint_config(
            env,
            conn,
            "cluster-announce-tls-port",
            32002,
            expected_port=32002,
        )
        _set_endpoint_config(
            env,
            conn,
            "cluster-announce-tls-port",
            0,
            expected_port=initial_tls_port,
        )


def test_asm():
    env = Env(shardsCount=3, decodeResponses=True, skipRefreshCluster=True)
    skip_if_needed(env)

    def post_migration(env):
        wait_for_valid_cluster(env)
        wait_for_valid_ts_infocluster(env)

    fill_some_data(env)
    validate_queries_during_migrations(env, post_migration, COMMAND, validate_result)

def test_failover():
    env = Env(shardsCount=3, decodeResponses=True, skipRefreshCluster=True)
    skip_if_needed(env)
    if env.useTLS:
        env.skip()

    def post_failover(env):
        wait_for_valid_cluster(env)
        wait_for_valid_ts_infocluster(env)

    with added_slaves_to_cluster(env):
        fill_some_data(env)
        validate_queries_during_failovers(env, post_failover, COMMAND, validate_result)


def test_clusterset_after_topology_event_keeps_current_topology():
    env = Env(shardsCount=3, decodeResponses=True, skipRefreshCluster=True)
    skip_if_needed(env)

    wait_for_valid_cluster(env)
    wait_for_valid_ts_infocluster(env)

    conn = env.getConnection(0)
    expected = ts_cluster_from_conn(conn)

    # CLUSTERSET is obsolete after topology events take over, so it should be
    # ignored without freeing the topology that was installed by the event.
    assert conn.execute_command("timeseries.CLUSTERSET") == "OK"
    actual = ts_cluster_from_conn(conn)
    assert actual is not None, "CLUSTERSET discarded the topology installed by the event"
    assert compare_clusters(expected, actual)


# Helpers:

NUMBER_OF_KEYS = 1000 if not (VALGRIND or SANITIZER) else 100
SAMPLES_PER_KEY = 150
COMMAND = "TS.MRANGE - + FILTER label1=17 GROUPBY label1 REDUCE count"


def skip_if_needed(env):
    if env.env != "oss-cluster":
        env.skip()

    # macos-15-intel is the slowest hosted runner and can't reliably serve the multi-shard
    # query within LibMR's 5s max-idle during migration churn (MOD-14615 residual).
    if RUNNER_LABEL == "macos-15-intel":
        env.skip()

def fill_some_data(env):
    fill_ts_data(env, NUMBER_OF_KEYS, SAMPLES_PER_KEY, label1=17, label2=19)


def validate_result(result):
    ((filtered_by, withlabels, samples),) = result
    assert filtered_by == "label1=17"
    assert withlabels == []  # No WITHLABLES
    assert len(samples) == SAMPLES_PER_KEY
    assert all(int(sample[1]) == NUMBER_OF_KEYS for sample in samples)


def validate_queries_during_failovers(env, post_failover, command, validate_result):
    TOPOLOGY_CHANGED_ERROR = "A multi-shard command failed because the cluster topology has changed"
    # Clients of multi-shard commands are blocked. If a node that serves such a command is demoted
    # while the client is still blocked we expect the following error:
    UNBLOCKED_ERROR = "UNBLOCKED force unblock from blocking operation, instance state changed (master -> replica?)"
    # During a failover there is a brief period when the cluster state is set to fail
    # with cluster-allow-reads-when-down off it then rejects reads until it recovers (sub-second)
    # during which time we expect the following error:
    CLUSTERDOWN_ERROR = "CLUSTERDOWN The cluster is down"
    # A multi-shard fan-out can race slot-ownership propagation during a failover (the owning node's
    # id changes) and momentarily see a slot as unavailable, so we also expect this:
    SLOT_RANGES_ERROR = "Query requires unavailable slots"

    master_conns = {}

    def random_master_conn():
        masters = [
            node
            for node in map(ClusterNode.from_str, env.getConnection(0).execute_command("CLUSTER", "NODES").splitlines())
            if "master" in node.flags
        ]
        node = random.choice(masters)
        return master_conns.setdefault(
            (node.ip, node.port), redis.Redis(host=node.ip, port=node.port, decode_responses=True)
        )

    def strict_validation(env):
        validate_result(random_master_conn().execute_command(command))

    def tolerable_validation(env):
        try:
            result = random_master_conn().execute_command(command)
        except redis.exceptions.ResponseError as x:
            assert str(x) in (TOPOLOGY_CHANGED_ERROR, UNBLOCKED_ERROR, CLUSTERDOWN_ERROR, SLOT_RANGES_ERROR), str(x)
            return
        validate_result(result)

    def validate_after_failover(env):
        post_failover(env)
        strict_validation(env)

    strict_validation(env)

    done = threading.Event()

    def validate_command_in_a_loop():
        while not done.is_set():
            tolerable_validation(env)

    def failover_back_and_forth():
        failover_all_slaves(env, validate_after_failover)
        failover_all_slaves(env, validate_after_failover)

    with ThreadPoolExecutor() as executor:
        # Just one round-robin and back, unfortunately. We can't loop this since redis rate-limits
        # failover votes to once per 2*cluster-node-timeout per demoted primary, so a second cycle
        # would have to wait out that cooldown period before it could failover the same nodes again.
        # This will make the test too long.
        futures = map(executor.submit, [validate_command_in_a_loop, failover_back_and_forth])
        for future in as_completed(futures):
            done.set()
            future.result()

    strict_validation(env)


def failover_all_slaves(env, validator=None):
    nodes = {
        node.id: node
        for node in map(ClusterNode.from_str, env.getConnection(0).execute_command("CLUSTER", "NODES").splitlines())
    }
    for replica in [node for node in nodes.values() if "slave" in node.flags]:
        master = nodes[replica.master]
        failover_node(redis.Redis(host=replica.ip, port=replica.port, decode_responses=True))
        if validator is not None:
            print(f"\n----- master {master.ip}:{master.port} failed over to replica {replica.ip}:{replica.port} -----")
            validator(env)


def ts_cluster_from_conn(conn):
    """Parse timeseries.INFOCLUSTER as seen by conn into a dict of ClusterNode.id -> ClusterNode,
    or None if its slots don't fully and uniquely cover the keyspace."""
    conn.execute_command("debug", "MARK-INTERNAL-CLIENT")
    reply = conn.execute_command("timeseries.INFOCLUSTER")
    if reply == "no cluster mode":
        return None
    nodes = {}
    total = 0
    min_start = NUMBER_OF_SLOTS
    max_end = -1
    for node in reply[4]:
        fields = {}
        slot_ranges = []
        for key, val in zip(node[::2], node[1::2]):
            if key == "minHslot":
                slot_ranges.append([val])
            elif key == "maxHslot":
                slot_ranges[-1].append(val)
            else:
                fields[key] = val
        slots = {SlotRange(lo, hi) for lo, hi in slot_ranges}
        nodes[fields["id"]] = ClusterNode(
            id=fields["id"],
            ip=fields["ip"],
            port=int(fields["port"]),
            flags={"master"},  # timeseries.INFOCLUSTER only exposes master nodes
            slots=slots,
        )
        for sr in slots:
            total += sr.end - sr.start + 1
            min_start = min(min_start, sr.start)
            max_end = max(max_end, sr.end)

    if min_start != 0 or max_end != NUMBER_OF_SLOTS - 1 or total != NUMBER_OF_SLOTS:
        return None
    return nodes


def wait_for_valid_ts_infocluster(env):
    """Wait until every node's timeseries.INFOCLUSTER reports full coverage and all nodes agree."""
    timeout = 60 if (VALGRIND or SANITIZER) else 5
    deadline = time.time() + timeout
    while True:
        clusters = [ts_cluster_from_conn(env.getConnection(i)) for i in range(env.shardsCount)]
        if all(c is not None for c in clusters) and all(compare_clusters(clusters[0], c) for c in clusters[1:]):
            return
        assert time.time() < deadline, "timeseries.INFOCLUSTER did not reach a valid, agreed state in time"
        time.sleep(0.2)
