import random
import select
import socket
import socketserver
import threading
import time
import unittest
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
    get_timeout,
)
from test_asm import validate_queries_during_migrations


ENDPOINT_TEST_KEYS = 8
ENDPOINT_TEST_SAMPLES = 2
ENDPOINT_TIMEOUT = 60 if (VALGRIND or SANITIZER) else 10


def _shard_connection(env, shard):
    return env.getConnection(shardId=shard + 1)


@contextmanager
def _endpoint_test_env(use_tls=False):
    if use_tls != Defaults.use_TLS:
        raise unittest.SkipTest()

    kwargs = {
        "shardsCount": 2,
        "decodeResponses": True,
        "skipRefreshCluster": True,
        "noLog": False,
    }
    env = Env(**kwargs)
    if env.env != "oss-cluster":
        env.skip()

    wait_for_valid_cluster(env)
    fill_ts_data(
        env,
        ENDPOINT_TEST_KEYS,
        ENDPOINT_TEST_SAMPLES,
        endpoint_event="yes",
    )
    _assert_connected_topology_query(env)
    yield env


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class _ThreadingTCPProxy(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


class _TCPProxyHandler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            with socket.create_connection(
                self.server.target,
                timeout=ENDPOINT_TIMEOUT,
            ) as upstream:
                peers = {
                    self.request: upstream,
                    upstream: self.request,
                }
                while not self.server.stop_event.is_set():
                    readable, _, _ = select.select(peers, [], [], 0.1)
                    for source in readable:
                        data = source.recv(65536)
                        if not data:
                            return
                        peers[source].sendall(data)
        except OSError:
            return


@contextmanager
def _tcp_proxy(target_host, target_port, listen_host="127.0.0.1", listen_port=0):
    server = _ThreadingTCPProxy((listen_host, listen_port), _TCPProxyHandler)
    server.target = (target_host, target_port)
    server.stop_event = threading.Event()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address[1]
    finally:
        server.stop_event.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=ENDPOINT_TIMEOUT)


def _can_connect(host, port):
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def _wait_for_tcp_endpoint(host, port):
    deadline = time.time() + ENDPOINT_TIMEOUT
    while time.time() < deadline:
        if _can_connect(host, port):
            return
        time.sleep(0.05)
    raise AssertionError(f"endpoint {host}:{port} did not start accepting connections")


def _infocluster_state(conn):
    conn.execute_command("DEBUG", "MARK-INTERNAL-CLIENT")
    reply = conn.execute_command("timeseries.INFOCLUSTER")
    if reply == "no cluster mode":
        return None, {}

    nodes = {}
    for raw_node in reply[4]:
        fields = dict(zip(raw_node[::2], raw_node[1::2]))
        nodes[fields["id"]] = fields
    return reply[1], nodes


def _connected_endpoint_snapshot(env):
    _, nodes = _infocluster_state(_shard_connection(env, 0))
    return {
        node_id: (node["ip"], int(node["port"]))
        for node_id, node in nodes.items()
    }


def _wait_for_topology(env, expected_endpoints=None, require_connected=True):
    expected_endpoints = expected_endpoints or {}
    deadline = time.time() + ENDPOINT_TIMEOUT
    last_seen = {}
    while time.time() < deadline:
        valid = True
        last_seen = {}
        for shard in range(env.shardsCount):
            conn = _shard_connection(env, shard)
            local_node_id, nodes = _infocluster_state(conn)
            last_seen[shard] = {
                "local_node_id": local_node_id,
                "nodes": {
                    node_id: (
                        node.get("ip"),
                        node.get("port"),
                        node.get("status"),
                    )
                    for node_id, node in nodes.items()
                },
            }
            if len(nodes) != env.shardsCount:
                valid = False
                continue
            for node in nodes.values():
                if require_connected and node.get("status") != "connected":
                    valid = False
            for node_id, (expected_ip, expected_port) in expected_endpoints.items():
                node = nodes.get(node_id)
                if node is None:
                    valid = False
                    continue
                if expected_ip is not None and node.get("ip") != expected_ip:
                    valid = False
                if expected_port is not None and int(node.get("port")) != expected_port:
                    valid = False
        if valid:
            return
        time.sleep(0.05)

    raise AssertionError(
        f"LibMR topology did not converge; require_connected={require_connected}, "
        f"expected endpoints={expected_endpoints}, last seen={last_seen}"
    )


def _assert_multishard_query(env):
    for shard in range(env.shardsCount):
        result = _shard_connection(env, shard).execute_command(
            "TS.MRANGE",
            "-",
            "+",
            "FILTER",
            "endpoint_event=yes",
        )
        assert len(result) == ENDPOINT_TEST_KEYS
        assert all(len(series[2]) == ENDPOINT_TEST_SAMPLES for series in result)


def _assert_connected_topology_query(env, expected_endpoints=None):
    # Wait until every observer has received the new topology before making
    # LibMR open its remote connections lazily on first use.
    _wait_for_topology(env, expected_endpoints, require_connected=False)
    _assert_multishard_query(env)
    _wait_for_topology(env, expected_endpoints)
    _assert_multishard_query(env)


def _node_topology_event_count(env, shard):
    logfile = _shard_connection(env, shard).execute_command(
        "CONFIG",
        "GET",
        "logfile",
    )[1]
    logfile = logfile.decode() if isinstance(logfile, bytes) else logfile
    path = Path(logfile)
    if not path.is_absolute():
        path = Path(env.logDir) / path
    with path.open(errors="replace") as server_log:
        return sum(
            "Cluster topology change:" in line and " NODE" in line
            for line in server_log
        )


def _wait_for_node_topology_event(env, shard, previous_count):
    deadline = time.time() + ENDPOINT_TIMEOUT
    while time.time() < deadline:
        current_count = _node_topology_event_count(env, shard)
        if current_count > previous_count:
            return
        time.sleep(0.05)
    raise AssertionError(
        f"shard {shard} NODE topology event count stayed {previous_count}"
    )


def _wait_for_topology_events_to_settle(env):
    previous_counts = None
    stable_since = time.time()
    deadline = time.time() + ENDPOINT_TIMEOUT
    while time.time() < deadline:
        counts = tuple(
            _node_topology_event_count(env, shard)
            for shard in range(env.shardsCount)
        )
        if counts != previous_counts:
            previous_counts = counts
            stable_since = time.time()
        elif time.time() - stable_since >= 0.5:
            return
        time.sleep(0.05)
    raise AssertionError("NODE topology events did not settle")


def _set_endpoint_config(env, shard, config, value):
    conn = _shard_connection(env, shard)
    previous_count = _node_topology_event_count(env, shard)
    assert conn.execute_command("CONFIG", "SET", config, value) == "OK"
    _wait_for_node_topology_event(env, shard, previous_count)


def _wait_for_cluster_hostname(env, node_id, expected_hostname):
    deadline = time.time() + ENDPOINT_TIMEOUT
    last_lines = {}
    while time.time() < deadline:
        matches = True
        last_lines = {}
        for shard in range(env.shardsCount):
            lines = (
                _shard_connection(env, shard)
                .execute_command("CLUSTER", "NODES")
                .splitlines()
            )
            line = next((line for line in lines if line.startswith(f"{node_id} ")), "")
            last_lines[shard] = line
            hostname = ClusterNode.from_str(line).hostname if line else None
            if hostname != expected_hostname:
                matches = False
        if matches:
            return
        time.sleep(0.05)
    raise AssertionError(
        f"hostname for {node_id} did not become {expected_hostname}; "
        f"last seen={last_lines}"
    )


def test_tls_cluster_change_reconnects_topology():
    with _endpoint_test_env(use_tls=True) as env:
        node_ids = [
            _infocluster_state(_shard_connection(env, shard))[0]
            for shard in range(env.shardsCount)
        ]
        original_values = [
            _shard_connection(env, shard).execute_command(
                "CONFIG",
                "GET",
                "tls-cluster",
            )[1]
            for shard in range(env.shardsCount)
        ]
        tls_endpoints = _connected_endpoint_snapshot(env)
        tcp_endpoints = {}

        for shard, node_id in enumerate(node_ids):
            conn = _shard_connection(env, shard)
            # tls-cluster also changes Redis's derived bus-port value, but the
            # live bus listener remains on its startup port. Pin that listener
            # so the two shards can still exchange their new client endpoints.
            assert conn.execute_command(
                "CONFIG",
                "SET",
                "cluster-announce-bus-port",
                tls_endpoints[node_id][1] + 10000,
            ) == "OK"
            tcp_port = _find_free_port()
            assert conn.execute_command("CONFIG", "SET", "port", tcp_port) == "OK"
            _wait_for_tcp_endpoint(
                conn.connection_pool.connection_kwargs["host"],
                tcp_port,
            )
            tcp_endpoints[node_id] = (tls_endpoints[node_id][0], tcp_port)
        _wait_for_topology_events_to_settle(env)

        for shard in range(env.shardsCount):
            _set_endpoint_config(env, shard, "tls-cluster", "no")
        _assert_connected_topology_query(env, tcp_endpoints)

        for shard, original_value in enumerate(original_values):
            _set_endpoint_config(env, shard, "tls-cluster", original_value)
        _assert_connected_topology_query(env, tls_endpoints)


def test_cluster_announce_ip_change_reconnects_topology():
    with _endpoint_test_env() as env:
        shard = 0
        conn = _shard_connection(env, shard)
        node_id, _ = _infocluster_state(conn)
        initial_endpoints = _connected_endpoint_snapshot(env)
        initial_ip, initial_port = initial_endpoints[node_id]
        # cluster-announce-ip explicitly permits resolvable hostnames. This
        # gives the replacement a real reachable address on every CI runner.
        alternate_ip = "localhost"
        assert alternate_ip != initial_ip

        # Pin every node's initial IP so changing one announcement does not
        # alter another node's auto-detected address as cluster links reconnect.
        for observer in range(env.shardsCount):
            observer_conn = _shard_connection(env, observer)
            observer_id, _ = _infocluster_state(observer_conn)
            assert observer_conn.execute_command(
                "CONFIG",
                "SET",
                "cluster-announce-ip",
                initial_endpoints[observer_id][0],
            ) == "OK"
        _wait_for_topology_events_to_settle(env)
        _assert_connected_topology_query(env, initial_endpoints)

        _set_endpoint_config(
            env,
            shard,
            "cluster-announce-ip",
            alternate_ip,
        )
        changed_endpoints = dict(initial_endpoints)
        changed_endpoints[node_id] = (alternate_ip, initial_port)
        _assert_connected_topology_query(env, changed_endpoints)

        _set_endpoint_config(
            env,
            shard,
            "cluster-announce-ip",
            initial_ip,
        )
        _assert_connected_topology_query(env, initial_endpoints)


def test_cluster_announce_hostname_change_keeps_topology_connected():
    with _endpoint_test_env() as env:
        shard = 0
        conn = _shard_connection(env, shard)
        node_id = conn.execute_command("CLUSTER", "MYID")
        initial_endpoints = _connected_endpoint_snapshot(env)
        original_hostname = conn.execute_command(
            "CONFIG",
            "GET",
            "cluster-announce-hostname",
        )[1]

        _set_endpoint_config(
            env,
            shard,
            "cluster-announce-hostname",
            "localhost",
        )
        _wait_for_cluster_hostname(env, node_id, "localhost")
        _assert_connected_topology_query(env, initial_endpoints)

        _set_endpoint_config(
            env,
            shard,
            "cluster-announce-hostname",
            original_hostname,
        )
        _wait_for_cluster_hostname(env, node_id, original_hostname or None)
        _assert_connected_topology_query(env, initial_endpoints)


def test_cluster_announce_port_change_reconnects_topology():
    with _endpoint_test_env() as env:
        shard = 0
        conn = _shard_connection(env, shard)
        node_id, _ = _infocluster_state(conn)
        initial_endpoints = _connected_endpoint_snapshot(env)
        target_host = conn.connection_pool.connection_kwargs["host"]
        target_port = initial_endpoints[node_id][1]
        original_value = conn.execute_command(
            "CONFIG",
            "GET",
            "cluster-announce-port",
        )[1]

        with _tcp_proxy(target_host, target_port) as proxy_port:
            _set_endpoint_config(
                env,
                shard,
                "cluster-announce-port",
                proxy_port,
            )
            changed_endpoints = dict(initial_endpoints)
            changed_endpoints[node_id] = (initial_endpoints[node_id][0], proxy_port)
            _assert_connected_topology_query(env, changed_endpoints)

            _set_endpoint_config(
                env,
                shard,
                "cluster-announce-port",
                original_value,
            )
            _assert_connected_topology_query(env, initial_endpoints)


def test_cluster_announce_tls_port_change_reconnects_topology():
    with _endpoint_test_env(use_tls=True) as env:
        shard = 0
        conn = _shard_connection(env, shard)
        node_id, _ = _infocluster_state(conn)
        initial_endpoints = _connected_endpoint_snapshot(env)
        target_host = conn.connection_pool.connection_kwargs["host"]
        target_port = initial_endpoints[node_id][1]
        original_value = conn.execute_command(
            "CONFIG",
            "GET",
            "cluster-announce-tls-port",
        )[1]

        with _tcp_proxy(target_host, target_port) as proxy_port:
            _set_endpoint_config(
                env,
                shard,
                "cluster-announce-tls-port",
                proxy_port,
            )
            changed_endpoints = dict(initial_endpoints)
            changed_endpoints[node_id] = (initial_endpoints[node_id][0], proxy_port)
            _assert_connected_topology_query(env, changed_endpoints)

            _set_endpoint_config(
                env,
                shard,
                "cluster-announce-tls-port",
                original_value,
            )
            _assert_connected_topology_query(env, initial_endpoints)


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
    timeout = get_timeout()
    deadline = time.time() + timeout
    while True:
        clusters = [ts_cluster_from_conn(env.getConnection(i)) for i in range(env.shardsCount)]
        if all(c is not None for c in clusters) and all(compare_clusters(clusters[0], c) for c in clusters[1:]):
            return
        assert time.time() < deadline, "timeseries.INFOCLUSTER did not reach a valid, agreed state in time"
        time.sleep(0.2)
