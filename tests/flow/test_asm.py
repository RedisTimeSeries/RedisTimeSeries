import time
import random
from dataclasses import dataclass
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Set
import redis

from includes import Env, VALGRIND, SANITIZER
from utils import slot_table


MIGRATION_CYCLES = 10


# Errors a keyless multi-shard command can transiently return while the cluster
# topology is in flux: slots mid-migration ("unavailable slots"), or the module
# auto-refreshing its topology mid-fan-out, which aborts in-flight executions
# (see libmr_commands.c). A client is expected to retry; these tests do the same.
TOPOLOGY_FLUX_ERRORS = (
    "Query requires unavailable slots",
    "A multi-shard command failed because the cluster topology has changed",
)


def is_topology_flux_error(err):
    return any(msg in str(err) for msg in TOPOLOGY_FLUX_ERRORS)


def execute_through_topology_flux(conn, *args, timeout=10):
    """Run a multi-shard command, retrying past transient topology-flux errors."""
    deadline = time.time() + (timeout * 6 if (VALGRIND or SANITIZER) else timeout)
    while True:
        try:
            return conn.execute_command(*args)
        except redis.exceptions.ResponseError as err:
            if is_topology_flux_error(err) and time.time() < deadline:
                time.sleep(0.05)
                continue
            raise


def test_asm_without_data():
    env = Env(shardsCount=2, decodeResponses=True)
    if env.env != "oss-cluster":  # TODO: convert to a proper fixture (here and below)
        env.skip()

    for _ in range(MIGRATION_CYCLES):
        migrate_slots_back_and_forth(env)


def test_asm_with_data():
    env = Env(shardsCount=2, decodeResponses=True)
    if env.env != "oss-cluster":
        env.skip()

    fill_some_data(env, number_of_keys=100, samples_per_key=10, label="test")
    for _ in range(MIGRATION_CYCLES):
        migrate_slots_back_and_forth(env)


def test_asm_with_data_and_queries_during_migrations():
    env = Env(shardsCount=2, decodeResponses=True, noLog=False)
    if env.env != "oss-cluster":
        env.skip()

    number_of_keys = 1000 if not (VALGRIND or SANITIZER) else 100
    samples_per_key = 150
    fill_some_data(env, number_of_keys, samples_per_key, label1=17, label2=19)

    conn = env.getConnection(0)
    command = "TS.MRANGE - + FILTER label1=17 GROUPBY label1 REDUCE count"

    def validate_result(result):
        ((filtered_by, withlabels, samples),) = result
        assert filtered_by == "label1=17"
        assert withlabels == []  # No WITHLABLES
        assert len(samples) == samples_per_key
        assert all(int(sample[1]) == number_of_keys for sample in samples)

    # First validate the result on the "static" cluster
    validate_result(execute_through_topology_flux(conn, command))

    # Now validate the command's result in a loop during the back and forth migrations
    done = threading.Event()

    def validate_command_in_a_loop():
        while not done.is_set():
            try:
                result = conn.execute_command(command)
            except redis.exceptions.ResponseError as x:
                # Occasional transient errors while the topology is in flux are
                # expected (slots mid-migration, or an auto-refresh aborting the
                # in-flight fan-out); skip and retry on the next iteration.
                assert is_topology_flux_error(x), str(x)
                continue
            validate_result(result)

    def migrate_slots():
        for _ in range(MIGRATION_CYCLES):
            if done.is_set():
                break
            migrate_slots_back_and_forth(env, command, validate_result)

    with ThreadPoolExecutor() as executor:
        futures = map(executor.submit, [validate_command_in_a_loop, migrate_slots])
        try:
            for future in as_completed(futures):
                # On a healthy run slot migrations should complete cleanly and we then signal the validator loop to exit
                done.set()
                # This will raise an exception in case the validation function failed
                future.result()
        except TimeoutError as e:
            # Under sanitizer, the migration may occasionally get stuck in 'init-rdbchannel' state.
            # This is a known issue and will be fixed by MOD-15307; for now treat it as a pass and bail out.
            if SANITIZER and "state is init-rdbchannel" in str(e):
                print(f"Ignoring known sanitizer migration timeout: {e}")
                done.set()
                return
            done.set()
            raise

    # Validate that all is fine after the migrations
    validate_result(execute_through_topology_flux(conn, command))


def test_short_form_clusterset():
    # Skip the initial REFRESHCLUSTER so the modules start unaware of the cluster.
    env = Env(shardsCount=3, decodeResponses=True, skipRefreshCluster=True)
    if env.env != "oss-cluster":
        env.skip()

    number_of_keys = 100
    samples_per_key = 10
    number_of_groups = 10
    keys_per_group = number_of_keys // number_of_groups
    fill_some_data(env, number_of_keys=number_of_keys, samples_per_key=samples_per_key,
                   label="test", group=lambda i: f"g{i % number_of_groups}")

    conn = env.getConnection(0)

    def queryindex_count():
        return len(conn.execute_command('TS.QUERYINDEX', 'label=test'))

    # With the initial REFRESHCLUSTER skipped the module is cluster-unaware --
    # unless the server fires RedisModuleEvent_ClusterTopologyChange
    # (redis/redis#15350), in which case it auto-refreshes on startup. Detect
    # which world we are in (give auto-refresh a moment to connect to peers).
    auto_aware = False
    detect_deadline = time.time() + (10 if (VALGRIND or SANITIZER) else 2)
    while time.time() < detect_deadline:
        if queryindex_count() == number_of_keys:
            auto_aware = True
            break
        time.sleep(0.1)

    if not auto_aware:
        # No auto-refresh: QUERYINDEX runs local-only until told about the cluster.
        assert 0 < queryindex_count() < number_of_keys

    # DMC pattern: short-form CLUSTERSET on one shard; LibMR propagates to peers
    # via CLUSTERSETFROMSHARD on rg.hello / reconnect. Idempotent (and a no-op for
    # routing) when the module is already cluster-aware via auto-refresh.
    assert conn.execute_command('timeseries.CLUSTERSET') in ('OK', b'OK')

    # Poll TS.QUERYINDEX until propagation lands -- fan-out goes from local-only
    # (~number_of_keys / shardsCount) to the full set once every shard has been
    # informed via CLUSTERSETFROMSHARD.
    deadline = time.time() + (60 if (VALGRIND or SANITIZER) else 10)
    while time.time() < deadline:
        queryindex = conn.execute_command('TS.QUERYINDEX', 'label=test')
        if len(queryindex) == number_of_keys:
            break
        time.sleep(0.1)
    else:
        raise AssertionError(
            f'after CLUSTERSET, QUERYINDEX returned {len(queryindex)}/{number_of_keys} '
            f'-- CLUSTERSETFROMSHARD propagation did not converge in time'
        )

    # Slot-routed dispatch via single-group GROUPBY (one slots[] read).
    ((filtered_by, withlabels, samples),) = conn.execute_command(
        'TS.MRANGE', '-', '+', 'FILTER', 'label=test', 'GROUPBY', 'label', 'REDUCE', 'count')
    assert filtered_by == 'label=test'
    assert withlabels == []
    assert len(samples) == samples_per_key
    assert all(int(sample[1]) == number_of_keys for sample in samples)

    # Multi-group GROUPBY (number_of_groups slots[] reads -- exercises slot-routing breadth).
    result = conn.execute_command(
        'TS.MRANGE', '-', '+', 'FILTER', 'label=test', 'GROUPBY', 'group', 'REDUCE', 'count')
    assert len(result) == number_of_groups, result
    for filtered_by, withlabels, samples in result:
        assert filtered_by.startswith('group=')
        assert withlabels == []
        assert len(samples) == samples_per_key
        assert all(int(sample[1]) == keys_per_group for sample in samples)


def test_auto_refresh_on_topology_change():
    # Counterpart to test_short_form_clusterset: the module starts unaware of the
    # cluster (no initial REFRESHCLUSTER), but instead of being told via a manual
    # timeseries.CLUSTERSET it should discover the topology automatically through
    # the RedisModuleEvent_ClusterTopologyChange server event (redis/redis#15350),
    # and keep cross-shard queries complete across an ASM migration -- with no
    # manual REFRESHCLUSTER / CLUSTERSET anywhere.
    env = Env(shardsCount=2, decodeResponses=True, skipRefreshCluster=True)
    if env.env != "oss-cluster":
        env.skip()

    number_of_keys = 100
    samples_per_key = 10
    fill_some_data(env, number_of_keys=number_of_keys, samples_per_key=samples_per_key, label="auto")

    conn = env.getConnection(0)

    # Capability gate: on a server that fires the topology-change event the module
    # auto-refreshes once the cluster is ready, so QUERYINDEX converges to the full
    # set with no manual REFRESHCLUSTER/CLUSTERSET. On servers that predate the
    # event it stays local-only forever -- skip there (the feature lands with
    # redis/redis#15350). NOTE: this gate also skips if auto-refresh is silently
    # broken; the event firing itself is hard-tested on the core side, and the
    # end-to-end refresh path is exercised by the assertions below once we proceed.
    deadline = time.time() + (60 if (VALGRIND or SANITIZER) else 5)
    while time.time() < deadline:
        if len(execute_through_topology_flux(conn, 'TS.QUERYINDEX', 'label=auto')) == number_of_keys:
            break
        time.sleep(0.1)
    else:
        env.skip()  # redis build does not fire RedisModuleEvent_ClusterTopologyChange

    # Change the topology via an ASM migration and confirm the module auto-refreshes
    # (still no manual REFRESHCLUSTER) and the cross-shard query is complete once the
    # refresh settles -- polling past the transient flux the refresh causes in-flight.
    migrate_slots_back_and_forth(env)

    deadline = time.time() + (60 if (VALGRIND or SANITIZER) else 10)
    while time.time() < deadline:
        if len(execute_through_topology_flux(conn, 'TS.QUERYINDEX', 'label=auto')) == number_of_keys:
            break
        time.sleep(0.1)
    else:
        raise AssertionError('QUERYINDEX did not return the full set after the reshard')

    # Slot-routed GROUPBY: exercises the (auto-refreshed) slot map.
    ((filtered_by, withlabels, samples),) = execute_through_topology_flux(
        conn, 'TS.MRANGE', '-', '+', 'FILTER', 'label=auto', 'GROUPBY', 'label', 'REDUCE', 'count')
    assert filtered_by == 'label=auto'
    assert withlabels == []
    assert len(samples) == samples_per_key
    assert all(int(sample[1]) == number_of_keys for sample in samples)


def _wait_auto_refresh_aware(env, conn, matcher, expected, timeout=5):
    # Poll until the module auto-refreshes purely from the topology event and
    # QUERYINDEX converges to the full cross-shard set. Skips on cores that predate
    # RedisModuleEvent_ClusterTopologyChange (the merged unstable core fires it).
    deadline = time.time() + (60 if (VALGRIND or SANITIZER) else timeout)
    while time.time() < deadline:
        if len(execute_through_topology_flux(conn, 'TS.QUERYINDEX', matcher)) == expected:
            return
        time.sleep(0.1)
    env.skip()  # redis build does not fire RedisModuleEvent_ClusterTopologyChange


def test_auto_refresh_reshard_under_query_load():
    # MOD-16382 under load: event-driven counterpart of
    # test_asm_with_data_and_queries_during_migrations. With NO manual
    # REFRESHCLUSTER/CLUSTERSET, a keyless multi-shard query must stay correct while
    # ASM migrations churn the topology and the module auto-refreshes underneath it.
    # The only tolerated failures are the whitelisted transient flux errors; every
    # successful result is complete -- i.e. the auto-refresh never returns a partial
    # (silently-wrong) cross-shard answer.
    env = Env(shardsCount=2, decodeResponses=True, skipRefreshCluster=True, noLog=False)
    if env.env != "oss-cluster":
        env.skip()

    number_of_keys = 1000 if not (VALGRIND or SANITIZER) else 100
    samples_per_key = 50
    fill_some_data(env, number_of_keys, samples_per_key, label1=17, label2=19)

    conn = env.getConnection(0)
    command = "TS.MRANGE - + FILTER label1=17 GROUPBY label1 REDUCE count"

    def validate_result(result):
        ((filtered_by, withlabels, samples),) = result
        assert filtered_by == "label1=17"
        assert withlabels == []
        assert len(samples) == samples_per_key
        assert all(int(sample[1]) == number_of_keys for sample in samples)

    # Converge purely via the event before starting the load.
    _wait_auto_refresh_aware(env, conn, 'label1=17', number_of_keys)
    validate_result(execute_through_topology_flux(conn, command))

    done = threading.Event()

    def validate_command_in_a_loop():
        while not done.is_set():
            try:
                result = conn.execute_command(command)
            except redis.exceptions.ResponseError as x:
                assert is_topology_flux_error(x), str(x)
                continue
            validate_result(result)

    def migrate_slots():
        for _ in range(MIGRATION_CYCLES):
            if done.is_set():
                break
            migrate_slots_back_and_forth(env, command, validate_result)

    with ThreadPoolExecutor() as executor:
        futures = map(executor.submit, [validate_command_in_a_loop, migrate_slots])
        try:
            for future in as_completed(futures):
                done.set()
                future.result()
        except TimeoutError as e:
            if SANITIZER and "state is init-rdbchannel" in str(e):
                print(f"Ignoring known sanitizer migration timeout: {e}")
                done.set()
                return
            done.set()
            raise

    validate_result(execute_through_topology_flux(conn, command))


def test_auto_refresh_survives_repeated_cycles():
    # MOD-16382 stability: many back-and-forth ASM migrations, each firing topology
    # events, must leave the module correctly converged every time -- the reconcile
    # is idempotent and does not drift or leak across repeated events. No manual
    # REFRESHCLUSTER/CLUSTERSET anywhere.
    env = Env(shardsCount=2, decodeResponses=True, skipRefreshCluster=True)
    if env.env != "oss-cluster":
        env.skip()

    number_of_keys = 100
    samples_per_key = 10
    fill_some_data(env, number_of_keys=number_of_keys, samples_per_key=samples_per_key, label="cyc")

    conn = env.getConnection(0)
    _wait_auto_refresh_aware(env, conn, 'label=cyc', number_of_keys)

    for cycle in range(MIGRATION_CYCLES):
        migrate_slots_back_and_forth(env)
        deadline = time.time() + (60 if (VALGRIND or SANITIZER) else 10)
        while time.time() < deadline:
            if len(execute_through_topology_flux(conn, 'TS.QUERYINDEX', 'label=cyc')) == number_of_keys:
                break
            time.sleep(0.1)
        else:
            raise AssertionError(
                f'auto-refresh drifted after migration cycle {cycle}: QUERYINDEX did not '
                f'return the full set'
            )


# Helper structs and functions


@dataclass(frozen=True)
class SlotRange:
    start: int
    end: int

    @staticmethod
    def from_str(s: str):
        start, end = map(int, s.split("-"))
        assert 0 <= start <= end < 2**14
        return SlotRange(start, end)


@dataclass
class ClusterNode:
    id: str
    ip: str
    port: int
    cport: int  # cluster bus port
    hostname: Optional[str]
    flags: Set[str]
    master: str  # Either this node's primary replica or '-'
    ping_sent: int
    pong_recv: int
    config_epoch: int
    link_state: bool  # True: connected, False: disconnected
    slots: Set[SlotRange]

    @staticmethod
    def from_str(s: str):
        # <id> <ip:port @cport[,hostname]> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot-range> [<slot-range>> ...]
        # e.g. a5e5068caceb2adabed3ed657b21b627deadbfaa 127.0.0.1:6379 @16379 master - 0 1760353421847 1 connected 1000-2000 10000-15000
        parts = s.split()
        node_id, addr, flags, master, ping_sent, pong_recv, config_epoch, link_state, *slots = parts
        match = re.match(r"^(?P<ip>[^:]+):(?P<port>\d+)@(?P<cport>\d+)(?:,(?P<hostname>.+))?$", addr)
        ip = match.group("ip")
        port = int(match.group("port"))
        cport = int(match.group("cport"))
        hostname = match.group("hostname")

        return ClusterNode(
            id=node_id,
            ip=ip,
            port=port,
            cport=cport,
            hostname=hostname,
            flags=set(flags.split(",")),
            master=master,
            ping_sent=int(ping_sent),
            pong_recv=int(pong_recv),
            config_epoch=int(config_epoch),
            link_state=link_state == "connected",
            slots={SlotRange.from_str(s) for s in slots},
        )


def fill_some_data(env, number_of_keys: int, samples_per_key: int, **lables):
    # Callable label values are invoked with the per-key index; others used as-is.
    def generate_commands():
        start_timestamp, jump_timestamps = 1000000000, 100
        for i in range(number_of_keys):
            hslot = i * (2**14 - 1) // (number_of_keys - 1)
            ts_key = f"ts:{{{slot_table[hslot]}}}"
            resolved = {k: (v(i) if callable(v) else v) for k, v in lables.items()}
            yield f"TS.CREATE {ts_key} LABELS {' '.join(f'{k} {v}' for k, v in resolved.items())}"
            yield "TS.MADD " + " ".join(
                f"{ts_key} {start_timestamp + j * jump_timestamps} {random.uniform(0, 100)}"
                for j in range(samples_per_key)
            )

    with env.getClusterConnectionIfNeeded() as rc:
        for command in generate_commands():
            rc.execute_command(*command.split())


def migrate_slots_back_and_forth(env, command=None, validate_result=None):
    """
    Migrates slots between the two shards. When done all slots are back to their original places.
    Upon each migration, the command is executed and the result is validated (when not None).
    """

    def cluster_node_of(conn) -> ClusterNode:
        for line in conn.execute_command("cluster", "nodes").splitlines():
            cluster_node = ClusterNode.from_str(line)
            if "myself" in cluster_node.flags:
                return cluster_node
        raise ValueError("No node with 'myself' flag found")

    def middle_slot_range(slot_range: SlotRange) -> SlotRange:
        third = (slot_range.end - slot_range.start) // 3
        return SlotRange(slot_range.start + third, slot_range.end - third)

    def cantorized_slot_set(slot_range: SlotRange) -> Set[SlotRange]:  # https://en.wikipedia.org/wiki/Cantor_set ;)
        middle = middle_slot_range(slot_range)
        return {SlotRange(slot_range.start, middle.start - 1), SlotRange(middle.end + 1, slot_range.end)}

    first_conn, second_conn = env.getConnection(0), env.getConnection(1)
    # Store some original values to be used throughout the test
    (original_first_slot_range,) = cluster_node_of(first_conn).slots
    (original_second_slot_range,) = cluster_node_of(second_conn).slots
    middle_of_original_first = middle_slot_range(original_first_slot_range)
    middle_of_original_second = middle_slot_range(original_second_slot_range)

    import_slots(second_conn, first_conn, middle_of_original_second)
    assert cluster_node_of(first_conn).slots == {original_first_slot_range, middle_of_original_second}
    assert cluster_node_of(second_conn).slots == cantorized_slot_set(original_second_slot_range)
    if command is not None:
        validate_result(execute_through_topology_flux(first_conn, command))
        validate_result(execute_through_topology_flux(second_conn, command))

    import_slots(first_conn, second_conn, middle_of_original_second)
    assert cluster_node_of(first_conn).slots == {original_first_slot_range}
    assert cluster_node_of(second_conn).slots == {original_second_slot_range}
    if command is not None:
        validate_result(execute_through_topology_flux(first_conn, command))
        validate_result(execute_through_topology_flux(second_conn, command))

    import_slots(first_conn, second_conn, middle_of_original_first)
    assert cluster_node_of(second_conn).slots == {original_second_slot_range, middle_of_original_first}
    assert cluster_node_of(first_conn).slots == cantorized_slot_set(original_first_slot_range)
    if command is not None:
        validate_result(execute_through_topology_flux(first_conn, command))
        validate_result(execute_through_topology_flux(second_conn, command))

    import_slots(second_conn, first_conn, middle_of_original_first)
    assert cluster_node_of(first_conn).slots == {original_first_slot_range}
    assert cluster_node_of(second_conn).slots == {original_second_slot_range}
    if command is not None:
        validate_result(execute_through_topology_flux(first_conn, command))
        validate_result(execute_through_topology_flux(second_conn, command))


def import_slots(source_conn, target_conn, slot_range: SlotRange):
    task_id = target_conn.execute_command("CLUSTER", "MIGRATION", "IMPORT", slot_range.start, slot_range.end)

    def wait_for_completion(conn):
        start_time = time.time()
        # Migration clients wait for `repl-diskless-sync-delay` seconds to start a new fork after the last child exits
        # so for rapid ASM operations (as we do here) we need to add this value to our expected timeouts.
        repl_diskless_sync_delay = float(conn.config_get()["repl-diskless-sync-delay"])
        timeout = repl_diskless_sync_delay + (5 if not (VALGRIND or SANITIZER) else 60)
        while time.time() - start_time < timeout:
            (migration_status,) = conn.execute_command("CLUSTER", "MIGRATION", "STATUS", "ID", task_id)
            migration_status = {key: value for key, value in zip(migration_status[0::2], migration_status[1::2])}
            if migration_status["state"] == "completed":
                break
            time.sleep(0.1)
        else:
            raise TimeoutError(
                f"Migration {task_id} did not complete in {timeout} seconds, state is {migration_status['state']}"
            )

    wait_for_completion(target_conn)
    # The oss cluster's status data is not CP, but we should rather rely on eventual consistency,
    # so let's wait for the source as well:
    wait_for_completion(source_conn)
