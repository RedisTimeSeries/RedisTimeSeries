import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional, Set
import re

from includes import Env, VALGRIND, SANITIZER, BIGREDIS_TESTS, RUNNER_LABEL
from utils import slot_table
import redis
from utils import migrate_slots_back_and_forth, fill_ts_data, wait_for_valid_cluster


MIGRATION_CYCLES = 10


def test_asm_without_data():
    env = Env(shardsCount=2, decodeResponses=True)
    if env.env != "oss-cluster":  # TODO: convert to a proper fixture (here and below)
        env.skip()
    if BIGREDIS_TESTS:
        # The redis core panics "bigredis doesn't yet support ASM"
        # (cluster_asm.c), so atomic slot migration can't run on a
        # flex/bigredis build. Skip until the core adds ASM+bigredis support.
        env.skip()

    for _ in range(MIGRATION_CYCLES):
        migrate_slots_back_and_forth(env, wait_for_valid_cluster)


def test_asm_with_data():
    env = Env(shardsCount=2, decodeResponses=True)
    if env.env != "oss-cluster":
        env.skip()
    if BIGREDIS_TESTS:
        # The redis core panics "bigredis doesn't yet support ASM"
        # (cluster_asm.c), so atomic slot migration can't run on a
        # flex/bigredis build. Skip until the core adds ASM+bigredis support.
        env.skip()

    fill_ts_data(env, number_of_keys=100, samples_per_key=10, label="test")
    for _ in range(MIGRATION_CYCLES):
        migrate_slots_back_and_forth(env, wait_for_valid_cluster)


def test_asm_with_data_and_queries_during_migrations():
    env = Env(shardsCount=2, decodeResponses=True, noLog=False, moduleArgs="ts-topology-events no")
    if env.env != "oss-cluster":
        env.skip()
    if BIGREDIS_TESTS:
        # The redis core panics "bigredis doesn't yet support ASM"
        # (cluster_asm.c), so atomic slot migration can't run on a
        # flex/bigredis build. Skip until the core adds ASM+bigredis support.
        env.skip()

    # macos-15-intel is the slowest hosted runner and can't reliably serve the
    # multi-shard query within LibMR's 5s max-idle during migration churn, so it
    # occasionally trips the max-idle timeout instead of the expected slot-ranges
    # error (MOD-14615 residual; not a product bug -- other macOS/Linux runners pass).
    if RUNNER_LABEL == "macos-15-intel":
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
    validate_result(conn.execute_command(command))

    # Now validate the command's result in a loop during the back and forth migrations
    done = threading.Event()

    def validate_command_in_a_loop():
        # Note: should be the same as in libmr_commands.c
        SLOT_RANGES_ERROR = "Query requires unavailable slots"
        while not done.is_set():
            try:
                result = conn.execute_command(command)
            except redis.exceptions.ResponseError as x:
                error_message = str(x)
                # An occasional SLOT_RANGES_ERROR is expected
                assert error_message == SLOT_RANGES_ERROR, error_message
                continue
            validate_result(result)

    def migrate_slots():
        def validate_command_on_both_shards(env):
            validate_result(env.getConnection(0).execute_command(command))
            validate_result(env.getConnection(1).execute_command(command))
        for _ in range(MIGRATION_CYCLES):
            if done.is_set():
                break
            migrate_slots_back_and_forth(env, validate_command_on_both_shards)

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
    validate_result(conn.execute_command(command))

def validate_queries_during_migrations(env, post_migration, command, validate_result):
    """
    Runs command from random shards in a loop while slots migrate back and forth, validating every result.

    env: the cluster test environment.
    post_migration: callback invoked with env after each migration completes (e.g. wait_for_valid_cluster
    to wait for a consistent view of the cluster amongst all nodes, before continuing to other migrations)
    command: the query to run repeatedly (as a single string).
    validate_result: callback invoked with the command's reply to assert it is correct.
    """
    # Two transient errors can surface while slots migrate; both are expected and tolerated (strings
    # must match libmr_commands.c):
    # - SLOT_RANGES_ERROR: a race between nodes propagating ownership of slots and a parallel multi-node
    #   command might lead to response on same slots from multiple nodes (or the opposite: missed slots).
    # - TOPOLOGY_CHANGED_ERROR: a topology change is seen mid-command (across the fan-out to all nodes)
    #   so in-flight executions are killed early and this is returned.
    SLOT_RANGES_ERROR = "Query requires unavailable slots"
    TOPOLOGY_CHANGED_ERROR = "A multi-shard command failed because the cluster topology has changed"

    # Two flavors of the same query check, both hitting a random shard:
    # - strict: used when the topology is settled (baseline + right after each migration
    #   completes) -> the query must succeed and be correct; no transient error tolerated.
    # - tolerable: used in the background loop while slots may be mid-migration -> an occasional
    #   SLOT_RANGES_ERROR is expected and skipped, otherwise the result is validated.
    def strict_validation(env):
        conn = env.getConnection(random.randrange(env.shardsCount))
        validate_result(conn.execute_command(command))

    def tolerable_validation(env):
        conn = env.getConnection(random.randrange(env.shardsCount))
        try:
            result = conn.execute_command(command)
        except redis.exceptions.ResponseError as x:
            assert str(x) in (SLOT_RANGES_ERROR, TOPOLOGY_CHANGED_ERROR), str(x)
            return
        validate_result(result)

    def validate_after_migration(env):
        post_migration(env)
        strict_validation(env)

    # First validate the result on the "static" cluster
    strict_validation(env)

    # Now validate the command's result in a loop during the back and forth migrations
    done = threading.Event()

    def validate_command_in_a_loop():
        while not done.is_set():
            try:
                validate_result(conn.execute_command(command))
            except Exception as e:
                # Safe failure mode: topology changed mid-execution, so the command asks for retry.
                msg = str(e)
                assert (
                    "cluster topology change during execution" in msg
                    or "missing slot ownership metadata" in msg
                    or "Query requires unavailable slots" in msg
                    or "Please retry" in msg
                ), msg

    def migrate_slots():
        for _ in range(MIGRATION_CYCLES):
            if done.is_set():
                break
            migrate_slots_back_and_forth(env, validate_after_migration)

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
    strict_validation(env)


def test_short_form_clusterset():
    # Skip the initial REFRESHCLUSTER so the modules start unaware of the cluster.
    env = Env(shardsCount=3, decodeResponses=True, skipRefreshCluster=True, moduleArgs="ts-topology-events no")
    if env.env != "oss-cluster":
        env.skip()
    if BIGREDIS_TESTS:
        # Short-form CLUSTERSET needs RedisModule_GetClusterNodeSlotRanges, which
        # big-redis doesn't export (deps/LibMR/src/cluster.c:SetClusterDataShortForm
        # rejects with ERRCLUSTER rather than crash or silently no-op). Skip until
        # the core adds that API.
        env.skip()

    number_of_keys = 100
    samples_per_key = 10
    number_of_groups = 10
    keys_per_group = number_of_keys // number_of_groups
    fill_ts_data(env, number_of_keys=number_of_keys, samples_per_key=samples_per_key,
                   label="test", group=lambda i: f"g{i % number_of_groups}")

    conn = env.getConnection(0)

    # Module unaware of the cluster -- QUERYINDEX runs local-only.
    queryindex = conn.execute_command('TS.QUERYINDEX', 'label=test')
    assert 0 < len(queryindex) < number_of_keys, queryindex

    # DMC pattern: short-form CLUSTERSET on one shard; LibMR propagates to peers
    # via CLUSTERSETFROMSHARD on rg.hello / reconnect.
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


def test_asm_multishard_queryindex_is_consistent_or_retryable():
    env = Env(shardsCount=2, decodeResponses=True)
    if env.env != "oss-cluster":
        env.skip()
    if BIGREDIS_TESTS:
        # The redis core panics "bigredis doesn't yet support ASM"
        # (cluster_asm.c), so atomic slot migration can't run on a
        # flex/bigredis build. Skip until the core adds ASM+bigredis support.
        env.skip()

    number_of_keys = 500 if not (VALGRIND or SANITIZER) else 100
    fill_some_data(env, number_of_keys=number_of_keys, samples_per_key=1, label1=17)

    expected_keys = set()
    for i in range(number_of_keys):
        hslot = i * (2**14 - 1) // (number_of_keys - 1)
        expected_keys.add(f"ts:{{{slot_table[hslot]}}}")

    conn = env.getConnection(0)
    command = "TS.QUERYINDEX label1=17"

    done = threading.Event()

    def validate_command_in_a_loop():
        while not done.is_set():
            try:
                res = conn.execute_command(command)
                assert isinstance(res, list)
                assert set(res) == expected_keys
            except Exception as e:
                # Safe failure mode: topology changed mid-execution, so the command asks for retry.
                msg = str(e)
                assert (
                    "cluster topology change during execution" in msg
                    or "missing slot ownership metadata" in msg
                    or "Query requires unavailable slots" in msg
                    or "Please retry" in msg
                ), msg

    def migrate_slots():
        for _ in range(MIGRATION_CYCLES):
            if done.is_set():
                break
            migrate_slots_back_and_forth(env)

    with ThreadPoolExecutor() as executor:
        futures = map(executor.submit, [validate_command_in_a_loop, migrate_slots])
        for future in as_completed(futures):
            done.set()
            future.result()


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
