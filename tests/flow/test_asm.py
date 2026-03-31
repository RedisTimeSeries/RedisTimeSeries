import time
import random
import sys
import os
import traceback
from dataclasses import dataclass
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Set
import redis

from includes import Env, VALGRIND, SANITIZER
from utils import slot_table


MIGRATION_CYCLES = 10


def _dump_diagnostics(env, context="unknown"):
    """Dump comprehensive diagnostics from all shards to help debug CI failures."""
    print(f"\n{'='*80}", file=sys.stderr)
    print(f"DIAGNOSTICS DUMP — context: {context}", file=sys.stderr)
    print(f"{'='*80}", file=sys.stderr)
    for shard in range(env.shardsCount):
        conn = env.getConnection(shard)
        port = conn.connection_pool.connection_kwargs.get('port', '?')
        print(f"\n--- Shard {shard} (port {port}) ---", file=sys.stderr)
        try:
            print(f"CLUSTER NODES:\n{conn.execute_command('CLUSTER', 'NODES')}", file=sys.stderr)
        except Exception as e:
            print(f"  CLUSTER NODES failed: {e}", file=sys.stderr)
        try:
            statuses = conn.execute_command("CLUSTER", "MIGRATION", "STATUS")
            print(f"CLUSTER MIGRATION STATUS: {statuses}", file=sys.stderr)
        except Exception as e:
            print(f"  CLUSTER MIGRATION STATUS failed: {e}", file=sys.stderr)
        try:
            info = conn.execute_command("INFO", "server")
            for key in ("redis_version", "process_id", "uptime_in_seconds",
                        "tcp_port", "executable"):
                if key in info:
                    print(f"  {key}: {info[key]}", file=sys.stderr)
        except Exception as e:
            print(f"  INFO server failed: {e}", file=sys.stderr)
        try:
            info = conn.execute_command("INFO", "replication")
            for key in ("role", "connected_slaves", "master_failover_state",
                        "rdb_bgsave_in_progress", "rdb_last_bgsave_status"):
                if key in info:
                    print(f"  {key}: {info[key]}", file=sys.stderr)
        except Exception as e:
            print(f"  INFO replication failed: {e}", file=sys.stderr)
        try:
            info = conn.execute_command("INFO", "persistence")
            for key in ("rdb_bgsave_in_progress", "rdb_last_bgsave_status",
                        "rdb_last_bgsave_time_sec", "aof_rewrite_in_progress",
                        "rdb_last_cow_size", "current_fork_perc"):
                if key in info:
                    print(f"  {key}: {info[key]}", file=sys.stderr)
        except Exception as e:
            print(f"  INFO persistence failed: {e}", file=sys.stderr)
        try:
            clients = conn.execute_command("CLIENT", "LIST")
            print(f"CLIENT LIST:\n{clients}", file=sys.stderr)
        except Exception as e:
            print(f"  CLIENT LIST failed: {e}", file=sys.stderr)
    print(f"{'='*80}\n", file=sys.stderr)


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
    env = Env(shardsCount=2, decodeResponses=True)
    if env.env != "oss-cluster":
        env.skip()

    if VALGRIND or SANITIZER:
        for shard in range(env.shardsCount):
            c = env.getConnection(shard)
            c.config_set("repl-timeout", 3600)

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
    try:
        validate_result(conn.execute_command(command))
    except Exception:
        _dump_diagnostics(env, "initial static validation FAILED")
        raise

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
                if error_message != SLOT_RANGES_ERROR:
                    _dump_diagnostics(env, f"validation loop unexpected error: {error_message}")
                assert error_message == SLOT_RANGES_ERROR, error_message
                continue
            validate_result(result)

    def migrate_slots():
        for cycle in range(MIGRATION_CYCLES):
            if done.is_set():
                break
            migrate_slots_back_and_forth(env, command, validate_result, cycle=cycle)

    with ThreadPoolExecutor() as executor:
        futures = map(executor.submit, [validate_command_in_a_loop, migrate_slots])
        for future in as_completed(futures):
            # On a healthy run slot migrations should complete cleanly and we then signal the validator loop to exit
            done.set()
            # This will raise an exception in case the validation function failed (or got stuck)
            future.result()

    # Validate that all is fine after the migrations
    try:
        validate_result(conn.execute_command(command))
    except Exception:
        _dump_diagnostics(env, "post-migration validation FAILED")
        raise


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
    def generate_commands():
        start_timestamp, jump_timestamps = 1000000000, 100
        for i in range(number_of_keys):
            hslot = i * (2**14 - 1) // (number_of_keys - 1)
            ts_key = f"ts:{{{slot_table[hslot]}}}"
            yield f"TS.CREATE {ts_key} LABELS {' '.join(f'{k} {v}' for k, v in lables.items())}"
            yield "TS.MADD " + " ".join(
                f"{ts_key} {start_timestamp + j * jump_timestamps} {random.uniform(0, 100)}"
                for j in range(samples_per_key)
            )

    with env.getClusterConnectionIfNeeded() as rc:
        for command in generate_commands():
            rc.execute_command(*command.split())


def migrate_slots_back_and_forth(env, command=None, validate_result=None, cycle=None):
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

    def _safe_validate(conn, step_label):
        try:
            validate_result(conn.execute_command(command))
        except Exception:
            _dump_diagnostics(env, f"post-import validation FAILED at {step_label} (cycle={cycle})")
            raise

    first_conn, second_conn = env.getConnection(0), env.getConnection(1)
    # Store some original values to be used throughout the test
    (original_first_slot_range,) = cluster_node_of(first_conn).slots
    (original_second_slot_range,) = cluster_node_of(second_conn).slots
    middle_of_original_first = middle_slot_range(original_first_slot_range)
    middle_of_original_second = middle_slot_range(original_second_slot_range)

    import_slots(env, second_conn, first_conn, middle_of_original_second, label=f"cycle={cycle} step=1")
    assert cluster_node_of(first_conn).slots == {original_first_slot_range, middle_of_original_second}
    assert cluster_node_of(second_conn).slots == cantorized_slot_set(original_second_slot_range)
    if command is not None:
        _safe_validate(first_conn, "step1-first")
        _safe_validate(second_conn, "step1-second")

    import_slots(env, first_conn, second_conn, middle_of_original_second, label=f"cycle={cycle} step=2")
    assert cluster_node_of(first_conn).slots == {original_first_slot_range}
    assert cluster_node_of(second_conn).slots == {original_second_slot_range}
    if command is not None:
        _safe_validate(first_conn, "step2-first")
        _safe_validate(second_conn, "step2-second")

    import_slots(env, first_conn, second_conn, middle_of_original_first, label=f"cycle={cycle} step=3")
    assert cluster_node_of(second_conn).slots == {original_second_slot_range, middle_of_original_first}
    assert cluster_node_of(first_conn).slots == cantorized_slot_set(original_first_slot_range)
    if command is not None:
        _safe_validate(first_conn, "step3-first")
        _safe_validate(second_conn, "step3-second")

    import_slots(env, second_conn, first_conn, middle_of_original_first, label=f"cycle={cycle} step=4")
    assert cluster_node_of(first_conn).slots == {original_first_slot_range}
    assert cluster_node_of(second_conn).slots == {original_second_slot_range}
    if command is not None:
        _safe_validate(first_conn, "step4-first")
        _safe_validate(second_conn, "step4-second")


def _wait_for_bgsave(conn, timeout, label=""):
    """Wait for any ongoing RDB background save to finish.
    Redis only allows one child fork at a time, so a new migration can't
    start its RDB channel while a previous fork is still running."""
    start = time.time()
    while time.time() - start < timeout:
        info = conn.execute_command("INFO", "persistence")
        if not info.get("rdb_bgsave_in_progress", 0):
            return
        time.sleep(0.1)
    raise TimeoutError(f"[{label}] rdb_bgsave_in_progress still 1 after {timeout}s")


def import_slots(env, source_conn, target_conn, slot_range: SlotRange, label=""):
    bgsave_timeout = 10 if not (VALGRIND or SANITIZER) else 120
    _wait_for_bgsave(source_conn, bgsave_timeout, f"{label} source pre-import")
    _wait_for_bgsave(target_conn, bgsave_timeout, f"{label} target pre-import")
    task_id = target_conn.execute_command("CLUSTER", "MIGRATION", "IMPORT", slot_range.start, slot_range.end)

    def wait_for_completion(conn, role):
        start_time = time.time()
        # Migration clients wait for `repl-diskless-sync-delay` seconds to start a new fork after the last child exits
        # so for rapid ASM operations (as we do here) we need to add this value to our expected timeouts.
        repl_diskless_sync_delay = float(conn.config_get()["repl-diskless-sync-delay"])
        timeout = repl_diskless_sync_delay + (5 if not (VALGRIND or SANITIZER) else 360)
        last_state = None
        while time.time() - start_time < timeout:
            (migration_status,) = conn.execute_command("CLUSTER", "MIGRATION", "STATUS", "ID", task_id)
            migration_status = {key: value for key, value in zip(migration_status[0::2], migration_status[1::2])}
            if migration_status["state"] == "completed":
                break
            if migration_status["state"] != last_state:
                elapsed = time.time() - start_time
                print(f"  [{label}] {role} migration {task_id}: "
                      f"state={migration_status['state']} (after {elapsed:.1f}s), "
                      f"full_status={migration_status}",
                      file=sys.stderr)
                last_state = migration_status["state"]
            time.sleep(0.1)
        else:
            _dump_diagnostics(env, f"migration TIMEOUT [{label}] {role}: "
                              f"task={task_id} state={migration_status['state']} "
                              f"slots={slot_range.start}-{slot_range.end}")
            raise TimeoutError(
                f"Migration {task_id} did not complete in {timeout} seconds, state is {migration_status['state']}"
            )

    wait_for_completion(target_conn, "target")
    # The oss cluster's status data is not CP, but we should rather rely on eventual consistency,
    # so let's wait for the source as well:
    wait_for_completion(source_conn, "source")
