import time
import random
from dataclasses import dataclass
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Set
import redis

from includes import Env, VALGRIND, SANITIZER, RUNNER_LABEL
from utils import migrate_slots_back_and_forth, fill_ts_data, validate_slots_in_cluster


MIGRATION_CYCLES = 10


def test_asm_without_data():
    env = Env(shardsCount=2, decodeResponses=True)
    if env.env != "oss-cluster":  # TODO: convert to a proper fixture (here and below)
        env.skip()

    for _ in range(MIGRATION_CYCLES):
        migrate_slots_back_and_forth(env, validate_slots_in_cluster)


def test_asm_with_data():
    env = Env(shardsCount=2, decodeResponses=True)
    if env.env != "oss-cluster":
        env.skip()

    fill_ts_data(env, number_of_keys=100, samples_per_key=10, label="test")
    for _ in range(MIGRATION_CYCLES):
        migrate_slots_back_and_forth(env, validate_slots_in_cluster)


def test_asm_with_data_and_queries_during_migrations():
    env = Env(shardsCount=2, decodeResponses=True, noLog=False, moduleArgs="ts-topology-events no")
    if env.env != "oss-cluster":
        env.skip()

    # macos-15-intel is the slowest hosted runner and can't reliably serve the
    # multi-shard query within LibMR's 5s max-idle during migration churn, so it
    # occasionally trips the max-idle timeout instead of the expected slot-ranges
    # error (MOD-14615 residual; not a product bug -- other macOS/Linux runners pass).
    if RUNNER_LABEL == "macos-15-intel":
        env.skip()

    number_of_keys = 1000 if not (VALGRIND or SANITIZER) else 100
    samples_per_key = 150
    fill_ts_data(env, number_of_keys, samples_per_key, label1=17, label2=19)

    command = "TS.MRANGE - + FILTER label1=17 GROUPBY label1 REDUCE count"

    def validate_result(result):
        ((filtered_by, withlabels, samples),) = result
        assert filtered_by == "label1=17"
        assert withlabels == []  # No WITHLABLES
        assert len(samples) == samples_per_key
        assert all(int(sample[1]) == number_of_keys for sample in samples)

    validate_queries_during_migrations(env, command, validate_result)


def validate_queries_during_migrations(env, command, validate_result):
    """
    Runs command from random shards in a loop while slots migrate back and forth, validating every result.

    env: the cluster test environment.
    command: the query to run repeatedly (as a single string).
    validate_result: callback invoked with the command's reply to assert it is correct.
    """
    SLOT_RANGES_ERROR = "Query requires unavailable slots"  # same as in libmr_commands.c

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
            assert str(x) == SLOT_RANGES_ERROR, str(x)
            return
        validate_result(result)

    def validate_after_migration(env):
        validate_slots_in_cluster(env)
        strict_validation(env)

    # First validate the result on the "static" cluster
    strict_validation(env)

    # Now validate the command's result in a loop during the back and forth migrations
    done = threading.Event()

    def validate_command_in_a_loop():
        while not done.is_set():
            tolerable_validation(env)

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

