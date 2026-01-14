import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from includes import Env, VALGRIND, SANITIZER


"""
Manual A/B stats test (kept intentionally):

Purpose:
- Quantify mismatch rate vs retryable-error rate for multi-shard aggregation under ASM,
  primarily for comparing two module binaries (e.g., master vs fix branch).

How to run (example):
  source .venv/bin/activate
  RUN_ASM_AB_STATS=1 OSS_CLUSTER=1 SHARDS=2 GEN=0 SLAVES=0 AOF=0 AOF_SLAVES=0 \
    TEST='test_asm_ab_stats.py:test_asm_mrange_groupby_reduce_count_stats' \
    REDIS_SERVER=/Users/tom.gabsow/Projects/redis/src/redis-server \
    MODULE=/path/to/redistimeseries.so \
    ./tests/flow/tests.sh

Notes:
- Gated behind RUN_ASM_AB_STATS=1 so it won't run in CI by accident.
- Prints stats (iterations, retries, mismatches).
"""


RETRYABLE_SUBSTRINGS = (
    "cluster topology change during execution",
    "missing slot ownership metadata",
    "Please retry",
)


@dataclass
class Stats:
    ok: int = 0
    retryable: int = 0
    mismatches: int = 0
    other_errors: int = 0


def _is_retryable_error(exc: Exception) -> bool:
    msg = str(exc)
    return any(s in msg for s in RETRYABLE_SUBSTRINGS)


def _validate_mrange_groupby_count(res, expected_count: int, samples_per_key: int) -> bool:
    """
    Returns True if result is correct, False if mismatch detected.
    """
    # Expected shape:
    # [ [ "label1=17", [], [ [ts, count], ... ] ] ]
    ((filtered_by, withlabels, samples),) = res
    if filtered_by != "label1=17":
        return False
    if withlabels != []:
        return False
    if len(samples) != samples_per_key:
        return False
    for _, count in samples:
        if int(count) != expected_count:
            return False
    return True


def test_asm_mrange_groupby_reduce_count_stats():
    if os.environ.get("RUN_ASM_AB_STATS") != "1":
        # keep out of CI / regular runs
        env = Env()
        env.skip()

    env = Env(shardsCount=2, decodeResponses=True)
    if env.env != "oss-cluster":
        env.skip()

    # Tunables via env vars
    keys = int(os.environ.get("ASM_AB_KEYS", "2000" if not (VALGRIND or SANITIZER) else "200"))
    samples_per_key = int(os.environ.get("ASM_AB_SAMPLES", "150"))
    cycles = int(os.environ.get("ASM_AB_MIGRATION_CYCLES", "30" if not (VALGRIND or SANITIZER) else "5"))

    # Reuse existing helpers from test_asm.py (keeps behavior consistent with earlier experiments)
    from test_asm import fill_some_data, migrate_slots_back_and_forth

    fill_some_data(env, number_of_keys=keys, samples_per_key=samples_per_key, label1=17, label2=19)

    conn = env.getConnection(0)
    command = "TS.MRANGE - + FILTER label1=17 GROUPBY label1 REDUCE count"

    stats = Stats()
    done = threading.Event()

    def validate_loop():
        while not done.is_set():
            try:
                res = conn.execute_command(command)
                if _validate_mrange_groupby_count(res, expected_count=keys, samples_per_key=samples_per_key):
                    stats.ok += 1
                else:
                    stats.mismatches += 1
            except Exception as e:
                if _is_retryable_error(e):
                    stats.retryable += 1
                else:
                    stats.other_errors += 1

    def migrate_loop():
        for _ in range(cycles):
            if done.is_set():
                break
            migrate_slots_back_and_forth(env)

    with ThreadPoolExecutor() as executor:
        futures = map(executor.submit, [validate_loop, migrate_loop])
        for future in as_completed(futures):
            done.set()
            future.result()

    total = stats.ok + stats.retryable + stats.mismatches + stats.other_errors
    print(
        "ASM_AB_STATS "
        f"keys={keys} samples={samples_per_key} cycles={cycles} "
        f"total={total} ok={stats.ok} retryable={stats.retryable} "
        f"mismatches={stats.mismatches} other_errors={stats.other_errors}"
    )

