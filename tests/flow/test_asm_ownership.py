import os
import time
import redis

from includes import *
from utils import ClusterNode, SlotRange, fill_ts_data, get_timeout, import_slots, wait_for_valid_cluster

NUM_KEYS = 200
SAMPLES = 2

# Seen right after a slot ownership change, until every node agrees on it (see test_asm.py).
TRANSIENT_ERRORS = ("Query requires unavailable slots",
                    "A multi-shard command failed because the cluster topology has changed")


def own_slots(conn):
    for line in conn.execute_command("CLUSTER", "NODES").splitlines():
        node = ClusterNode.from_str(line)
        if "myself" in node.flags:
            return node.slots


def task_status(conn, task_id):
    (fields,) = conn.execute_command("CLUSTER", "MIGRATION", "STATUS", "ID", task_id)
    return dict(zip(fields[0::2], fields[1::2]))


def active_trim(conn):
    info = conn.execute_command("CLUSTER", "INFO")
    info = info if isinstance(info, dict) else dict(line.split(":", 1) for line in info.splitlines() if ":" in line)
    return {field: int(info[f"cluster_slot_migration_active_trim_{field}"])
            for field in ("running", "current_job_keys", "current_job_trimmed")}


def wait_for(predicate, what):
    deadline = time.time() + get_timeout()
    while not predicate():
        assert time.time() < deadline, f"timed out waiting for {what}"
        time.sleep(0.05)


class testAsmOwnership:
    """ASM callbacks in src/module.c and the label-index ownership filter in src/indexer.c.

    Forces a 2-shard OSS cluster so that the coverage (standalone) pass runs it too.
    Each test holds a migration open with a Redis debug hook, so that one shard's
    label index holds series of slots it doesn't own (imported but not taken over,
    or migrated but not trimmed yet). Queries must still report every series once.
    Every test leaves slot ownership as it found it, so all tests share one env.
    """

    def __init__(self):
        skip_on_rlec()  # before Env: env="oss-cluster" would override RLEC's existing-env
        # tests.sh passes the bus-port waiver only to cluster lanes; this env is a cluster in every lane.
        self.env = Env(env="oss-cluster", shardsCount=2, decodeResponses=True,
                       clusterBusPortProtectedMode=os.getenv("CLUSTER_BUS_PROTECTED_MODE", "no") or None)
        self.target, self.source = self.env.getConnection(0), self.env.getConnection(1)
        try:
            self.target.execute_command("DEBUG", "ASM-FAILPOINT", "", "")
        except redis.exceptions.ResponseError:
            self.env.skip()  # Redis without the ASM debug hooks
        fill_ts_data(self.env, NUM_KEYS, SAMPLES, x=1)
        self.keys = sorted(self.target.execute_command("TS.QUERYINDEX", "x=1"))
        assert len(self.keys) == NUM_KEYS
        # Move the middle third of the source's slots; fill_ts_data put at most one series per slot.
        (source_range,) = own_slots(self.source)
        third = (source_range.end - source_range.start) // 3
        self.moved = SlotRange(source_range.start + third, source_range.end - third)
        slots = [self.target.execute_command("CLUSTER", "KEYSLOT", key) for key in self.keys]
        self.moved_keys = sum(self.moved.start <= slot <= self.moved.end for slot in slots)
        assert self.moved_keys > 0
        self.ownership = (own_slots(self.target), own_slots(self.source))
        # A held import keeps the source's writes paused; don't let it fail the task on slow (valgrind) runs.
        self.source.config_set("cluster-slot-migration-write-pause-timeout", 120000)

    def assert_each_series_once(self):
        for conn in (self.target, self.source):  # each shard coordinates once
            deadline = time.time() + get_timeout()
            while True:
                try:
                    keys = conn.execute_command("TS.QUERYINDEX", "x=1")
                    groups = conn.execute_command("TS.MRANGE", "-", "+", "FILTER", "x=1",
                                                  "GROUPBY", "x", "REDUCE", "count")
                    break
                except redis.exceptions.ResponseError as e:
                    if str(e) not in TRANSIENT_ERRORS or time.time() > deadline:
                        raise
                    time.sleep(0.1)
            self.env.assertEqual(sorted(keys), self.keys)
            ((_, _, samples),) = groups
            self.env.assertEqual([int(float(count)) for _, count in samples], [NUM_KEYS] * SAMPLES)

    def test_import_cancelled_before_takeover(self):
        # The target loads the snapshot but never takes the slots over, so it stays importing.
        task = None
        try:
            self.target.execute_command("DEBUG", "ASM-FAILPOINT", "import-main-channel", "takeover")
            task = self.target.execute_command("CLUSTER", "MIGRATION", "IMPORT", self.moved.start, self.moved.end)
            wait_for(lambda: task_status(self.target, task)["state"] == "wait-stream-eof",
                     "the target to load the snapshot")
            self.env.assertEqual(task_status(self.target, task)["snapshot_processed_keys"], self.moved_keys)
            self.assert_each_series_once()
            self.env.assertEqual(task_status(self.target, task)["state"], "wait-stream-eof")  # queried mid-import
        finally:
            self.target.execute_command("DEBUG", "ASM-FAILPOINT", "", "")
            if task is not None:
                # Fails the import on the target (which then trims the imported keys) and the
                # migration on the source (which resumes writes).
                self.target.execute_command("CLUSTER", "MIGRATION", "CANCEL", "ID", task)
                self.source.execute_command("CLUSTER", "MIGRATION", "CANCEL", "ID", task)
        self.env.assertEqual(task_status(self.target, task)["state"], "canceled")
        self.env.assertEqual(task_status(self.source, task)["state"], "canceled")
        wait_for(lambda: active_trim(self.target)["running"] == 0, "the target to trim the cancelled import")
        self.env.assertEqual((own_slots(self.target), own_slots(self.source)), self.ownership)
        self.assert_each_series_once()

    def test_import_completes_while_source_trim_held(self):
        # Active trim starts (TRIM_STARTED) but deletes nothing while its delay is negative.
        try:
            self.source.execute_command("DEBUG", "ASM-TRIM-METHOD", "active", -1)
            import_slots(self.source, self.target, self.moved)
            wait_for_valid_cluster(self.env)
            self.env.assertEqual(active_trim(self.source),
                                 {"running": 1, "current_job_keys": self.moved_keys, "current_job_trimmed": 0})
            self.assert_each_series_once()
        finally:
            self.source.execute_command("DEBUG", "ASM-TRIM-METHOD", "default", 0)
        wait_for(lambda: active_trim(self.source)["running"] == 0, "the source to trim the migrated slots")
        self.assert_each_series_once()

        import_slots(self.target, self.source, self.moved)  # restore the original ownership
        wait_for_valid_cluster(self.env)
        self.env.assertEqual((own_slots(self.target), own_slots(self.source)), self.ownership)
        self.assert_each_series_once()
