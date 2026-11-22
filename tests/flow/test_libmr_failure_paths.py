"""Failure paths of distributed queries (src/libmr_commands.c).

A distributed query fails when a shard stops replying, when the coordinator's
cluster topology changes while the query runs, or when no node owns some slot.
Each case checks the error, that no client stays blocked, and that the same
queries succeed once the cluster is healthy again.

The classes always start a 3-shard cluster, so they also run in the general
pass that code coverage measures. Every case leaves the cluster healthy, so one
class (one cluster start) per LibMR transport runs them all.
"""
import os
import time
from concurrent.futures import ThreadPoolExecutor

from includes import *
from utils import slot_table, wait_for_valid_cluster, get_timeout

TIMEOUT_ERROR = ('A multi-keys command failed because at least one shard '
                 'did not reply within the given timeframe.')
TOPOLOGY_ERROR = 'A multi-shard command failed because the cluster topology has changed'
SLOTS_ERROR = 'Query requires unavailable slots'

# LibMR fails an execution after 5 s without a reply (EXECUTION_DEFAULT_MAX_IDLE_MS).
MAX_IDLE_SECS = 5

# One query per distributed command. Each one has its own done handler.
QUERIES = [
    ('TS.MRANGE', '-', '+', 'FILTER', 'name=x'),
    ('TS.MGET', 'FILTER', 'name=x'),
    ('TS.QUERYINDEX', 'name=x'),
    ('TS.QUERYLABELS', 'LABELS', 'FILTER', 'name=x'),
]

# One series on each shard (RLTest gives the 3 shards 0-5460, 5461-10922 and 10923-16383).
KEYS = [f'x:{{{slot_table[slot]}}}' for slot in (0, 8192, 16383)]
# A slot of shard 1 that holds no series, so it can change owner without moving data.
FREE_SLOT = 1


class _LibmrFailures:
    TRANSPORT = None

    def __init__(self):
        skip_on_rlec()  # needs DEBUG SLEEP and CLUSTER SETSLOT on real shards
        self.env = Env(env='oss-cluster', shardsCount=3, useSlaves=False, useAof=False,
                       decodeResponses=True,
                       # A shard in DEBUG SLEEP must not be marked as failed.
                       clusterNodeTimeout=60000,
                       redisConfigFileContent=f'ts-libmr-protocol {self.TRANSPORT}\n',
                       clusterBusPortProtectedMode=os.getenv('CLUSTER_BUS_PROTECTED_MODE', 'no') or None)
        for shard in range(1, 4):  # a config mistake must not silently test the default
            assert self.env.getConnection(shard).execute_command(
                'CONFIG', 'GET', 'ts-libmr-protocol') == ['ts-libmr-protocol', self.TRANSPORT.upper()]
        with self.env.getClusterConnectionIfNeeded() as conn:
            for i, key in enumerate(KEYS):
                conn.execute_command('TS.ADD', key, 1, i, 'LABELS', 'name', 'x')
        self._wait_until_queries_succeed()

    def _reply_of(self, query, shard=1):
        try:
            return self.env.getConnection(shard).execute_command(*query)
        except redis.exceptions.ResponseError as e:
            return str(e)

    def _assert_queries_succeed(self, shard):
        conn = self.env.getConnection(shard)
        mrange, mget, queryindex, querylabels = [conn.execute_command(*q) for q in QUERIES]
        assert sorted(mrange) == sorted([key, [], [[1, str(i)]]] for i, key in enumerate(KEYS)), mrange
        assert sorted(mget) == sorted([key, [], [1, str(i)]] for i, key in enumerate(KEYS)), mget
        assert sorted(queryindex) == sorted(KEYS), queryindex
        assert querylabels == ['name'], querylabels

    def _wait_until_queries_succeed(self):
        # After a topology change LibMR reconnects to the shards in the background.
        deadline = time.monotonic() + get_timeout()
        for shard in range(1, 4):
            while True:
                try:
                    self._assert_queries_succeed(shard)
                    break
                except redis.exceptions.ResponseError:
                    assert time.monotonic() < deadline, f'queries on shard {shard} kept failing'
                    time.sleep(0.2)

    def _assert_no_blocked_clients(self):
        for shard in range(1, 4):
            self.env.assertEqual(self.env.getConnection(shard).info('clients')['blocked_clients'], 0)

    def _run_while_shard_2_sleeps(self, seconds, action=None):
        """Send QUERIES to shard 1 while shard 2 sleeps, then call action. Return the replies."""
        with ThreadPoolExecutor(len(QUERIES) + 1) as pool:
            sleeper = pool.submit(self.env.getConnection(2).execute_command, 'DEBUG', 'SLEEP', seconds)
            time.sleep(0.5)  # let DEBUG SLEEP start
            started = time.monotonic()
            replies = [pool.submit(self._reply_of, q) for q in QUERIES]
            if action:
                time.sleep(0.5)  # let the queries reach the shards
                action()
            replies = [r.result() for r in replies]
            elapsed = time.monotonic() - started
            sleeper.result()
        return replies, elapsed

    def _move_slot(self, slot, src, dst):
        """Move an empty slot to another shard, as redis-cli --cluster reshard does."""
        src, dst = self.env.getConnection(src), self.env.getConnection(dst)
        src_id, dst_id = src.execute_command('CLUSTER', 'MYID'), dst.execute_command('CLUSTER', 'MYID')
        dst.execute_command('CLUSTER', 'SETSLOT', slot, 'IMPORTING', src_id)
        src.execute_command('CLUSTER', 'SETSLOT', slot, 'MIGRATING', dst_id)
        dst.execute_command('CLUSTER', 'SETSLOT', slot, 'NODE', dst_id)
        src.execute_command('CLUSTER', 'SETSLOT', slot, 'NODE', dst_id)

    def test_shard_timeout(self):
        # Shard 2 sleeps longer than LibMR waits: every query gives up on it.
        sleep_secs = MAX_IDLE_SECS + 3
        replies, elapsed = self._run_while_shard_2_sleeps(sleep_secs)
        self.env.assertEqual(replies, [TIMEOUT_ERROR] * len(QUERIES))
        # The queries failed at the max idle time, not when shard 2 woke up.
        self.env.assertLess(elapsed, sleep_secs - 0.5)
        self._assert_no_blocked_clients()
        self._wait_until_queries_succeed()

    def test_topology_change(self):
        # Shard 1 gives a slot away while its queries wait for shard 2.
        # Its topology changes, which aborts them.
        replies, _ = self._run_while_shard_2_sleeps(2, lambda: self._move_slot(FREE_SLOT, 1, 3))
        self.env.assertEqual(replies, [TOPOLOGY_ERROR] * len(QUERIES))
        self._assert_no_blocked_clients()
        self._move_slot(FREE_SLOT, 3, 1)
        wait_for_valid_cluster(self.env)
        self._wait_until_queries_succeed()


class testLibmrFailuresInternal(_LibmrFailures):
    TRANSPORT = 'internal'

    def test_slot_loss(self):
        # Only INTERNAL collects the shards' slot ranges and checks them. GEARS does not.
        shards = [self.env.getConnection(shard) for shard in range(1, 4)]
        for conn in shards:
            conn.execute_command('CONFIG', 'SET', 'cluster-require-full-coverage', 'no')
        try:
            # Now no node owns the slot. Every coordinator refuses to answer from
            # the remaining slots.
            shards[0].execute_command('CLUSTER', 'DELSLOTS', FREE_SLOT)
            for shard in range(1, 4):
                self.env.assertEqual([self._reply_of(q, shard) for q in QUERIES],
                                     [SLOTS_ERROR] * len(QUERIES), message=f'shard {shard}')
            self._assert_no_blocked_clients()
        finally:
            shards[0].execute_command('CLUSTER', 'ADDSLOTS', FREE_SLOT)
            for conn in shards:
                conn.execute_command('CONFIG', 'SET', 'cluster-require-full-coverage', 'yes')
        wait_for_valid_cluster(self.env)
        self._wait_until_queries_succeed()


class testLibmrFailuresGears(_LibmrFailures):
    TRANSPORT = 'gears'
