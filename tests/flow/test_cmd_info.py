from includes import *
from docs_utils import *
import json
import os
import re

class testCommandDocsAndHelp():
    def __init__(self):
        self.env = Env(decodeResponses=True)

    def test_command_info_ts_add(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.ADD')
            assert res
            assert_docs(env, 'TS.ADD', summary='Append a sample to a time series', complexity='O(M) where M is the number of compaction rules or O(1) with no compaction', arity='-4', since='1.0.0', group='module')

    def test_command_info_ts_alter(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.ALTER')
            assert res
            assert_docs(env, 'TS.ALTER', summary='Update the retention, chunk size, duplicate policy, and labels of an existing time series', complexity='O(N) where N is the number of labels requested to update', arity='-2', since='1.0.0', group='module')

    def test_command_info_ts_create(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.CREATE')
            assert res
            assert_docs(env, 'TS.CREATE', summary='Create a new time series', complexity='O(1)', arity='-2', since='1.0.0', group='module')

    def test_command_info_ts_createrule(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.CREATERULE')
            assert res
            assert_docs(env, 'TS.CREATERULE', summary='Create a compaction rule', complexity='O(1)', arity='-5', since='1.0.0', group='module')

    def test_command_info_ts_range(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.RANGE')
            assert res
            assert_docs(env, 'TS.RANGE', summary='Query a range in forward direction', complexity='O(n/m+k) where n = Number of data points, m = Chunk size (data points per chunk), k = Number of data points that are in the requested range', arity='-4', since='1.0.0', group='module')

    def test_command_info_ts_queryindex(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.QUERYINDEX')
            assert res
            assert_docs(env, 'TS.QUERYINDEX', summary='Get all time series keys matching a filter list', complexity='O(n) where n is the number of time-series that match the filters', arity='-2', since='1.0.0', group='module')

    def test_command_info_ts_querylabels(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.QUERYLABELS')
            assert res
            assert_docs(env, 'TS.QUERYLABELS', summary='Get all label names, or all values of a given label, for time series matching a filter list, or all series', complexity='O(n) where n is the number of time-series that match the filters (all indexed series when FILTER is omitted)', arity='-2', since='8.10.0', group='module')

    def test_command_info_ts_info(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.INFO')
            assert res
            assert_docs(env, 'TS.INFO', summary='Returns information and statistics for a time series', complexity='O(1)', arity='-2', since='1.0.0', group='module')

    def test_command_info_ts_madd(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.MADD')
            assert res
            assert_docs(env, 'TS.MADD', summary='Append new samples to one or more time series', complexity='O(N*M) when N is the amount of series updated and M is the amount of compaction rules or O(N) with no compaction', arity='-4', since='1.0.0', group='module')

    def test_command_info_ts_mget(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.MGET')
            assert res
            assert_docs(env, 'TS.MGET', summary='Get the sample with the highest timestamp from each time series matching a specific filter', complexity='O(n) where n is the number of time-series that match the filters', arity='-3', since='1.0.0', group='module')

    def test_command_info_ts_revrange(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.REVRANGE')
            assert res
            assert_docs(env, 'TS.REVRANGE', summary='Query a range in reverse direction', complexity='O(n/m+k) where n = Number of data points, m = Chunk size (data points per chunk), k = Number of data points that are in the requested range', arity='-4', since='1.4.0', group='module')

    def test_command_info_ts_nrange(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.NRANGE')
            assert res
            assert_docs(env, 'TS.NRANGE', summary='Query a range across multiple time series in forward direction, returning the results pivoted by timestamp (one value column per key)', complexity='O(numkeys*(n/m+k)) where n = Number of samples, m = Chunk size (samples per chunk), k = Number of samples that are in the requested range', arity='-5', since='8.10.0', group='module')

    def test_command_info_ts_nrevrange(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.NREVRANGE')
            assert res
            assert_docs(env, 'TS.NREVRANGE', summary='Query a range across multiple time series in reverse direction, returning the results pivoted by timestamp (one value column per key)', complexity='O(numkeys*(n/m+k)) where n = Number of samples, m = Chunk size (samples per chunk), k = Number of samples that are in the requested range', arity='-5', since='8.10.0', group='module')

    def test_command_info_ts_read(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.READ')
            assert res
            assert_docs(env, 'TS.READ', summary='Read: return up to max_count samples with timestamp >= timestamp. With BLOCK, waits up to milliseconds ms until at least min_count qualifying samples exist', complexity='O(log(n)+k) where n is the number of samples in the series and k is the number of returned samples', arity='-3', since='8.10.0', group='module')

    def test_command_info_ts_mrange(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.MRANGE')
            assert res
            assert_docs(env, 'TS.MRANGE', summary='Query a range across multiple time series by filters in forward direction', complexity='O(n/m+k) where n = Number of data points, m = Chunk size (data points per chunk), k = Number of data points that are in the requested ranges', arity='-4', since='1.0.0', group='module')

    def test_command_info_ts_mrevrange(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.MREVRANGE')
            assert res
            assert_docs(env, 'TS.MREVRANGE', summary='Query a range across multiple time series by filters in reverse direction', complexity='O(n/m+k) where n = Number of data points, m = Chunk size (data points per chunk), k = Number of data points that are in the requested ranges', arity='-4', since='1.4.0', group='module')

    def test_command_info_ts_incrby(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.INCRBY')
            assert res
            assert_docs(env, 'TS.INCRBY', summary='Increase the value of the latest sample', complexity='O(M) when M is the amount of compaction rules or O(1) with no compaction', arity='-3', since='1.0.0', group='module')

    def test_command_info_ts_decrby(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.DECRBY')
            assert res
            assert_docs(env, 'TS.DECRBY', summary='Decrease the value of the latest sample', complexity='O(M) when M is the amount of compaction rules or O(1) with no compaction', arity='-3', since='1.0.0', group='module')

    def test_command_info_ts_del(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.DEL')
            assert res
            # TS.DEL takes exactly 4 tokens (key fromTimestamp toTimestamp): TS_DEL_INFO
            # declares a fixed arity of 4 and TSDB_delete rejects argc != 4, so the
            # previously asserted '-4' did not match the command's real arity.
            assert_docs(env, 'TS.DEL', summary='Delete all samples between two timestamps for a given time series', complexity='O(N) where N is the number of data points that will be removed', arity='4', since='1.6.0', group='module')

    def test_command_info_ts_deleterule(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.DELETERULE')
            assert res
            assert_docs(env, 'TS.DELETERULE', summary='Delete a compaction rule', complexity='O(1)', arity='3', since='1.0.0', group='module')

    def test_command_info_ts_get(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.GET')
            assert res
            assert_docs(env, 'TS.GET', summary='Get the sample with the highest timestamp from a given time series', complexity='O(1)', arity='-2', since='1.0.0', group='module')

    def test_all_registered_commands_in_commands_json(self):
        # Regression test for https://github.com/RedisTimeSeries/RedisTimeSeries/issues/2127
        # (MOD-17349): TS.QUERYLABELS was implemented and registered but missing from
        # commands.json. Assert that every command the module registers has a matching
        # entry in commands.json, so client/doc scaffolding generated from that file
        # cannot silently drop a command again.
        #
        # This compares the two repo artifacts directly instead of asking the server for
        # COMMAND LIST: redis-py parses a COMMAND reply with a callback that expects the
        # full command-table layout and raises on the flat name array that COMMAND LIST
        # returns, and in cluster mode that parsing happens on the per-node client. The
        # per-command tests above already cover the runtime metadata.
        env = self.env
        repo_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..')

        with open(os.path.join(repo_root, 'commands.json')) as f:
            documented = {name.upper() for name in json.load(f).keys()}

        with open(os.path.join(repo_root, 'src', 'module.c')) as f:
            module_src = f.read()
        # Commands are registered either through the RegisterCommandWithModesAndAcls helper
        # or through a direct RedisModule_CreateCommand call, both taking the command name as
        # the argument after ctx.
        registered = {name.upper() for name in re.findall(
            r'(?:RedisModule_CreateCommand|RegisterCommandWithModesAndAcls)\s*\(\s*ctx\s*,\s*"(ts\.[a-z_]+)"',
            module_src)}
        # Guard against the pattern above silently matching nothing if the registration
        # style changes, which would make this test vacuously pass.
        env.assertGreater(len(registered), 15)

        missing = sorted(registered - documented)
        env.assertEqual(missing, [], message='Commands registered in src/module.c but missing from commands.json: %s' % missing)
        # Direct guard for the command from issue #2127.
        env.assertContains('TS.QUERYLABELS', documented)

    # NOTE: Skipping COMMAND DOCS test for now due to client parsing differences across redis-py versions

