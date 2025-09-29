from includes import *
from docs_utils import *

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
            assert_docs(env, 'TS.INCRBY', summary='Increment the value of a sample at a timestamp or last value in a time series', complexity='O(M) where M is the number of compaction rules or O(1) with no compaction', arity='-3', since='1.0.0', group='module')

    def test_command_info_ts_decrby(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.DECRBY')
            assert res
            assert_docs(env, 'TS.DECRBY', summary='Decrement the value of a sample at a timestamp or last value in a time series', complexity='O(M) where M is the number of compaction rules or O(1) with no compaction', arity='-3', since='1.0.0', group='module')

    def test_command_info_ts_del(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.DEL')
            assert res
            assert_docs(env, 'TS.DEL', summary='Delete all samples between two timestamps for a given time series', complexity='O(N) where N is the number of data points that will be removed', arity='-4', since='1.6.0', group='module')

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

    # NOTE: Skipping COMMAND DOCS test for now due to client parsing differences across redis-py versions

