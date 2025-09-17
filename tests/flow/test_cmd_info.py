from includes import *
from docs_utils import *

class testCommandDocsAndHelp():
    def __init__(self):
        self.env = Env(decodeResponses=True)

    def test_command_info_ts_revrange(self):
        env = self.env
        con = env.getConnection()
        if is_redis_version_lower_than(con, '7.0.0', env.isCluster()):
            env.skip()
        with env.getClusterConnectionIfNeeded() as r:
            res = r.execute_command('COMMAND', 'INFO', 'TS.REVRANGE')
            assert res
            assert_docs(env, 'TS.REVRANGE', summary='Query a range in reverse direction', complexity='O(n/m+k) where n = Number of data points, m = Chunk size (data points per chunk), k = Number of data points that are in the requested range', arity='-4', since='1.4.0', group='module')

    # NOTE: Skipping COMMAND DOCS test for now due to client parsing differences across redis-py versions

