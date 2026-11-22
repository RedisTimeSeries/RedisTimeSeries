from includes import *

# TS_AGG_TYPES_MAX is 16, so 17 aggregators is one too many.
TOO_MANY_AGGS = ','.join(['avg'] * 17)


class testQueryArgErrors:
    """Argument-parser error paths in src/query_language.c.

    All methods share one env: no case writes data, so the series created in
    __init__ stays valid for every method.
    """

    def __init__(self):
        self.env = Env(decodeResponses=True)
        self.env.getClusterConnectionIfNeeded().execute_command(
            'TS.ADD', '{q}:src', 1, 1, 'LABELS', 'name', 'q')

    def _assert_errors(self, conn, cases):
        for args, msg in cases:
            with self.env.assertResponseError(contained=msg):
                conn.execute_command(*args)

    def test_range_args(self):
        # Keyed commands: the cluster client routes them to the key's shard.
        conn = self.env.getClusterConnectionIfNeeded()
        self._assert_errors(conn, [
            (('TS.RANGE', '{q}:src', '-', '+', 'BUCKETTIMESTAMP', 'start', 'AGGREGATION', 'avg', 10),
             'BUCKETTIMESTAMP flag should be the 3rd or 4th flag after AGGREGATION flag'),
            (('TS.RANGE', '{q}:src', '-', '+', 'AGGREGATION', 'avg', 10, 'BUCKETTIMESTAMP'),
             'wrong number of arguments'),
            (('TS.RANGE', '{q}:src', '-', '+', 'EMPTY', 'AGGREGATION', 'avg', 10),
             'EMPTY flag should be the 3rd or 5th flag after AGGREGATION flag'),
            (('TS.RANGE', '{q}:src', '-', '+', 'AGGREGATION', TOO_MANY_AGGS, 10),
             'Too many aggregation types'),
            (('TS.RANGE', '{q}:src', '-', '+', 'FILTER_BY_TS', 'abc'),
             'FILTER_BY_TS one or more arguments are missing'),
        ])
        # The rejected requests left nothing behind: a valid one still works.
        self.env.assertEqual(conn.execute_command('TS.RANGE', '{q}:src', '-', '+'), [[1, '1']])

    def test_multi_series_args(self):
        # Key-less commands: any shard parses them, so send them to the first one.
        conn = self.env.getConnection()
        self._assert_errors(conn, [
            (('TS.MGET', 'FILTER', '='), 'failed parsing labels'),
            (('TS.MGET', 'FILTER', '!='), 'failed parsing labels'),
            (('TS.MGET', 'FILTER', '!=('), 'failed parsing labels'),
            (('TS.MGET', 'FILTER'), 'wrong number of arguments'),
            (('TS.MRANGE', '-', '+'), 'wrong number of arguments'),
        ])
        # The rejected requests left nothing behind: a valid one still works.
        self.env.assertEqual(conn.execute_command('TS.MGET', 'FILTER', 'name=q'),
                             [['{q}:src', [], [1, '1']]])
