import asyncio
import unittest

from client import (CoordinationError, LocalMRClient, Series, merge_series,
                    reduce_series, supports_local, topology, validate_local_replies)


SLOTS = [[0, 8191, ['a', 7000, 'id-a']], [8192, 16383, ['b', 7001, 'id-b']]]
DOCS = [name for command in ('ts.mrange', 'ts.mrevrange') for name in (
    command, ['arguments', [[], [], ['type', 'pure-token', 'token', 'LOCAL']]])]


class MergeTests(unittest.TestCase):
    def test_slot_coverage_fragmented_unsorted_and_empty_shards(self):
        replies = [[1, [[8192, 12000], [0, 4095]], []],
                   [1, [[12001, 16383], [4096, 8191]], []], [1, [], []]]
        self.assertEqual(validate_local_replies(replies), [[], [], []])

    def test_slot_gaps_overlaps_and_empty_coverage_rejected(self):
        for ranges in ([], [[0, 8190], [8192, 16383]],
                       [[0, 8192], [8192, 16383]], [[1, 16383]],
                       [[0, 16382]], [[0, 16383], [0, 16383]]):
            with self.subTest(ranges=ranges), self.assertRaises(CoordinationError):
                validate_local_replies([[1, ranges, []]])

    def test_slot_envelope_validation(self):
        for reply in ([], [2, [[0, 16383]], []], [True, [[0, 16383]], []],
                      [1, [[-1, 16383]], []], [1, [[0, 16384]], []],
                      [1, [[10, 0]], []], [1, [[0, '16383']], []],
                      [1, [], [['key', [], []]]]):
            with self.subTest(reply=reply), self.assertRaises(CoordinationError):
                validate_local_replies([reply])

    def test_nested_shard_error_propagates(self):
        with self.assertRaisesRegex(RuntimeError, 'denied'):
            validate_local_replies([[1, [[0, 16383]], RuntimeError('denied')]])

    def test_average_weights_series_not_shards(self):
        replies = [
            [['a', [['site', 'x']], [[0, '0']]],
             ['b', [['site', 'x']], [[0, '10']]]],
            [['c', [['site', 'x']], [[0, '50']]]],
        ]
        group = reduce_series(merge_series(replies), 'site', 'avg')[0]
        self.assertEqual(group.samples, [(0, 20.0)])
        self.assertEqual(group.sources, ('a', 'b', 'c'))

    def test_sparse_samples_reverse_and_count(self):
        rows = [Series('a', {'g': 'x'}, [(0, 1), (10, 2)]),
                Series('b', {'g': 'x'}, [(10, 8), (20, 9)]),
                Series('c', {}, [(20, 100)])]
        expected = {'sum': 10, 'avg': 5, 'count': 2, 'min': 2, 'max': 8}
        for reducer, value in expected.items():
            group = reduce_series(rows, 'g', reducer)[0]
            self.assertEqual(group.samples[1], (10, value))
        self.assertEqual(reduce_series(rows, 'g', 'sum', 1, True)[0].samples,
                         [(20, 9)])

    def test_duplicate_series_fails(self):
        row = ['a', [], [[0, '1']]]
        with self.assertRaises(CoordinationError):
            merge_series([[row], [row]])

    def test_nonfinite_group_values_rejected(self):
        for value in (float('nan'), float('inf')):
            with self.assertRaises(ValueError):
                reduce_series([Series('a', {'g': 'x'}, [(0, value)])], 'g', 'sum')

    def test_capability_required_for_both_commands(self):
        self.assertTrue(supports_local(DOCS))
        self.assertFalse(supports_local([]))
        self.assertFalse(supports_local(DOCS[:2]))

    def test_full_slot_coverage_and_owner_identity(self):
        self.assertEqual(len(topology(SLOTS, 'seed')), 2)
        with self.assertRaises(CoordinationError):
            topology(SLOTS[:1], 'seed')
        with self.assertRaises(CoordinationError):
            topology(SLOTS + SLOTS, 'seed')
        changed = [[0, 8191, ['a', 7000, 'new-id']], SLOTS[1]]
        self.assertNotEqual(topology(SLOTS, 'seed'), topology(changed, 'seed'))


class Node:
    def __init__(self, key, value, rendezvous):
        self.key, self.value, self.rendezvous = key, value, rendezvous
        self.calls = []
        self.error = None
        self.closed = False
        self.ranges = [[0, 8191]] if key == 'a' else [[8192, 16383]]

    async def execute_command(self, *args):
        self.calls.append(args)
        if args[0] == 'COMMAND DOCS':
            return DOCS
        self.rendezvous['started'] += 1
        if self.rendezvous['started'] == 2:
            self.rendezvous['event'].set()
        await self.rendezvous['event'].wait()
        if self.error:
            raise self.error
        return [1, self.ranges, [[self.key, [['g', 'x']], [[0, str(self.value)]]]]]

    async def aclose(self):
        self.closed = True


class Seed:
    def __init__(self):
        self.maps = [SLOTS]

    async def execute_command(self, *args):
        return self.maps.pop(0) if len(self.maps) > 1 else self.maps[0]

    async def aclose(self):
        pass


class ClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        rendezvous = {'started': 0, 'event': asyncio.Event()}
        self.nodes = [Node('a', 1, rendezvous), Node('b', 3, rendezvous)]
        self.seed = Seed()
        self.client = LocalMRClient(self.seed, lambda host, port: self.nodes[port - 7000],
                                    'seed', timeout=0.2)
        await self.client._initialize()

    async def asyncTearDown(self):
        await self.client.aclose()

    async def test_parallel_fanout_and_local_wire_position(self):
        rows = await self.client.mrange('-', '+', filters=['g=x'])
        self.assertEqual([row.key for row in rows], ['a', 'b'])
        for node in self.nodes:
            self.assertEqual(node.calls[-1], ('TS.MRANGE', '-', '+', 'LOCAL', 'FILTER', 'g=x'))

    async def test_grouped_count_is_not_sent_to_shards(self):
        result = await self.client.mrange('-', '+', filters=['g=x'],
                                          groupby='g', reducer='avg', count=1, reverse=True)
        self.assertEqual(result[0].samples, [(0, 2)])
        for node in self.nodes:
            self.assertNotIn('COUNT', node.calls[-1])
            self.assertNotIn('GROUPBY', node.calls[-1])
            self.assertIn('WITHLABELS', node.calls[-1])
            self.assertEqual(node.calls[-1][0], 'TS.MREVRANGE')

    async def test_shard_error_no_partial_result(self):
        self.nodes[1].error = RuntimeError('shard unavailable')
        with self.assertRaisesRegex(RuntimeError, 'shard unavailable'):
            await self.client.mrange('-', '+', filters=['g=x'])

    async def test_slot_overlap_rejected_before_grouping(self):
        self.nodes[1].ranges = [[8191, 16383]]
        self.client.check_topology = False
        with self.assertRaisesRegex(CoordinationError, 'unavailable slots'):
            await self.client.mrange('-', '+', filters=['g=x'], groupby='g', reducer='sum')

    async def test_topology_change_after_query_rejected(self):
        changed = [[0, 8191, ['a', 7000, 'new-id']], SLOTS[1]]
        self.seed.maps = [SLOTS, changed]
        with self.assertRaisesRegex(CoordinationError, 'during query'):
            await self.client.mrange('-', '+', filters=['g=x'])

    async def test_old_module_rejected(self):
        async def old_module(*args):
            return []
        self.nodes[0].execute_command = old_module
        with self.assertRaisesRegex(CoordinationError, 'LOCAL support'):
            await self.client._initialize()

    async def test_timeout_cancels_shard_tasks(self):
        cancelled = asyncio.Event()

        async def stuck(*args):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        self.nodes[0].execute_command = stuck
        with self.assertRaises(asyncio.TimeoutError):
            await self.client.mrange('-', '+', filters=['g=x'])
        self.assertTrue(cancelled.is_set())


if __name__ == '__main__':
    unittest.main()
