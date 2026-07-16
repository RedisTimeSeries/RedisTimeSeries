from includes import *
from utils import slot_table


def test_querylabels_split_merge():
    # Pin two keys to two different shards (slot 0 and the top slot) so the same label
    # name+value is spread across shards. The coordinator must dedup them into a single
    # entry, not report duplicates -- this is the multi-shard merge's main correctness case.
    env = Env(shardsCount=3)
    env.flush()
    keyA = 'keyA{%s}' % slot_table[0]
    keyB = 'keyB{%s}' % slot_table[16383]

    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        assert r.execute_command('TS.CREATE', keyA, 'LABELS', 'commonlabel', 'sharedvalue', 'onlyA', 'x')
        assert r.execute_command('TS.CREATE', keyB, 'LABELS', 'commonlabel', 'sharedvalue', 'onlyB', 'y')

        # LABELS: both shards contribute 'commonlabel'; must be deduped to one entry, and both
        # shard-local labels ('onlyA'/'onlyB') must still show up.
        res = r1.execute_command('TS.QUERYLABELS', 'LABELS', 'FILTER', 'commonlabel=sharedvalue')
        assert sorted(res) == sorted([b'commonlabel', b'onlyA', b'onlyB'])

        # VALUES: the shared value is deduped across shards, not duplicated.
        res = r1.execute_command('TS.QUERYLABELS', 'VALUES', 'commonlabel', 'FILTER', 'commonlabel=sharedvalue')
        assert res == [b'sharedvalue']

        res = r1.execute_command('TS.QUERYLABELS', 'VALUES', 'onlyA', 'FILTER', 'commonlabel=sharedvalue')
        assert res == [b'x']

        res = r1.execute_command('TS.QUERYLABELS', 'VALUES', 'onlyB', 'FILTER', 'commonlabel=sharedvalue')
        assert res == [b'y']


def test_querylabels_no_filter_multishard():
    env = Env(shardsCount=3)
    env.flush()
    keyA = 'keyA{%s}' % slot_table[0]
    keyB = 'keyB{%s}' % slot_table[16383]

    with env.getClusterConnectionIfNeeded() as r, env.getConnection(1) as r1:
        assert r.execute_command('TS.CREATE', keyA, 'LABELS', 'region', 'us')
        assert r.execute_command('TS.CREATE', keyB, 'LABELS', 'region', 'eu')

        res = r1.execute_command('TS.QUERYLABELS', 'LABELS')
        assert sorted(res) == sorted([b'region'])

        res = r1.execute_command('TS.QUERYLABELS', 'VALUES', 'region')
        assert sorted(res) == sorted([b'us', b'eu'])
