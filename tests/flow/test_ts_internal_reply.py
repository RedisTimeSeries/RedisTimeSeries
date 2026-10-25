from includes import Env, RLEC_CLUSTER, verifyClusterInitialized
from redis.crc import key_slot


def _check_internal_replies(protocol):
    if RLEC_CLUSTER:
        Env().skip()
    env = Env(env='oss-cluster', shardsCount=3, protocol=protocol,
              redisConfigFileContent='ts-libmr-protocol internal\n')
    for shard in range(1, 4):
        conn = env.getConnection(shard)
        config = conn.execute_command('CONFIG', 'GET', 'ts-libmr-protocol')
        env.assertEqual(config[b'ts-libmr-protocol'] if isinstance(config, dict) else config[1],
                        b'INTERNAL')
        conn.execute_command('CONFIG', 'SET', 'ts-chunk-size-bytes', 128)
        conn.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
        conn.execute_command('timeseries.FORCESHARDSCONNECTION')
    verifyClusterInitialized(env)

    with env.getClusterConnectionIfNeeded() as client, env.getConnection(1) as coordinator:
        # Put every reply size on every shard, including empty keys before populated
        # keys in multi-aggregation replies. Eight samples fit in a 128-byte regular chunk.
        ranges = coordinator.execute_command('CLUSTER', 'SLOTS')
        keys = []
        for shard, slot_range in enumerate(ranges):
            tag = next(str(i) for i in range(100000)
                       if slot_range[0] <= key_slot(str(i).encode()) <= slot_range[1])
            for count in (0, 1, 2, 7, 8, 9, 65):
                key = f'reply:{{{tag}}}:{count:03}'
                keys.append(key)
                client.execute_command('TS.CREATE', key, 'LABELS', 'test', 'reply', 'group', 'all')
                for timestamp in range(count):
                    client.execute_command('TS.ADD', key, timestamp, shard + timestamp * 0.5)

        def entries(reply):
            if isinstance(reply, dict):
                return {key: value[-1] for key, value in reply.items()}
            return {row[0]: row[2] for row in reply}

        for reverse in (False, True):
            local_command = 'TS.REVRANGE' if reverse else 'TS.RANGE'
            multi_command = 'TS.MREVRANGE' if reverse else 'TS.MRANGE'
            for options in ([], ['COUNT', 3], ['FILTER_BY_VALUE', 1, 3],
                            ['AGGREGATION', 'avg', 2],
                            ['AGGREGATION', 'min,max', 2]):
                expected = {key.encode(): client.execute_command(local_command, key, '-', '+', *options)
                            for key in keys}
                actual = coordinator.execute_command(multi_command, '-', '+', *options,
                                                     'WITHLABELS', 'FILTER', 'test=reply')
                env.assertEqual(entries(actual), expected)
                label_blocks = ([value[0] for value in actual.values()] if isinstance(actual, dict)
                                else [row[1] for row in actual])
                for labels in label_blocks:
                    env.assertEqual(dict(labels), {b'test': b'reply', b'group': b'all'})

        expected_last = {key.encode(): client.execute_command('TS.GET', key) for key in keys}
        actual_last = coordinator.execute_command('TS.MGET', 'SELECTED_LABELS', 'group', 'missing',
                                                  'FILTER', 'test=reply')
        env.assertEqual(entries(actual_last), expected_last)
        label_blocks = ([value[0] for value in actual_last.values()] if isinstance(actual_last, dict)
                        else [row[1] for row in actual_last])
        for labels in label_blocks:
            env.assertEqual(dict(labels), {b'group': b'all', b'missing': None})

        # Reduce across the mix of small uncompressed and larger compressed temporary results.
        expected_group = []
        local_samples = [client.execute_command('TS.RANGE', key, '-', '+') for key in keys]
        for timestamp in range(65):
            value = sum(float(samples[timestamp][1]) for samples in local_samples
                        if len(samples) > timestamp)
            expected_group.append([timestamp, value])
        grouped = coordinator.execute_command('TS.MRANGE', '-', '+', 'FILTER', 'test=reply',
                                              'GROUPBY', 'group', 'REDUCE', 'sum')
        samples = entries(grouped)[b'group=all']
        env.assertEqual([[ts, float(value)] for ts, value in samples], expected_group)


def test_internal_reply_chunk_boundaries_resp2():
    _check_internal_replies(2)


def test_internal_reply_chunk_boundaries_resp3():
    _check_internal_replies(3)
