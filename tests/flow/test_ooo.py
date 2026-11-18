import json
import random
import time

import pytest
import redis
from includes import Env
from test_helper_classes import _get_ts_info
from includes import *


def test_ooo(self):
    with Env().getClusterConnectionIfNeeded() as r:
        quantity = 50001
        type_list = ['', 'UNCOMPRESSED']
        for chunk_type in type_list:
            r.execute_command('ts.create', 'no_ooo', chunk_type, 'CHUNK_SIZE', 136, 'DUPLICATE_POLICY', 'BLOCK')
            r.execute_command('ts.create', 'ooo', chunk_type, 'CHUNK_SIZE', 136, 'DUPLICATE_POLICY', 'LAST')
            for i in range(0, quantity, 5):
                r.execute_command('ts.add', 'no_ooo', i, i)
            for i in range(0, quantity, 10):
                r.execute_command('ts.add', 'ooo', i, i)
            for i in range(5, quantity, 10):  # limit
                r.execute_command('ts.add', 'ooo', i, i)

            ooo_res = r.execute_command('ts.range', 'ooo', '-', '+')
            no_ooo_res = r.execute_command('ts.range', 'no_ooo', '-', '+')
            assert len(ooo_res) == len(no_ooo_res)
            for i in range(len(ooo_res)):
                assert ooo_res[i] == no_ooo_res[i]

            ooo_res = r.execute_command('ts.range', 'ooo', 1000, 1000)
            assert ooo_res[0] == [1000, b'1000']
            last_sample = r.execute_command('ts.get', 'ooo')
            r.execute_command('ts.add', 'ooo', 1000, 42)
            ooo_res = r.execute_command('ts.range', 'ooo', 1000, 1000)
            assert ooo_res[0] == [1000, b'42']
            assert last_sample == r.execute_command('ts.get', 'ooo')

            r.execute_command('ts.add', 'ooo', last_sample[0], 42)
            assert [last_sample[0], b'42'] == r.execute_command('ts.get', 'ooo')

            r.execute_command('DEL', 'no_ooo')
            r.execute_command('DEL', 'ooo')


def test_ooo_with_retention(self):
    with Env().getClusterConnectionIfNeeded() as r:
        retention = 13
        batch = 100
        r.execute_command('ts.create', 'ooo', 'CHUNK_SIZE', 128, 'RETENTION', retention, 'DUPLICATE_POLICY', 'LAST')
        for i in range(batch):
            assert r.execute_command('ts.add', 'ooo', i, i) == i
        assert r.execute_command('ts.range', 'ooo' ,0, batch - retention - 2) == []
        assert len(r.execute_command('ts.range', 'ooo', '-', '+')) == retention + 1

        with pytest.raises(redis.ResponseError) as excinfo:
            assert r.execute_command('ts.add', 'ooo', 70, 70)

        for i in range(batch, batch * 2):
            assert r.execute_command('ts.add', 'ooo', i, i) == i
        assert r.execute_command('ts.range', 'ooo', 0, batch * 2 - retention - 2) == []
        assert len(r.execute_command('ts.range', 'ooo', '-', '+')) == retention + 1

        # test for retention larger than timestamp
        r.execute_command('ts.create', 'large', 'RETENTION', 1000000, 'DUPLICATE_POLICY', 'LAST')
        assert r.execute_command('ts.add', 'large', 100, 0) == 100
        assert r.execute_command('ts.add', 'large', 101, 0) == 101
        assert r.execute_command('ts.add', 'large', 100, 0) == 100


def test_ooo_split(self):
    with Env().getClusterConnectionIfNeeded() as r:
        quantity = 5000

        type_list = ['', 'UNCOMPRESSED']
        for chunk_type in type_list:
            r.execute_command('ts.create', 'split', chunk_type)
            r.execute_command('ts.add', 'split', quantity, 42)
            for i in range(quantity):
                r.execute_command('ts.add', 'split', i, i * 1.01)
            assert _get_ts_info(r, 'split').chunk_count in [13, 32]
            res = r.execute_command('ts.range', 'split', '-', '+')
            for i in range(quantity - 1):
                assert res[i][0] + 1 == res[i + 1][0]
                assert round(float(res[i][1]) + 1.01, 2) == round(float(res[i + 1][1]), 2)

            r.execute_command('DEL', 'split')


def test_ooo_split_after_delete_tracks_total_samples(self):
    with Env().getClusterConnectionIfNeeded() as r:
        key = 'split_after_delete'
        samples = [
            (1978, 0.011848837952325164),
            (3866, 0.057698316631206836),
            (4973, 0.4736283651797485),
            (997, 0.8497118940972751),
            (4718, 0.6473209756322149),
            (2396, 0.7736732429407744),
            (836, 0.4298092626856276),
            (3360, 0.3925336842535311),
        ]

        r.execute_command('TS.CREATE', key, 'COMPRESSED', 'CHUNK_SIZE', 48,
                          'DUPLICATE_POLICY', 'LAST')
        for timestamp, value in samples:
            r.execute_command('TS.ADD', key, timestamp, value)
        assert r.execute_command('TS.DEL', key, 973, 3666) == 4
        r.execute_command('TS.ADD', key, 2988, 0.24194011099574075)

        assert _get_ts_info(r, key).total_samples == 5
        assert len(r.execute_command('TS.RANGE', key, '-', '+')) == 5

        dump = r.execute_command('DUMP', key)
        r.execute_command('DEL', key)
        r.execute_command('RESTORE', key, 0, dump)
        assert _get_ts_info(r, key).total_samples == 5
        assert len(r.execute_command('TS.RANGE', key, '-', '+')) == 5


def test_random_upserts_and_deletes(self):
    iterations = 10
    min_samples_per_iteration, max_samples_per_iteration = 10, 50
    min_timestamp, max_timestamp = 1000, 2000
    min_value, max_value = 1000.0, 2000.0
    key = 'random_upserts_and_deletes'
    seed = time.time_ns()
    print(f'test_random_upserts_and_deletes seed: {seed}', flush=True)
    rng = random.Random(seed)

    def jsonify(info_debug):
        info_debug = decode_if_needed(info_debug)
        info_debug_json = dict(zip(info_debug[::2], info_debug[1::2]))
        info_debug_json['Chunks'] = [
            dict(zip(chunk[::2], chunk[1::2]))
            for chunk in info_debug_json['Chunks']
        ]
        return info_debug_json

    def json_dump(j):
        return json.dumps(j, indent=2)

    def assert_invariants(r):
        info_debug = r.execute_command('TS.INFO', key, 'DEBUG')
        info_debug_json = jsonify(info_debug)
        chunks = info_debug_json['Chunks']
        total_samples = info_debug_json['totalSamples']

        assert info_debug_json['chunkCount'] == len(chunks), json_dump(info_debug_json)
        assert sum(chunk['samples'] for chunk in chunks) == total_samples, json_dump(info_debug_json)
        first_timestamp = chunks[0]['startTimestamp'] if total_samples else 0
        last_timestamp = chunks[-1]['endTimestamp'] if total_samples else 0
        assert info_debug_json['firstTimestamp'] == first_timestamp, json_dump(info_debug_json)
        assert info_debug_json['lastTimestamp'] == last_timestamp, json_dump(info_debug_json)

    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('TS.CREATE', key, 'COMPRESSED', 'CHUNK_SIZE', 48,
                          'DUPLICATE_POLICY', 'LAST')
        for _ in range(iterations):
            sample_count = rng.randint(min_samples_per_iteration, max_samples_per_iteration)
            for _ in range(sample_count):
                timestamp = rng.randint(min_timestamp, max_timestamp)
                value = rng.uniform(min_value, max_value)
                r.execute_command('TS.ADD', key, timestamp, value)
                assert_invariants(r)

            start_timestamp = rng.randint(min_timestamp, max_timestamp)
            end_timestamp = rng.randint(min_timestamp, max_timestamp)
            if start_timestamp > end_timestamp:
                start_timestamp, end_timestamp = end_timestamp, start_timestamp
            r.execute_command('TS.DEL', key, start_timestamp, end_timestamp)
            assert_invariants(r)


def test_rand_oom(self):
    random.seed(20)
    start_ts = 1592917924000
    current_ts = int(start_ts)
    data = []
    ooo_data = []
    start_ooo = random.randrange(500, 9000)
    amount = random.randrange(250, 1000)
    for i in range(10000):
        val = '%.5f' % random.gauss(50, 10.5)
        if i < start_ooo or i > start_ooo + amount:
            data.append([current_ts, val])
        else:
            ooo_data.append([current_ts, val])
        current_ts += random.randrange(20, 1000)
    with Env().getClusterConnectionIfNeeded() as r:
        r.execute_command('ts.create', 'tester')
        for sample in data:
            r.execute_command('ts.add', 'tester', sample[0], sample[1])
        for sample in ooo_data:
            r.execute_command('ts.add', 'tester', sample[0], sample[1])

        all_data = sorted(data + ooo_data, key=lambda x: x[0])
        res = r.execute_command('ts.range', 'tester', '-', '+')
        assert len(res) == len(all_data)
        for i in range(len(all_data)):
            assert all_data[i][0] == res[i][0]
            assert float(all_data[i][1]) == float(res[i][1])
