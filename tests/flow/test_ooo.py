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


def test_random_upserts_and_deletes(self):
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

    def assert_invariants(r, key, expected_samples):
        info_debug = r.execute_command('TS.INFO', key, 'DEBUG')
        info_debug_json = jsonify(info_debug)
        chunks = info_debug_json['Chunks']
        total_samples = info_debug_json['totalSamples']

        assert info_debug_json['chunkCount'] == len(chunks), json_dump(info_debug_json)
        assert sum(chunk['samples'] for chunk in chunks) == total_samples, json_dump(info_debug_json)
        assert total_samples == len(expected_samples), json_dump(info_debug_json)
        first_timestamp = chunks[0]['startTimestamp'] if total_samples else 0
        last_timestamp = chunks[-1]['endTimestamp'] if total_samples else 0
        assert info_debug_json['firstTimestamp'] == first_timestamp, json_dump(info_debug_json)
        assert info_debug_json['lastTimestamp'] == last_timestamp, json_dump(info_debug_json)

        actual_samples = r.execute_command('TS.RANGE', key, '-', '+')
        assert [timestamp for timestamp, _ in actual_samples] == sorted(expected_samples), json_dump(info_debug_json)
        epsilon = 1e-9
        assert all(
            abs(float(value) - expected_samples[timestamp]) <= epsilon
            for timestamp, value in actual_samples
        ), json_dump(info_debug_json)

        copy_key = f'{key}:copy'
        r.execute_command('RESTORE', copy_key, 0, r.execute_command('DUMP', key))
        copy_info_debug_json = jsonify(r.execute_command('TS.INFO', copy_key, 'DEBUG'))
        key_specific_fields = {'keySelfName', 'memoryUsage'}
        original_info = {k: v for k, v in info_debug_json.items() if k not in key_specific_fields}
        copy_info = {k: v for k, v in copy_info_debug_json.items() if k not in key_specific_fields}
        assert copy_info == original_info, json_dump({'original': info_debug_json, 'copy': copy_info_debug_json})
        copy_samples = r.execute_command('TS.RANGE', copy_key, '-', '+')
        assert copy_samples == actual_samples, json_dump({'original': decode_if_needed(actual_samples), 'copy': decode_if_needed(copy_samples)})
        r.execute_command('DEL', copy_key)

    def run_iterations(r, key):
        expected_samples = {}
        iterations = 10
        min_samples_per_iteration, max_samples_per_iteration = 10, 50
        min_timestamp, max_timestamp = 1000, 2000
        min_value, max_value = 1000.0, 2000.0
        for _ in range(iterations):
            sample_count = rng.randint(min_samples_per_iteration, max_samples_per_iteration)
            for _ in range(sample_count):
                timestamp = rng.randint(min_timestamp, max_timestamp)
                value = rng.uniform(min_value, max_value)
                r.execute_command('TS.ADD', key, timestamp, value)
                expected_samples[timestamp] = value  # since the DUPLICATE_POLICY is LAST
                assert_invariants(r, key, expected_samples)

            start_timestamp = rng.randint(min_timestamp, max_timestamp)
            end_timestamp = rng.randint(min_timestamp, max_timestamp)
            if start_timestamp > end_timestamp:
                start_timestamp, end_timestamp = end_timestamp, start_timestamp
            r.execute_command('TS.DEL', key, start_timestamp, end_timestamp)
            for timestamp in list(expected_samples):
                if start_timestamp <= timestamp <= end_timestamp:
                    del expected_samples[timestamp]
            assert_invariants(r, key, expected_samples)

    with Env().getClusterConnectionIfNeeded() as r:
        for encoding in ('COMPRESSED', 'UNCOMPRESSED'):
            key = f'random_upserts_and_deletes_{encoding.lower()}'
            r.execute_command('TS.CREATE', key, encoding, 'CHUNK_SIZE', 48, 'DUPLICATE_POLICY', 'LAST')
            run_iterations(r, key)


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
