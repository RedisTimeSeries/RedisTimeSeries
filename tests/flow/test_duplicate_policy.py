import random

import pytest
import redis
from utils import Env
from test_helper_classes import _fill_data


class testDuplicationPolicyTests():
    def __init__(self):
        self.env = Env(moduleArgs='DUPLICATE_POLICY BLOCK')

    def test_ts_add_unknow_duplicate_policy(self):
        with self.env.getClusterConnectionIfNeeded() as r:
            with pytest.raises(redis.ResponseError) as excinfo:
                assert r.execute_command('TS.ADD', "test", 1, 1.5, "DUPLICATE_POLICY", "---------------")

    def test_precendence_key(self):
        with self.env.getClusterConnectionIfNeeded() as r:
            key = 'tester'
            key_no_dup = 'tester_no_dup'
            r.execute_command('TS.CREATE', key, 'DUPLICATE_POLICY', 'LAST')
            r.execute_command('TS.CREATE', key_no_dup)
            _fill_data(r, key)
            date_ranges = _fill_data(r, key_no_dup)

            overrided_ts = date_ranges[0][0] + 10
            overrided_value = 666
            with pytest.raises(redis.ResponseError):
                r.execute_command('TS.ADD', key_no_dup, overrided_ts, overrided_value)
            assert r.execute_command('TS.ADD', key, overrided_ts, overrided_value) == overrided_ts

            assert r.execute_command('TS.RANGE', key_no_dup, overrided_ts, overrided_ts) == [
                [overrided_ts, str(overrided_ts).encode('ascii')]]
            assert r.execute_command('TS.RANGE', key, overrided_ts, overrided_ts) == [
                [overrided_ts, str(overrided_value).encode('ascii')]]

            # check that inserting a non-duplicate sample doesn't fail
            non_dup_ts = date_ranges[0][1] + 1
            assert r.execute_command('TS.ADD', key_no_dup, non_dup_ts, overrided_value) == non_dup_ts

            # check that `ON_DUPLICATE` overrides the module configuration
            assert r.execute_command('TS.ADD', key_no_dup, overrided_ts, overrided_value, 'ON_DUPLICATE',
                                     'LAST') == overrided_ts
            assert r.execute_command('TS.RANGE', key_no_dup, overrided_ts, overrided_ts) == [
                [overrided_ts, str(overrided_value).encode('ascii')]]

            # check that `ON_DUPLICATE` overrides the key configuration
            assert r.execute_command('TS.ADD', key, overrided_ts, overrided_value * 10, 'ON_DUPLICATE',
                                     'MAX') == overrided_ts
            assert r.execute_command('TS.RANGE', key, overrided_ts, overrided_ts) == \
                   [[overrided_ts, str(overrided_value * 10).encode('ascii')]]

    def test_policies_correctness(self):
        policies = {
            'LAST': lambda x, y: y,
            'FIRST': lambda x, y: x,
            'MIN': min,
            'MAX': max,
            'SUM': lambda x, y: x + y
        }

        with self.env.getClusterConnectionIfNeeded() as r:
            key = 'tester'

            for chunk_type in ['', 'UNCOMPRESSED']:
                r.execute_command('TS.CREATE', key, chunk_type)
                date_ranges = _fill_data(r, key)
                overrided_ts = date_ranges[0][0] + 10
                # Verified Block
                with pytest.raises(redis.ResponseError):
                    r.execute_command('TS.ADD', key, overrided_ts, 1)

                for policy in policies:
                    old_value = int(r.execute_command('TS.RANGE', key, overrided_ts, overrided_ts)[0][1])
                    new_value = random.randint(-5000, 1000000)
                    assert r.execute_command('TS.ADD', key, overrided_ts, new_value, 'ON_DUPLICATE',
                                             policy) == overrided_ts
                    proccessed_value = int(r.execute_command('TS.RANGE', key, overrided_ts, overrided_ts)[0][1])
                    assert policies[policy](old_value,
                                            new_value) == proccessed_value, "check that {} is correct".format(policy)

                r.execute_command('DEL', key)
