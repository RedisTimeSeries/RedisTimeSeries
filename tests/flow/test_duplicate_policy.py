import random

# import pytest
import redis
from utils import Env
from test_helper_classes import _fill_data
from includes import *


class testDuplicationPolicyTests():
    def __init__(self):
        self.env = Env(moduleArgs='DUPLICATE_POLICY BLOCK')

    def test_ts_add_unknow_duplicate_policy(self):
        env = self.env
        with env.getClusterConnectionIfNeeded() as r:
            env.expect('TS.ADD', "test", 1, 1.5, "DUPLICATE_POLICY", conn=r).error()

            env.expect('TS.ADD', "test", 1, 1.5, "DUPLICATE_POLICY", "---------------", conn=r).error()

    def test_precendence_key(self):
        env = self.env
        with env.getClusterConnectionIfNeeded() as r:
            key = 'tester'
            key_no_dup = 'tester_no_dup'
            r.execute_command('TS.CREATE', key, 'DUPLICATE_POLICY', 'LAST')
            r.execute_command('TS.CREATE', key_no_dup)
            _fill_data(r, key)
            date_ranges = _fill_data(r, key_no_dup)

            overrided_ts = date_ranges[0][0] + 10
            overrided_value = 666
            env.expect('TS.ADD', key_no_dup, overrided_ts, overrided_value, conn=r).error()
            env.expect('TS.ADD', key, overrided_ts, overrided_value, conn=r).equal(overrided_ts)

            env.expect('TS.RANGE', key_no_dup, overrided_ts, overrided_ts, conn=r).equal([[overrided_ts, str(overrided_ts)]])
            env.expect('TS.RANGE', key, overrided_ts, overrided_ts, conn=r).equal([[overrided_ts, str(overrided_value)]])

            # check that inserting a non-duplicate sample doesn't fail
            non_dup_ts = date_ranges[0][1] + 1
            env.expect('TS.ADD', key_no_dup, non_dup_ts, overrided_value, conn=r).equal(non_dup_ts)

            # check that `ON_DUPLICATE` overrides the module configuration
            env.expect('TS.ADD', key_no_dup, overrided_ts, overrided_value, 'ON_DUPLICATE', 'LAST', conn=r).equal(overrided_ts)
            env.expect('TS.RANGE', key_no_dup, overrided_ts, overrided_ts, conn=r).equal([[overrided_ts, str(overrided_value)]])

            # check that `ON_DUPLICATE` overrides the key configuration
            env.expect('TS.ADD', key, overrided_ts, overrided_value * 10, 'ON_DUPLICATE', 'MAX', conn=r).equal(overrided_ts)
            env.expect('TS.RANGE', key, overrided_ts, overrided_ts, conn=r).equal([[overrided_ts, str(overrided_value * 10)]])

    def test_policies_correctness(self):
        env = self.env
        policies = {
            'LAST': lambda x, y: y,
            'FIRST': lambda x, y: x,
            'MIN': min,
            'MAX': max,
            'SUM': lambda x, y: x + y
        }

        with env.getClusterConnectionIfNeeded() as r:
            key = 'tester'

            for chunk_type in ['', 'UNCOMPRESSED']:
                r.execute_command('TS.CREATE', key, chunk_type)
                date_ranges = _fill_data(r, key)
                overrided_ts = date_ranges[0][0] + 10
                # Verified Block
                env.expect('TS.ADD', key, overrided_ts, 1, conn=r).error()

                for policy in policies:
                    old_value = int(r.execute_command('TS.RANGE', key, overrided_ts, overrided_ts)[0][1])
                    new_value = random.randint(-5000, 1000000)
                    env.expect('TS.ADD', key, overrided_ts, new_value, 'ON_DUPLICATE', policy, conn=r).equal(overrided_ts)
                    proccessed_value = int(r.execute_command('TS.RANGE', key, overrided_ts, overrided_ts)[0][1])
                    env.assertEqual(policies[policy](old_value, new_value), proccessed_value, message="check that {} is correct".format(policy))

                r.execute_command('DEL', key)
