import random
import pytest
import redis
import time
from utils import Env, set_hertz
from test_helper_classes import _insert_data

CPU_METRICS = [
    "user",
    "system",
    "idle",
    "nice",
    "iowait",
    "irq",
    "softirq",
    "steal",
    "guest",
    "guest_nice",
]

CHUNK_TYPES = [
    "COMPRESSED:TURBO_GORILLA",
    "COMPRESSED",
    "",
    "COMPRESSED:GORILLA",
    "UNCOMPRESSED",
]


def serie_name(host_id, cpu_metric_name):
    return "{host_" + "{}".format(host_id) + "}}_cpu_usage_{}".format(cpu_metric_name)


def create_tsbs_host_series(r, host_id, chunk_type):
    for cpu_metric_name in CPU_METRICS:
        create_cmd = [
            "TS.CREATE",
            serie_name(host_id, cpu_metric_name),
            chunk_type,
            "LABELS",
            "hostname",
            "host_{}".format(host_id),
            "region",
            "us-west-1",
            "datacenter",
            "us-west-1a",
            "rack",
            "41",
            "os",
            "Ubuntu15.10",
            "arch",
            "x64",
            "team",
            "NYC",
            "service",
            "9",
            "service_version",
            "1",
            "service_environment",
            "staging",
            "measurement",
            "cpu",
            "fieldname",
            cpu_metric_name,
        ]
        assert r.execute_command(*create_cmd)


def populate_tsbs_host_serie(r, host_id, datapoints_per_serie, start_ts, delta):
    for datapoint_n in range(datapoints_per_serie):
        ts = start_ts + (datapoint_n * delta)
        for cpu_metric_name in CPU_METRICS:
            datapoint_cmd = [
                "TS.ADD",
                serie_name(host_id, cpu_metric_name),
                ts,
                random.random() * 100.0,
            ]
            assert r.execute_command(*datapoint_cmd)


def test_mrange_cpu_max_all_1():
    total_hosts = 5
    datapoints_per_serie = 4320
    start_ts = 1451606400000
    delta = 10000
    max_repetitions = 1000
    for CHUNK_TYPE in CHUNK_TYPES:
        e = Env()
        e.flush()
        with e.getClusterConnectionIfNeeded() as r:
            for host_id in range(1, total_hosts + 1):
                create_tsbs_host_series(r, host_id, CHUNK_TYPE)
                populate_tsbs_host_serie(
                    r, host_id, datapoints_per_serie, start_ts, delta
                )
        shard_conn = e.getConnection()
        for _ in range(max_repetitions):
            host_id = random.randint(1, total_hosts)
            res = shard_conn.execute_command(
                "TS.MRANGE - + WITHLABELS AGGREGATION MAX 3600000 FILTER measurement=cpu hostname=host_{}".format(
                    host_id
                )
            )
            e.assertEqual(len(res), 10)
