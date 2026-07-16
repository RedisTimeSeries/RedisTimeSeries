from includes import *
from test_helper_classes import _get_ts_info


# Regression tests for TS.INFO memoryUsage (and MEMORY USAGE via the mem_usage
# callback) under-reporting per-key memory.
#
# SeriesMemUsage() used to omit:
#   - the series key name string (Series.keyName)
#   - the series' share of the global label->series index (IndexMemUsage)
# As a result TS.INFO reported less memory than the key actually consumes,
# which skews capacity estimates (size one series, multiply by key count).


def test_memory_usage_accounts_for_key_name():
    # Two series identical except for key-name length: the longer key name
    # must be reflected in the reported memory.
    with Env().getClusterConnectionIfNeeded() as r:
        r.flushall()
        short_key = '{mem}s'
        long_key = '{mem}' + ('k' * 512)
        r.execute_command('TS.CREATE', short_key)
        r.execute_command('TS.CREATE', long_key)

        short_mem = _get_ts_info(r, short_key).memory_usage
        long_mem = _get_ts_info(r, long_key).memory_usage

        assert long_mem > short_mem
        assert long_mem - short_mem >= 256


def test_memory_usage_accounts_for_label_index():
    # A labeled series occupies space in the global label->series index, which
    # is now attributed back to the key. A labeled series must therefore report
    # more memory than an otherwise identical unlabeled one.
    with Env().getClusterConnectionIfNeeded() as r:
        r.flushall()
        plain_key = '{idx}plain'
        labeled_key = '{idx}labeled'
        r.execute_command('TS.CREATE', plain_key)
        r.execute_command('TS.CREATE', labeled_key,
                          'LABELS', 'sensor_id', '1234', 'area_id', '4567')

        plain_mem = _get_ts_info(r, plain_key).memory_usage
        labeled_mem = _get_ts_info(r, labeled_key).memory_usage

        assert labeled_mem > plain_mem


def test_memory_usage_grows_with_more_indexed_labels():
    # More indexed labels => larger index contribution attributed to the key.
    with Env().getClusterConnectionIfNeeded() as r:
        r.flushall()
        one_label = '{grow}one'
        many_labels = '{grow}many'
        r.execute_command('TS.CREATE', one_label, 'LABELS', 'a', '1')
        r.execute_command('TS.CREATE', many_labels,
                          'LABELS', 'a', '1', 'b', '2', 'c', '3', 'd', '4')

        one_mem = _get_ts_info(r, one_label).memory_usage
        many_mem = _get_ts_info(r, many_labels).memory_usage

        assert many_mem > one_mem
