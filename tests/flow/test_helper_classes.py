import builtins
import math

from includes import *


ALLOWED_ERROR = 0.001
SAMPLE_SIZE = 16


def _fill_data(r, key):
    start_ts = 1600001540000
    date_ranges = [(start_ts, start_ts + 100), (start_ts + 1000, start_ts + 1100)]
    for start, end in date_ranges:
        for ts in range(start, end):
            r.execute_command('TS.ADD', key, ts, ts)
    return date_ranges


def _get_series_value(ts_key_result):
    """
    Get only the values from the time stamp series
    :param ts_key_result: the output of ts.range command (pairs of timestamp and value)
    :return: float values of all the values in the series
    """
    return [float(value[1]) for value in ts_key_result]


def _assert_alter_cmd(env, r, key, start_ts, end_ts,
                      expected_data=None,
                      expected_retention=None,
                      expected_chunk_size=None,
                      expected_labels=None):
    """
    Test modifications didn't change the data
    :param r: Redis instance
    :param key: redis key
    :param start_ts:
    :param end_ts:
    :param expected_data:
    :return:
    """

    actual_result = _get_ts_info(r, key)

    if expected_data:
        env.expect('TS.range', key, start_ts, end_ts, conn=r).equal(expected_data)
    if expected_retention:
        env.assertEqual(expected_retention, actual_result.retention_msecs)
    if expected_labels:
        env.assertEqual(list_to_dict(expected_labels), actual_result.labels)
    if expected_chunk_size:
        env.assertEqual(expected_chunk_size, actual_result.chunk_size_bytes)


def _ts_alter_cmd(r, key, set_retention=None, set_chunk_size=None, set_labels=None):
    """
    Assert that changed data is the same as expected, with or without modification of label or retention.
    :param r: Redis instance
    :param key: redis key to work on
    :param set_retention: If none will skip (and test that didn't change from expected result)
    :param set_labels: Format is list. If none will skip (and test that didn't change from expected result)
    :return:
    """
    cmd = ['TS.ALTER', key]
    if set_retention:
        cmd.extend(['RETENTION', set_retention])
    if set_chunk_size:
        cmd.extend(['CHUNK_SIZE', set_chunk_size])
    if set_labels:
        new_labels = [item for sublist in set_labels for item in sublist]
        cmd.extend(['LABELS'])
        cmd.extend(new_labels)
    r.execute_command(*cmd)


def _calc_downsampling_series(values, bucket_size, calc_func):
    """
    calculate the downsampling series given the wanted calc_func, by applying to calc_func to all the full buckets
    and to the remainder bucket
    :param values: values of original series
    :param bucket_size: bucket size for downsampling
    :param calc_func: function that calculates the wanted rule, for example min/sum/avg
    :return: the values of the series after downsampling
    """
    series = []
    for i in range(0, int(math.ceil(len(values) / float(bucket_size)))):
        curr_bucket_size = bucket_size
        # we don't have enough values for a full bucket anymore
        if (i + 1) * bucket_size > len(values):
            curr_bucket_size = len(values) - i
        series.append(calc_func(values[i * bucket_size: i * bucket_size + curr_bucket_size]))
    return series


def calc_rule(rule, values, bucket_size):
    """
    Calculate the downsampling with the given rule
    :param rule: 'avg' / 'max' / 'min' / 'sum' / 'count'
    :param values: original series values
    :param bucket_size: bucket size for downsampling
    :return: the values of the series after downsampling
    """
    if rule == 'avg':
        return _calc_downsampling_series(values, bucket_size, lambda x: float(sum(x)) / len(x))
    elif rule in ['sum', 'max', 'min']:
        return _calc_downsampling_series(values, bucket_size, getattr(builtins, rule))
    elif rule == 'count':
        return _calc_downsampling_series(values, bucket_size, len)


def _insert_agg_data(env, r, key, agg_type, chunk_type="", fromTS=10, toTS=50, key_create_args=None):
    agg_key = '%s_agg_%s_10' % (key, agg_type)

    if key_create_args is None:
        key_create_args = []

    env.expect('TS.CREATE', key, chunk_type, *key_create_args, conn=r).noError()
    env.expect('TS.CREATE', agg_key, chunk_type, *key_create_args, conn=r).noError()
    env.expect('TS.CREATERULE', key, agg_key, "AGGREGATION", agg_type, 10, conn=r).noError()

    values = (31, 41, 59, 26, 53, 58, 97, 93, 23, 84)
    for i in range(fromTS, toTS):
        env.expect('TS.ADD', key, i, i // 10 * 100 + values[i % 10], conn=r).noError()
    # close last bucket
    env.expect('TS.ADD', key, toTS + 1000, 0, conn=r).noError()

    return agg_key


def _insert_data(r, key, start_ts, samples_count, value):
    """
    insert data to key, starting from start_ts, with 1 sec interval between them
    :param redis: redis connection
    :param key: name of time_series
    :param start_ts: beginning of time series
    :param samples_count: number of samples
    :param value: could be a list of samples_count values, or one value. if a list, insert the values in their
    order, if not, insert the single value for all the timestamps
    """
    for i in range(samples_count):
        if type(value) == list:
            value_to_insert = value[i]
        else:
            value_to_insert = float(value)
        actual_result = r.execute_command('TS.ADD', key, start_ts + i, value_to_insert)
        if type(actual_result) == int:
            env.assertEqual(actual_result, int(start_ts + i))
        else:
            assert actual_result


def list_to_dict(aList):
    return {aList[i][0]: aList[i][1] for i in range(len(aList))}


def _get_ts_info(r, key):
    return TSInfo(r.execute_command('TS.INFO', key))


class TSInfo(object):
    rules = []
    labels = []
    sourceKey = None
    chunk_count = None
    memory_usage = None
    total_samples = None
    retention_msecs = None
    last_time_stamp = None
    first_time_stamp = None
    chunk_size_bytes = None
    chunk_type = None
    chunks = None

    def __init__(self, args):
        response = dict(zip(args[::2], args[1::2]))
        if 'rules' in response: self.rules = response['rules']
        if 'sourceKey' in response: self.sourceKey = response['sourceKey']
        if 'chunkCount' in response: self.chunk_count = response['chunkCount']
        if 'labels' in response: self.labels = list_to_dict(response['labels'])
        if 'memoryUsage' in response: self.memory_usage = response['memoryUsage']
        if 'totalSamples' in response: self.total_samples = response['totalSamples']
        if 'retentionTime' in response: self.retention_msecs = response['retentionTime']
        if 'lastTimestamp' in response: self.last_time_stamp = response['lastTimestamp']
        if 'firstTimestamp' in response: self.first_time_stamp = response['firstTimestamp']
        if 'chunkSize' in response: self.chunk_size_bytes = response['chunkSize']
        if 'chunkType' in response: self.chunk_type = response['chunkType']
        if 'Chunks' in response: self.chunks = response['Chunks']

    def __eq__(self, other):
        if not isinstance(other, TSInfo):
            return NotImplemented
        return self.rules == other.rules and \
               self.sourceKey == other.sourceKey and \
               self.chunk_count == other.chunk_count and \
               self.memory_usage == other.memory_usage and \
               self.total_samples == other.total_samples and \
               self.retention_msecs == other.retention_msecs and \
               self.last_time_stamp == other.last_time_stamp and \
               self.first_time_stamp == other.first_time_stamp and \
               self.chunk_size_bytes == other.chunk_size_bytes and \
               self.chunks == other.chunks
