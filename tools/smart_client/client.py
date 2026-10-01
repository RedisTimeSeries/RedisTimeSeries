"""Experimental OSS-cluster MRANGE coordinator; requires the module LOCAL patch."""

import asyncio
import math
from collections import defaultdict
from dataclasses import dataclass


class CoordinationError(RuntimeError):
    pass


@dataclass(frozen=True)
class Series:
    key: str
    labels: dict
    samples: list  # (timestamp, float) pairs


@dataclass(frozen=True)
class Group:
    label: str
    value: str
    reducer: str
    sources: tuple
    samples: list


def _mapping(value):
    return value if isinstance(value, dict) else dict(zip(value[::2], value[1::2]))


def supports_local(docs):
    """Require the positional LOCAL token on both commands, not a version guess."""
    docs = _mapping(docs)
    for command in ('ts.mrange', 'ts.mrevrange'):
        info = _mapping(docs.get(command, []))
        args = info.get('arguments', [])
        if len(args) < 3:
            return False
        arg = _mapping(args[2])
        if arg.get('token') != 'LOCAL' or arg.get('type') != 'pure-token':
            return False
    return True


def topology(slots, seed_host):
    """Canonical full slot map, including owner IDs to detect primary changes."""
    result = []
    next_slot = 0
    for entry in sorted(slots, key=lambda row: int(row[0])):
        start, end = int(entry[0]), int(entry[1])
        if start != next_slot or end < start or end >= 16384:
            raise CoordinationError('Incomplete or overlapping cluster slot map')
        primary = entry[2]
        host = primary[0] or seed_host
        if host == '?':
            raise CoordinationError('Primary has no usable advertised endpoint')
        if len(primary) < 3 or not primary[2]:
            raise CoordinationError('Primary node ID missing')
        result.append((start, end, host, int(primary[1]), primary[2]))
        next_slot = end + 1
    if next_slot != 16384:
        raise CoordinationError('Cluster does not cover all slots')
    return tuple(result)


def merge_series(replies):
    result = {}
    for reply in replies:
        for key, labels, samples in reply:
            if key in result:
                raise CoordinationError('Duplicate series across shards; discard query and refresh')
            result[key] = Series(key, dict(labels), [(int(t), float(v)) for t, v in samples])
    return [result[key] for key in sorted(result)]


def validate_local_replies(replies):
    """LibMR rule: reply-time ownership must cover 0..16383 exactly once.

    Do not discard overlapping contributions: reject the entire query before
    merging, especially before irreversible cross-series reductions.
    """
    ranges = []
    payloads = []
    for reply in replies:
        if not isinstance(reply, (list, tuple)) or len(reply) != 3:
            raise CoordinationError('Expected versioned LOCAL reply')
        version, owned, payload = reply
        if type(version) is not int or version != 1:
            raise CoordinationError('Unsupported LOCAL reply version')
        if not isinstance(owned, (list, tuple)):
            raise CoordinationError('Invalid LOCAL slot ranges')
        for pair in owned:
            if (not isinstance(pair, (list, tuple)) or len(pair) != 2 or
                    any(type(slot) is not int for slot in pair) or
                    not 0 <= pair[0] <= pair[1] <= 16383):
                raise CoordinationError('Invalid LOCAL slot range')
            ranges.append(tuple(pair))
        if isinstance(payload, Exception):
            raise payload
        if not isinstance(payload, list) or (not owned and payload):
            raise CoordinationError('Invalid LOCAL result payload')
        payloads.append(payload)
    expected = 0
    for start, end in sorted(ranges):
        if start != expected:
            raise CoordinationError('Query requires unavailable slots')
        expected = end + 1
    if expected != 16384:
        raise CoordinationError('Query requires unavailable slots')
    return payloads


REDUCERS = {'sum', 'min', 'max', 'avg', 'count'}


def reduce_series(series, label, reducer, count=None, reverse=False):
    """Reduce contributions at each timestamp AFTER per-series time aggregation."""
    if reducer not in REDUCERS:
        raise ValueError('Supported reducers: ' + ', '.join(sorted(REDUCERS)))
    groups = defaultdict(list)
    for item in series:
        if label in item.labels:
            groups[item.labels[label]].append(item)
    result = []
    for value, members in sorted(groups.items()):
        points = defaultdict(list)
        for item in members:
            for timestamp, sample in item.samples:
                if not math.isfinite(sample):
                    raise ValueError('Prototype grouped reduction requires finite samples')
                points[timestamp].append(sample)
        samples = []
        for timestamp in sorted(points, reverse=reverse):
            values = points[timestamp]
            if reducer == 'sum':
                reduced = math.fsum(values)
            elif reducer == 'avg':
                reduced = math.fsum(values) / len(values)
            elif reducer == 'count':
                reduced = float(len(values))
            elif reducer == 'min':
                reduced = min(values)
            else:
                reduced = max(values)
            samples.append((timestamp, reduced))
        result.append(Group(label, value, reducer,
                            tuple(sorted(item.key for item in members)), samples[:count]))
    return result


class LocalMRClient:
    """One instance per event loop. Use connect()/aclose() or async with.

    Stable topology is required for reliable results. Optional before/after slot
    checks detect some changes but cannot provide a cross-shard snapshot or make
    live resharding safe. No partial replies or automatic query retries.
    """

    def __init__(self, seed, factory, seed_host, *, timeout=5.0,
                 max_inflight=32, check_topology=True):
        if timeout <= 0 or max_inflight < 1:
            raise ValueError('timeout and max_inflight must be positive')
        self.seed = seed
        self.factory = factory
        self.seed_host = seed_host
        self.timeout = timeout
        self.check_topology = check_topology
        self._gate = asyncio.Semaphore(max_inflight)
        self._nodes = {}
        self._topology = None
        self._closed = False

    @classmethod
    async def connect(cls, host='localhost', port=6379, *, timeout=5.0,
                      max_inflight=32, check_topology=True, **connection_options):
        # Lazy import keeps merge logic and tests independent of redis-py.
        from redis.asyncio import Redis
        from redis.backoff import NoBackoff
        from redis.asyncio.retry import Retry

        def factory(node_host, node_port):
            node = Redis(**dict(connection_options, host=node_host, port=node_port,
                                db=0, decode_responses=True, protocol=2,
                                socket_connect_timeout=timeout, socket_timeout=timeout,
                                max_connections=max_inflight + 2,
                                retry=Retry(NoBackoff(), 0)))
            # Keep RESP2 arrays, including node IDs that redis-py's CLUSTER SLOTS
            # convenience parser may omit. No TimeSeries response callback needed.
            for command in ('CLUSTER SLOTS', 'COMMAND DOCS', 'TS.MRANGE', 'TS.MREVRANGE'):
                node.set_response_callback(command, lambda response, **kwargs: response)
            return node

        client = cls(factory(host, port), factory, host, timeout=timeout,
                     max_inflight=max_inflight, check_topology=check_topology)
        try:
            await asyncio.wait_for(client._initialize(), timeout)
            return client
        except BaseException:
            await client.aclose()
            raise

    async def _discover(self):
        slots = await self.seed.execute_command('CLUSTER SLOTS')
        return topology(slots, self.seed_host)

    async def _initialize(self):
        self._topology = await self._discover()
        endpoints = sorted({(row[2], row[3]) for row in self._topology})
        self._nodes = {endpoint: self.factory(*endpoint) for endpoint in endpoints}

        async def validate(node):
            docs = await node.execute_command('COMMAND DOCS', 'ts.mrange', 'ts.mrevrange')
            if not supports_local(docs):
                raise CoordinationError('Every primary must advertise positional LOCAL support')

        await self._all([validate(node) for node in self._nodes.values()])

    @staticmethod
    async def _all(coroutines):
        tasks = [asyncio.create_task(coro) for coro in coroutines]
        try:
            return await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def mrange(self, start, end, *, filters, aggregation=None, bucket=None,
                     count=None, with_labels=False, groupby=None, reducer=None,
                     reverse=False):
        if self._closed or self._topology is None:
            raise CoordinationError('Client is closed or not initialized')
        if not filters or isinstance(filters, (str, bytes)):
            raise ValueError('filters must be a nonempty sequence of label predicates')
        if any(not isinstance(f, str) or '=' not in f for f in filters):
            raise ValueError('Each filter must be a label predicate')
        if (groupby is None) != (reducer is None):
            raise ValueError('groupby and reducer must be supplied together')
        if reducer is not None and reducer not in REDUCERS:
            raise ValueError('Unsupported reducer')
        if count is not None and (not isinstance(count, int) or count < 1):
            raise ValueError('count must be a positive integer')
        if (aggregation is None) != (bucket is None):
            raise ValueError('aggregation and bucket must be supplied together')
        if aggregation is not None and (
                not isinstance(aggregation, str) or ',' in aggregation or
                not isinstance(bucket, int) or bucket < 1):
            raise ValueError('Use one aggregation and a positive integer bucket')

        command = 'TS.MREVRANGE' if reverse else 'TS.MRANGE'
        args = [command, start, end, 'LOCAL']
        if groupby is not None or with_labels:
            args.append('WITHLABELS')
        if aggregation is not None:
            args.extend(['AGGREGATION', aggregation, bucket])
        # COUNT on grouped queries applies after GLOBAL reduction. Truncating
        # individual series first can change which contributions survive.
        if count is not None and groupby is None:
            args.extend(['COUNT', count])
        args.extend(['FILTER', *filters])

        async def execute():
            if self.check_topology and await self._discover() != self._topology:
                raise CoordinationError('Topology changed; create a new client')
            replies = await self._all([
                node.execute_command(*args) for node in self._nodes.values()])
            if self.check_topology and await self._discover() != self._topology:
                raise CoordinationError('Topology changed during query; discard result')
            series = merge_series(validate_local_replies(replies))
            if groupby is not None:
                return reduce_series(series, groupby, reducer, count, reverse)
            return series

        async def admitted():
            async with self._gate:
                return await execute()

        return await asyncio.wait_for(admitted(), self.timeout)

    async def aclose(self):
        self._closed = True
        await asyncio.gather(self.seed.aclose(),
                             *(node.aclose() for node in self._nodes.values()),
                             return_exceptions=True)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.aclose()
