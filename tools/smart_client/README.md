# Experimental OSS-cluster TimeSeries client

Requires Python 3.10+, redis-py, Redis 7+ (`COMMAND DOCS`), and the LOCAL module
patch on **every primary**. This is a correctness/architecture prototype, not a
production client or evidence of improved throughput.

The client discovers all primary owners using `CLUSTER SLOTS`, checks both
commands' `LOCAL` metadata, and maintains one connection pool per primary.
Each logical query fans out concurrently to all primaries. No label/key index
is copied and no shards are pruned. ACL credentials and TLS options are reused
on each connection; all advertised primary endpoints must be directly reachable.

```sh
python -m pip install -r tools/smart_client/requirements.txt
python -m unittest discover -s tools/smart_client -p 'test_*.py' -v
```

From this directory:

```python
import asyncio
from client import LocalMRClient

async def main():
    async with await LocalMRClient.connect('localhost', 7000) as client:
        series = await client.mrange(
            0, 60000, filters=['metric=cpu'],
            aggregation='avg', bucket=1000, with_labels=True)
        groups = await client.mrange(
            0, 60000, filters=['metric=cpu'],
            aggregation='avg', bucket=1000,
            groupby='tenant', reducer='avg')
        print(series, groups)

asyncio.run(main())
```

Outputs are `Series` or `Group` dataclasses with float sample values; they are
not a drop-in RESP reply or redis-py TimeSeries API replacement. Group results
include source key names explicitly. Grouping supports sum/min/max/avg/count
over finite per-series values. NaN/infinity in grouped inputs is rejected until
server-equivalent special-value semantics are implemented and verified.
Only one time aggregator per query is supported. Reverse order and COUNT are
supported; grouped COUNT is applied after global reduction. GROUPBY is never
sent to shards, so averaging finalized shard averages cannot occur. Floating
point results may differ slightly from server reductions because of summation
order/algorithm. Missing grouping labels exclude a series from grouped output.

LATEST, ALIGN, EMPTY, selected labels, sample filters, additional reducers and
multi-aggregation are intentionally not exposed by this first client API.

## Topology and failures

Use a stable cluster without resharding or failover. By default the client checks
the seed's slot map before and after each query, and rejects changed mappings.
These checks add two requests per query and can themselves bottleneck on the seed.
They cannot detect all migrations, transient changes, module reloads or stale
topology views, and do not provide a cross-shard snapshot. Initialization validates
capabilities once; reconnect after module changes. After topology errors, discard
the query and recreate the client when topology is stable.

Any shard error, timeout or duplicate series rejects the entire query; pending
requests are cancelled. There are no automatic retries or partial-success replies.
Concurrency is bounded per client, and the query timeout includes admission wait.
Network timeout cancellation does not stop a query already executing on a server.
The object is for one event loop; close it only after outstanding calls finish.

## Benchmark an existing dataset

The runner writes no data. Use a quiescent, representative dataset; it checks
ungrouped result parity, warms both paths, then reports completed logical queries
per second and end-to-end p50/p99 latency, including client decoding/merging.
Failures abort a run rather than count as completed queries.

```sh
python tools/smart_client/benchmark.py --host localhost --port 7000 \
  --filter metric=cpu --start 0 --end 60000 \
  --aggregation avg --bucket 1000 --queries 10000 --concurrency 32
```

For a controlled stable-topology performance run, add `--static-topology` to
disable per-query topology checks. Credentials come from `REDIS_USERNAME` and
`REDIS_PASSWORD`; use `--tls` for TLS. The same credentials need discovery and
command-metadata access as well as query access.

Run at 2, 3, 4 and 8 primaries with both fixed-total-data and fixed-data-per-shard
workloads. Record shard/client CPU and bytes transferred separately. Repeat runs
in alternating order to control for cache/load drift. Python reduction executes
on the event-loop thread and may become the next bottleneck; scale independent
client processes and measure before choosing a production implementation language.

Local automated tests use fake transports and exercise actual coordinator logic.
They do not replace the required live OSS cluster parity and performance runs.
