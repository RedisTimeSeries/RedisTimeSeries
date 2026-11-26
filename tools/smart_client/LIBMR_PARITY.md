# LibMR-to-redis-py parity audit

Reviewed the TimeSeries code at `9d967318a43850fd1b02d8b91430b9c7d5539bbb`
and its pinned LibMR submodule `121d4770e8556eb119aa9d330487c2e6313624cd`.
This is a source audit, not evidence of live ASM or performance validation.
Redis-py is the first native integration target; this directory remains a
reference prototype and has not modified redis-py's public API.

## Required behaviors

| Area | Existing behavior | Client/module requirement and current status |
| --- | --- | --- |
| Reply-time slot coverage | `collect_node_results` / `valid_slot_ranges` in `src/libmr_commands.c` require full, non-overlapping slot coverage | Implemented in LOCAL envelope and Python validator. Coverage is checked before reduction, including empty replies. |
| Atomic local scope | LibMR `MR_ClusterExecuteInternalCommands` runs its range and data steps together on Redis's main loop | LOCAL reads ranges and runs the query in one synchronous command. Do not fetch ranges as a separate client request. |
| ASM ownership filtering | `QueryIndex` calls `TrimUnownedKeysDuringReshard`; local label indexes may include imported or not-yet-trimmed keys | Keep this on the shard. Returning physical local keys and deduplicating client-side is insufficient. The local path reuses QueryIndex. |
| Topology changes | LibMR `MR_ClusterFree` aborts initiator executions through `MR_AbortRunningExecutions`; unchanged topology is compared before replacement | **Still a gap.** Before/after seed polling and slot coverage do not reproduce event-driven aborts. Native integration needs per-query topology/attempt identity, primary-only routing, rejection of stale attempts, and whole-query restart policy. Do not assume per-node topology epochs are globally interchangeable. |
| Failures and completion | Internal result handling accounts for each node/step, records nested errors, and waits for all nodes; TimeSeries preserves NOPERM classification | Prototype rejects any shard error, malformed envelope, coverage failure or duplicate series. Native integration must preserve redis-py error classes and avoid accepting partial success. |
| Timeout and cancellation | LibMR has a maximum-idle timer and terminal cleanup; this is not simply a total wall-clock deadline | Prototype uses an overall async timeout and cancels outstanding client tasks. Define the public timeout contract. Client cancellation does not cancel already-running server work; synchronous Python reduction can also delay timeout delivery. |
| Request identity and retries | LibMR tracks execution state; its message transport also uses sender/run/message IDs for duplicate suppression | Reuse redis-py's transport lifecycle, not LibMR's wire protocol. Keep attempts isolated; never combine retained replies from one topology/attempt with retried shards from another. Prototype has no automatic retries. |
| Authentication | TimeSeries copies the initiating username and applies/releases it on remote contexts | Direct requests must use the caller's credentials and key ACL checks on every primary. Prototype reuses credentials; native integration must not borrow an administrative discovery identity for data queries. |
| Processing order | Internal MRANGE applies raw-sample filters and per-series aggregation on shards; coordinator skips re-aggregation, raw filters and original timestamp clipping | Keep this split. Bucket timestamps may lie outside the raw query interval. Never filter aggregated values with raw FILTER_BY_VALUE or clip aligned bucket timestamps a second time. Exposed prototype aggregation follows this split; more options remain unimplemented. |
| Grouped COUNT and direction | Internal shard queries use no count and forward iteration; coordinator reduces, then applies final count/direction | Prototype omits shard COUNT for grouped queries and limits final groups. It currently sends MREVRANGE for reverse queries, unlike the internal path; differential tests must cover order-sensitive time aggregators before claiming parity. |
| Label and result metadata | Internal queries include labels for grouping; ResultSet builds group names, reducer/source labels, and RESP-specific source metadata | Prototype requests labels and excludes missing grouping labels, but returns custom dataclasses. Native redis-py must preserve its existing reply shape, bytes/decoded-string behavior, selected labels and source metadata. |
| Numerical and empty semantics | Module reducer classes specify NaN validity, empty behavior and floating-point algorithms; COUNT/COUNTNAN/COUNTALL differ | **Still a gap.** Prototype deliberately rejects non-finite grouped values and supports five reducers only. Do not silently enable it for every existing MRANGE option. Port semantics with differential fixtures, including infinity, all-NaN groups, empty buckets and numerical extremes. |
| Memory and backpressure | Server result construction/iteration controls how intermediate data is consumed | Prototype materializes replies and per-timestamp contribution lists. Native integration needs bounded concurrency and memory accounting, and preferably incremental merging of sorted streams. Partial shard grouping is a later optimization. |

## What does not need to move

Do not port LibMR thread pools, blocked-client contexts, internal authentication
transport, execution serialization or inter-shard reconnect machinery into the
client. Redis-py's pools and cluster node management provide transport primitives;
the feature needs their lifecycle hooks and equivalent observable semantics.
Retention, compaction/LATEST calculations, time aggregation and ASM filtering
belong on the shard. Removing LibMR from a request path does not by itself remove
the module's broader dependency on LibMR/topology initialization.

## Differential validation before enabling the redis-py feature

1. Compare ordinary MRANGE/MREVRANGE with client coordination on the same
   quiescent dataset: sparse/empty series, multiple groups, missing labels,
   count/reverse, labels/source metadata and RESP2/RESP3 decoding.
2. Exercise aggregation before reduction, bucket boundaries outside the raw
   interval, filters, LATEST, alignment and every supported reducer. Unsupported
   options should explicitly fall back or be rejected before dispatch.
3. Run live ASM transitions with importing and trimming indexes; assert that
   valid coverage yields complete results and gaps/overlaps fail. Independently
   test primary failover and topology refresh during in-flight queries.
4. Inject one-shard ACL errors, nested errors, timeouts, disconnects, duplicate
   replies and stale retries. Assert no partial aggregate escapes.
5. Benchmark only after result parity, measuring client memory/CPU and bytes as
   well as logical QPS and tail latency. Do not claim unlimited scaling.

## Source anchors

- `src/libmr_commands.c`: `collect_node_results`, `check_and_reply_on_error`,
  `RangeArgsSkipReAggregation`, `mrange_done_internal`.
- `src/libmr_integration.c`: `TS_INTERNAL_SLOT_RANGES`, `TS_INTERNAL_MRANGE_impl`,
  `ApplyCtxUser`, `ReleaseCtxUser`.
- `src/indexer.c`: `TrimUnownedKeysDuringReshard`, `QueryIndex`.
- `src/resultset.c`, `src/compaction.c`, `src/tsdb.c`: group metadata,
  reducer validity/finalization and `MultiSeriesReduce`.
- Pinned LibMR `src/cluster.c`: topology replacement, primary membership and
  sender/run/message handling; `src/mr.c`: internal execution, result/error
  collection, timeout handling and topology-triggered abort.
