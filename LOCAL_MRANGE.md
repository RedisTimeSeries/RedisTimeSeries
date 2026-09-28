# Shard-local MRANGE prototype

Syntax:

```
TS.MRANGE fromTimestamp toTimestamp [LOCAL] [existing options] FILTER ...
TS.MREVRANGE fromTimestamp toTimestamp [LOCAL] [existing options] FILTER ...
```

`LOCAL` is case-insensitive and must immediately follow the two timestamps.
It bypasses LibMR and uses the existing local query implementation, including
label filtering, key permission checks, time aggregation, and reply formatting.
Without it, distributed dispatch is unchanged. On a standalone server the
option has no effect on the result.

The result covers the receiving shard only. `GROUPBY/REDUCE` likewise reduces
only that shard's matching series; these finalized values are not a universal
partial-aggregation format. In particular, averaging shard averages is incorrect.
An initial client should request per-series results and perform the global
cross-series reduction itself.

Local execution does not enter the distributed path's blocking-context guard.
Redis's ordinary command/context checks still apply. No cross-shard snapshot or
migration deduplication is provided by this option.

The flag controls module execution, not Enterprise proxy routing. A client must
use a supported route to each intended primary shard. Repeated requests through
an arbitrary database endpoint do not guarantee coverage of all shards.

The first prototype deliberately excludes label-directory synchronization,
topology-aware client fan-out, and mergeable partial reducer states. It makes
local execution available for measuring the coordination overhead separately.

Validation: `tests/flow/test_ts_mrange_local.py` covers three-shard isolation,
unchanged default fan-out, local grouping, reverse queries, label-name collisions,
standalone option parity, invalid queries, and local key permission checks.
Run it in the existing standalone and three-shard RLTest matrices. The isolation
test skips Enterprise proxy environments: shard-targeted Enterprise integration
and throughput measurements remain required before shipping.
