# Shard-local MRANGE prototype

Syntax:

```
TS.MRANGE fromTimestamp toTimestamp [LOCAL] [existing options] FILTER ...
TS.MREVRANGE fromTimestamp toTimestamp [LOCAL] [existing options] FILTER ...
```

`LOCAL` is case-insensitive and must immediately follow the two timestamps.
It bypasses LibMR and uses the existing local query implementation, including
label filtering, key permission checks, time aggregation, and reply formatting.
Without it, distributed dispatch and reply format are unchanged.

LOCAL returns a versioned envelope in both RESP2 and RESP3:

```
[1, [[startSlot, endSlot], ...], normalQueryReply]
```

The payload retains the normal protocol-specific MRANGE reply format. Ranges
are inclusive. Even an empty result includes slot coverage. Standalone reports
`[[0, 16383]]`; a cluster node with no local slots reports `[]`. In cluster mode,
the slot-range API is required; an unavailable API or NULL snapshot returns an
error instead of fabricated coverage. This intentionally changes the earlier
draft's unversioned LOCAL response; upgrade the prototype client with the module.

Ranges use `RedisModule_ClusterGetLocalSlotRanges`, as does LibMR's
`TS.INTERNAL_SLOT_RANGES`, during the same synchronous command as the query.
The existing QueryIndex ASM ownership filtering remains active. Before merging,
the client must sort all reply ranges and require coverage of 0..16383 exactly
once, matching `valid_slot_ranges` in `src/libmr_commands.c`. Gaps or overlaps
fail the whole query. Never silently select or deduplicate overlapping ranges.
Payload errors must also fail the whole query, including nested RESP errors.

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

The prototype includes Python fan-out and coverage validation, but deliberately
excludes label-directory synchronization and mergeable partial reducer states.
See `tools/smart_client/LIBMR_PARITY.md` for remaining coordination and grouping
requirements before integration into redis-py's existing cluster client.

Validation: `tests/flow/test_ts_mrange_local.py` covers three-shard isolation,
unchanged default fan-out, local grouping, reverse queries, label-name collisions,
standalone option parity, invalid queries, and local key permission checks.
Run it in the existing standalone and three-shard RLTest matrices. The isolation
test skips Enterprise proxy environments: shard-targeted Enterprise integration
and throughput measurements remain required before shipping.
