---
syntax: 
---

Create a new time series

## Syntax

{{< highlight bash >}}
TS.CREATE key 
  [RETENTION retentionPeriod] 
  [ENCODING [UNCOMPRESSED|COMPRESSED]] 
  [CHUNK_SIZE size] 
  [DUPLICATE_POLICY policy] 
  [LABELS {label value}...]
{{< / highlight >}}

[Examples](#examples)

## Required arguments

<details open><summary><code>key</code></summary> 

is key name for the time series.
</details>

<note><b>Notes:</b>

- If a key already exists, you get a Redis error reply, `TSDB: key already exists`. You can check for the existence of a key with the `EXISTS` command.
- Other commands that also create a new time series when called with a key that does not exist are `TS.ADD`, `TS.INCRBY`, and `TS.DECRBY`.
</note>

## Optional arguments

<details open><summary><code>RETENTION retentionPeriod</code></summary> 

is maximum age for samples compared to the highest reported timestamp, in milliseconds. Samples are expired based solely on the difference between their timestamp and the timestamps passed to subsequent `TS.ADD`, `TS.MADD`, `TS.INCRBY`, and `TS.DECRBY` calls.

When set to 0, samples never expire. When not specified, the option is set to the global [RETENTION_POLICY](/docs/stack/timeseries/configuration/#retention_policy) configuration of the database, which by default is 0.
</details>

<details open><summary><code>ENCODING enc</code></summary> 

specifies the series samples encoding format as one of the following values:
 - `COMPRESSED`, applies compression to the series samples.
 - `UNCOMPRESSED`, keeps the raw samples in memory. Adding this flag keeps data in an uncompressed form. 

`COMPRESSED` is almost always the right choice. Compression not only saves memory but usually improves performance due to a lower number of memory accesses. It can result in about 90% memory reduction. The exception are highly irregular timestamps or values, which occur rarely.

When not specified, the option is set to `COMPRESSED`.
</details>

<details open><summary><code>CHUNK_SIZE size</code></summary> 

is initial allocation size, in bytes, for the data part of each new chunk. Actual chunks may consume more memory. Changing chunkSize (using `TS.ALTER`) does not affect existing chunks.

Must be a multiple of 8 in the range [64 .. 1048576]. When not specified, it is set to 4096 bytes (a single memory page).

Note: the minimal value was 128 between versions 1.6.10 and 1.6.17, and in version 1.8.0.

The data in each key is stored in chunks. Each chunk contains header and data for a given timeframe. An index contains all chunks. Iterations occur inside each chunk. Depending on your use case, consider these tradeoffs for having smaller or larger sizes of chunks:

  - Insert performance: Smaller chunks result in slower inserts (more chunks need to be created).
  - Query performance: Queries for a small subset when the chunks are very large are slower, as we need to iterate over the chunk to find the data.
  - Larger chunks may take more memory when you have a very large number of keys and very few samples per key, or less memory when you have many samples per key.

 If you are unsure about your use case, select the default.
</details>

<details open><summary><code>DUPLICATE_POLICY policy</code></summary> 

is policy for handling multiple samples with identical timestamps, with one of the following values:
  - `BLOCK`: an error will occur for any out of order sample
  - `FIRST`: ignore any newly reported value
  - `LAST`: override with the newly reported value
  - `MIN`: only override if the value is lower than the existing value
  - `MAX`: only override if the value is higher than the existing value
  - `SUM`: If a previous sample exists, add the new sample to it so that the updated value is equal to (previous + new). If no previous sample exists, set the updated value equal to the new value.

  When not specified: set to the global [DUPLICATE_POLICY](/docs/stack/timeseries/configuration/#duplicate_policy) configuration of the database (which, by default, is `BLOCK`).
</details>

<details open><summary><code>LABELS {label value}...</code></summary> 

is set of label-value pairs that represent metadata labels of the key and serve as a secondary index.

The `TS.MGET`, `TS.MRANGE`, and `TS.MREVRANGE` commands operate on multiple time series based on their labels. The `TS.QUERYINDEX` command returns all time series keys matching a given filter based on their labels.
</details>

## Examples 

<details open><summary><b>Create a temperature time series</b></summary>

{{< highlight bash >}}
127.0.0.1:6379> TS.CREATE temperature:2:32 RETENTION 60000 DUPLICATE_POLICY MAX LABELS sensor_id 2 area_id 32
OK
{{< / highlight >}}
</details>

## See also

`TS.ADD` | `TS.INCRBY` | `TS.DECRBY` | `TS.MGET` | `TS.MRANGE` | `TS.MREVRANGE` | `TS.QUERYINDEX`

## Related topics

- [RedisTimeSeries](/docs/stack/timeseries)
- [RedisTimeSeries Version 1.2 Is Here!](https://redis.com/blog/redistimeseries-version-1-2-is-here/)
