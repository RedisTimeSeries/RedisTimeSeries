## Create

### TS.CREATE

Create a new time series. 

```sql
TS.CREATE key [RETENTION retentionPeriod] [ENCODING [UNCOMPRESSED|COMPRESSED]] [CHUNK_SIZE size] [DUPLICATE_POLICY policy] [LABELS {label value}...]
```

- _key_ - Key name for time series

Optional args:

- `RETENTION` _retentionPeriod_ - Maximum age for samples compared to highest reported timestamp (in milliseconds).

   Samples are expired based solely on the difference between their timestamp and the timestamps passed to [TS.ADD](/commands/ts.add/), [TS.MADD](/commands/ts.madd/), [TS.INCRBY](/commands/ts.incrby/), and [TS.DECRBY](/commands/ts.decrby/). When none of these functions is called - samples would not expire.

   When set to 0: samples will never expire.

   When not specified: set to the global [RETENTION_POLICY](/docs/stack/timeseries/configuration/#retention_policy) configuration of the database (which, by default, is 0).

- `ENCODING` _enc_ - Specify the series samples encoding format. One of the following values:
   - `COMPRESSED`: apply compression to the series samples.
   - `UNCOMPRESSED`: keep the raw samples in memory. Adding this flag will keep data in an uncompressed form. 

   `COMPRESSED` (explained [here](https://redis.com/blog/redistimeseries-version-1-2-is-here/)) is almost always the right choice. Compression not only saves memory but usually improve performance due to lower number of memory accesses. You will usually gain about 90% memory reduction. The exception may be highly irregular (almost random) timestamps or values, which is rarely the case.

   When not specified: set to `COMPRESSED`.

- `CHUNK_SIZE` _size_ - memory size, in bytes, allocated for each data chunk. Must be a multiple of 8 in the range [128 .. 1048576].

   When not specified: set to 4096 bytes (a single memory page).

   The data in each key is stored in chunks. Each chunk contains data in a given timeframe. We keep an index of all chucks. Then, inside each chunk - we iterate. There are tradeoffs for having smaller or larger sizes of chunks - depending on your usage:

   - Insert performance: Smaller chunks result in slower inserts (more chunks need to be created).
   - Query performance: Queries for a small subset when the chunks are very large - are slower, as we need to iterate over the chunk to find the data.
   - Larger chunks take more memory. if you have a very large number of keys and very few samples per key - smaller chunks can save memory.

   If you are unsure about your use case, you should stick with the default.

- `DUPLICATE_POLICY` _policy_ - Policy for handling multiple samples with identical timestamps. One of the following values:
  - `BLOCK`: an error will occur for any out of order sample
  - `FIRST`: ignore any newly reported value
  - `LAST`: override with the newly reported value
  - `MIN`: only override if the value is lower than the existing value
  - `MAX`: only override if the value is higher than the existing value
  - `SUM`: If a previous sample exists, add the new sample to it so that the updated value is equal to (previous + new). If no previous sample exists, set the updated value equal to the new value.

  When not specified: set to the global [DUPLICATE_POLICY](/docs/stack/timeseries/configuration/#duplicate_policy) configuration of the database (which, by default, is `BLOCK`).

- `LABELS` {_label_ _value_}... - Set of label-value pairs that represent metadata labels of the key and serve as a secondary index.

  The [TS.MGET](/commands/ts.mget/), [TS.MRANGE](/commands/ts.mrange/), and [TS.MREVRANGE](/commands/ts.mrevrange/) commands operate on multiple time series - based on their labels. The [TS.QUERYINDEX](/commands/ts.queryindex/) command returns all time series keys matching a given filter - based on their labels.

#### Complexity

TS.CREATE complexity is O(1).

#### Create Example

```sql
TS.CREATE temperature:2:32 RETENTION 60000 DUPLICATE_POLICY MAX LABELS sensor_id 2 area_id 32
```

#### Errors

* If a key already exists, you get a normal Redis error reply `TSDB: key already exists`. You can check for the existence of a key with Redis [EXISTS command](/commands/exists).

#### Notes

* The following commands will also create a new time-series when called with a key that does not exist: [TS.ADD](/commands/ts.add/), [TS.INCRBY](/commands/ts.incrby/), and [TS.DECRBY](/commands/ts.decrby/)
