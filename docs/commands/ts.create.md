## Create

### TS.CREATE

Create a new time series. 

```sql
TS.CREATE key [RETENTION retentionTime] [ENCODING [UNCOMPRESSED|COMPRESSED]] [CHUNK_SIZE size] [DUPLICATE_POLICY policy] [LABELS {label value}...]
```

- _key_ - Key name for time series

Optional args:

- `RETENTION` _retentionTime_ - Maximum age for samples compared to last event time (in milliseconds)

   When set to 0, the series is not trimmed.

   When not specified: set to the global [RETENTION_POLICY](https://redis.io/docs/stack/timeseries/configuration/#retention_policy) configuration of the database (which, by default, is 0).

- `ENCODING` _enc_ - Specify the series samples encoding format. One of the following values:
   - `COMPRESSED`: apply the DoubleDelta compression to the series samples, meaning compression of Delta of Deltas between timestamps and compression of values via XOR encoding.
   - `UNCOMPRESSED`: keep the raw samples in memory. Adding this flag will keep data in an uncompressed form. Compression not only saves
   memory but usually improve performance due to lower number of memory accesses.

   When not specified: set to the global [CHUNK_TYPE](https://redis.io/docs/stack/timeseries/configuration/#chunk_type) configuration of the database (which, by default, is `COMPRESSED`).

- `CHUNK_SIZE` _size_ - memory size, in bytes, allocated for data. Must be a multiple of 8.

   When not specified: set to 4096.

- `DUPLICATE_POLICY` _policy_ - Policy for handling multiple samples with identical timestamps. One of the following values:
  - `BLOCK` - an error will occur for any out of order sample
  - `FIRST` - ignore any newly reported value
  - `LAST` - override with the newly reported value
  - `MIN` - only override if the value is lower than the existing value
  - `MAX` - only override if the value is higher than the existing value
  - `SUM` - If a previous sample exists, add the new sample to it so that the updated value is equal to (previous + new). If no previous sample exists, set the updated value equal to the new value.

  When not specified: set to the global [DUPLICATE_POLICY](https://redis.io/docs/stack/timeseries/configuration/#duplicate_policy) configuration of the database (which, by default, is `BLOCK`).

- `LABELS` {_label_ _value_}... - Set of label-value pairs that represent metadata labels of the key

#### Complexity

TS.CREATE complexity is O(1).

#### Create Example

```sql
TS.CREATE temperature:2:32 RETENTION 60000 DUPLICATE_POLICY MAX LABELS sensor_id 2 area_id 32
```

#### Errors

* If a key already exists, you get a normal Redis error reply `TSDB: key already exists`. You can check for the existence of a key with Redis [EXISTS command](https://redis.io/commands/exists).

#### Notes

`TS.ADD` can also create a new time-series if called with a key that does not exist.
