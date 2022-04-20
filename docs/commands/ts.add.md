### TS.ADD

Append a sample to a time series. 

```sql
TS.ADD key timestamp value [RETENTION retentionPeriod] [ENCODING [COMPRESSED|UNCOMPRESSED]] [CHUNK_SIZE size] [ON_DUPLICATE policy] [LABELS {label value}...]
```

If the time series does not exist - it will be automatically created.

- _key_ - Key name for time series
- _timestamp_ - (integer) UNIX sample timestamp **in milliseconds**. `*` can be used for an automatic timestamp from the server's clock.
- _value_ - (double) numeric data value of the sample. We expect the double number to follow [RFC 7159](https://tools.ietf.org/html/rfc7159) (JSON standard). In particular, the parser will reject overly large values that would not fit in binary64. It will not accept NaN or infinite values.

The following arguments are optional because they can be set by `TS.CREATE`:

 - `RETENTION` _retentionPeriod_ - Maximum retention period, compared to maximal existing timestamp (in milliseconds).

    Used only if a new time series is created. Ignored When adding samples to an existing time series.
  
    When set to 0, the series is not trimmed. If not specified: set to the global [RETENTION_POLICY](https://redis.io/docs/stack/timeseries/configuration/#retention_policy) configuration of the database (which, by default, is 0).
    
 - `ENCODING` _enc_ - Specify the series samples encoding format. One of the following values:
    - `COMPRESSED`: apply the DoubleDelta compression to the series samples, meaning compression of Delta of Deltas between timestamps and compression of values via XOR encoding.
    - `UNCOMPRESSED`: keep the raw samples in memory.

    Used only if a new time series is created. Ignored When adding samples to an existing time series.

 - `CHUNK_SIZE` _size_ - Memory size, in bytes, allocated for each data chunk. Must be a multiple of 8 in the range [128 .. 1048576].

    Used only if a new time series is created. Ignored When adding samples to an existing time series.

    If not specified: set to 4096.

 - `ON_DUPLICATE` _policy_ - Overwrite key and database configuration for [DUPLICATE_POLICY](https://redis.io/docs/stack/timeseries/configuration/#duplicate_policy) (policy for handling samples with identical timestamps). One of the following values:
   - `BLOCK` - an error will occur for any out of order sample
   - `FIRST` - ignore any newly reported value
   - `LAST` - override with the newly reported value
   - `MIN` - only override if the value is lower than the existing value
   - `MAX` - only override if the value is higher than the existing value
   - `SUM` - If a previous sample exists, add the new sample to it so that the updated value is equal to (previous + new). If no previous sample exists, set the updated value equal to the new value.

 - `LABELS` {_label_ _value_}... - Set of label-value pairs that represent metadata labels of the time series.

    Used only if a new time series is created. Ignored When adding samples to an existing time series.

#### Examples
```sql
127.0.0.1:6379>TS.ADD temperature:2:32 1548149180000 26 LABELS sensor_id 2 area_id 32
(integer) 1548149180000
127.0.0.1:6379>TS.ADD temperature:3:11 1548149183000 27 RETENTION 3600
(integer) 1548149183000
127.0.0.1:6379>TS.ADD temperature:3:11 * 30
(integer) 1559718352000
```

#### Complexity

If a compaction rule exits on a time series, `TS.ADD` performance might be reduced.
The complexity of `TS.ADD` is always O(M) when M is the number of compaction rules or O(1) with no compaction.

#### Notes

- You can use this command to add data to a nonexisting time series in a single command.
  This is why `RETENTION`, `ENCODING`, `CHUNK_SIZE`, `ON_DUPLICATE`, and `LABELS` are optional arguments.
- When specified and the key doesn't exist, a new time series will be created.
  Setting `RETENTION` and `LABELS` introduces additional time complexity.
- If _timestamp_ is older than the retention period (compared to maximal existing timestamp) - the sample will not be appended.
- When adding a sample to a time series for which compaction rules are defined:
  - If all the original samples for an affected aggregated time bucket are available - the compacted value will be recalculated based on the reported sample and the original samples.
  - If only part of the original samples for an affected aggregated time bucket are available (due to trimming caused in accordance with the time series RETENTION policy) - the compacted value will be recalculated based on the reported sample and the available original samples.
  - If the original samples for an affected aggregated time bucket are not available (due to trimming caused in accordance with the time series RETENTION policy) - the compacted value bucket will not be updated.  
