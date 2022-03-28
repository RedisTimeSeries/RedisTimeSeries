### TS.INCRBY

Creates a new sample that increments the latest sample's value.

If the time series does not exist - it will be automatically created.

> Note: TS.INCRBY support updates for the latest sample.

```sql
TS.INCRBY key value [TIMESTAMP timestamp] [RETENTION retentionTime] [UNCOMPRESSED] [CHUNK_SIZE size] [LABELS {label value}...]
```

This command can be used as a counter or gauge that automatically gets history as a time series.

- _key_ - Key name for time series
- _value_ - numeric data value of the sample (double)

Optional args:

- `TIMESTAMP` _timestamp_ - (integer) UNIX sample timestamp **in milliseconds**. `*` can be used for an automatic timestamp from the system clock.

- `RETENTION` _retentionTime_ - Maximum age for samples compared to last event time (in milliseconds).

  Used only if a new time sereies is created. Ignored When adding samples to an existing time series.

  When set to 0, the series is not trimmed. If not specified: set to the global [RETENTION_POLICY](https://redis.io/docs/stack/timeseries/configuration/#retention_policy) configuration of the database (which, by default, is 0).
 
- `UNCOMPRESSED` - Changes data storage from compressed (by default) to uncompressed

  Used only if a new time sereies is created. Ignored When adding samples to an existing time series.

- `CHUNK_SIZE` _size_ - Memory size, in bytes, allocated for data. Must be a multiple of 8.

  Used only if a new time sereies is created. Ignored When adding samples to an existing time series.

  If not specified: set to 4096.

- `LABELS` {_label_ _value_}... - Set of label-value pairs that represent metadata labels of the time series.

  Used only if a new time sereies is created. Ignored When adding samples to an existing time series.

#### Notes

- You can use this command to add data to a nonexisting time series in a single command.
  This is why `RETENTION`, `UNCOMPRESSED`,  `CHUNK_SIZE`, and `LABELS` are optional arguments.
- When specified and the key doesn't exist, a new time series is created.
  Setting the `RETENTION` and `LABELS` introduces additional time complexity.
