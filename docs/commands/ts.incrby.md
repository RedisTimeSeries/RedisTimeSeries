### TS.INCRBY

Creates a new sample that increments the latest sample's value.
> Note: TS.INCRBY support updates for the latest sample.

```sql
TS.INCRBY key value [TIMESTAMP timestamp] [RETENTION retentionTime] [UNCOMPRESSED] [CHUNK_SIZE size] [LABELS {label value}...]
```

This command can be used as a counter or gauge that automatically gets history as a time series.

* _key_ - Key name for time series
* _value_ - numeric data value of the sample (double)

Optional args:

 * `TIMESTAMP` _timestamp_ - (integer) UNIX sample timestamp **in milliseconds**. `*` can be used for an automatic timestamp from the system clock.
 * `RETENTION` _retentionTime_ - Maximum age for samples compared to last event time (in milliseconds).
    * Default: The global retention secs configuration of the database (by default, `0`)
    * When set to 0, the series is not trimmed at all
 * `UNCOMPRESSED` - Changes data storage from compressed (by default) to uncompressed
 * `CHUNK_SIZE` _size_ - amount of memory, in bytes, allocated for data. Must be a multiple of 8, Default: 4096.
 * `LABELS` {_label_ _value_}... - Set of label-value pairs that represent metadata labels of the key

#### Notes

- You can use this command to add data to a nonexisting time series in a single command.
  This is why `RETENTION` and `LABELS` are optional arguments.
- If this command is used to add data to an existing time series, `RETENTION` and `LABELS` are ignored.
- When specified and the key doesn't exist, a new key with the specified `RETENTION` and/or `LABELS` will be created.
  Setting the `RETENTION` and `LABELS` introduces additional time complexity.