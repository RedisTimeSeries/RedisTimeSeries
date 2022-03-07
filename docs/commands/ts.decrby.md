### TS.DECRBY

Creates a new sample that decrements the latest sample's value.
> Note: TS.DECRBY support updates for the latest sample.

```sql
TS.DECRBY key value [TIMESTAMP timestamp] [RETENTION retentionTime] [UNCOMPRESSED] [CHUNK_SIZE size] [LABELS label value..]
```

This command can be used as a counter or gauge that automatically gets history as a time series.

* key - Key name for timeseries
* value - numeric data value of the sample (double)

Optional args:

 * TIMESTAMP - UNIX timestamp of the sample. `*` can be used for automatic timestamp (using the system clock)
 * RETENTION - Maximum age for samples compared to last event time (in milliseconds)
    * Default: The global retention secs configuration of the database (by default, `0`)
    * When set to 0, the series is not trimmed at all
 * UNCOMPRESSED - Changes data storage from compressed (by default) to uncompressed
 * CHUNK_SIZE - amount of memory, in bytes, allocated for data. Must be a multiple of 8, Default: 4096.
 * labels - Set of label-value pairs that represent metadata labels of the key

If this command is used to add data to an existing timeseries, `retentionTime` and `labels` are ignored.

#### Notes

- You can use this command to add data to an non existing timeseries in a single command.
  This is the reason why `labels` and `retentionTime` are optional arguments.
- When specified and the key doesn't exist, RedisTimeSeries will create the key with the specified `labels` and or `retentionTime`.
  Setting the `labels` and `retentionTime` introduces additional time complexity.